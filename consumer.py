import argparse
import shlex
import threading
import time
import json
import os

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc
from coordinator_client import CoordinatorClient
from utils import log_error, log_event, log_success, log_io


def fetch_routes(client, topic):
    """Fetches routing table for a topic to determine which brokers hold which partitions"""
    return client.call("SubscribeMessage", pulsar_pb2.SubscribeMessageRequest(topic_name=topic))


def register_consumer(client, consumer_id):
    return client.call("Register", pulsar_pb2.RegisterRequest(node_type=pulsar_pb2.NODE_TYPE_CONSUMER, node_id=consumer_id, address=""))


def list_topics(client):
    return client.call("ListTopics", pulsar_pb2.ListTopicsRequest())


def group_routes_by_broker(routes):
    """Groups partitions by their assigned broker address for efficient batched polling"""
    grouped = {}
    for route in routes:
        addr = route.broker.address
        if addr not in grouped:
            grouped[addr] = []
        grouped[addr].append(route.partition)
    return grouped


def consume_from_broker(broker_addr, topic, consumer_id, start_offsets, stop_event, client):
    """
    Background worker that continuously polls a single broker for messages across
    multiple partitions. Re-evaluates routing if a redirect or error occurs
    """

    offsets = dict(start_offsets)
    current_partitions = list(offsets.keys())
    disconnect_logged = False

    while not stop_event.is_set():
        channel = grpc.insecure_channel(broker_addr)
        stub = pulsar_pb2_grpc.BrokerServiceStub(channel)
        request = pulsar_pb2.SubscribeRequest(
            topic=topic,
            consumer_id=consumer_id,
            start_offsets=[
                pulsar_pb2.PartitionOffset(partition=p, offset=offsets[p])
                for p in sorted(current_partitions)
            ],
        )

        call = stub.Subscribe(request)
        try:
            for response in call:
                if stop_event.is_set():
                    call.cancel()
                    break
                if response.status == pulsar_pb2.STATUS_OK and response.HasField("batch"):
                    for msg in response.batch.messages:
                        offsets[msg.partition] = msg.offset + 1
                        payload = msg.payload.decode("utf-8", errors="replace")
                        log_io(f"message broker={broker_addr} partition={msg.partition} payload={payload}")
                elif response.status == pulsar_pb2.STATUS_REDIRECT and response.HasField("redirect_broker"):
                    broker_addr = response.redirect_broker.address
                    log_event(f"redirected broker={broker_addr}")
                    break
                else:
                    if response.HasField("error_message"):
                        log_error(f"error {response.error_message}")
                    else:
                        log_error("error subscription failed")
                    break
        except grpc.RpcError as exc:
            if not disconnect_logged:
                log_event(f"Broker disconnect {exc.details()}")
                disconnect_logged = True

        if stop_event.is_set():
            break

        response = fetch_routes(client, topic)
        if not response:
            time.sleep(1)
            continue
        if response.status != pulsar_pb2.STATUS_OK:
            log_error(f"error {response.error_message}")
            time.sleep(1)
            continue

        broker_map = group_routes_by_broker(response.routes)
        new_broker = None
        new_partitions = []
        for addr, partitions in broker_map.items():
            selected = [p for p in current_partitions if p in partitions]
            if selected:
                new_broker = addr
                new_partitions = selected
                break

        if not new_broker:
            log_error("error: no brokers available for topic")
            return

        if new_broker != broker_addr or set(new_partitions) != set(current_partitions):
            log_event(f"routing refreshed broker={new_broker} partitions={','.join(str(p) for p in new_partitions)}")
            broker_addr = new_broker
            current_partitions = new_partitions
            offsets = {p: offsets.get(p, 0) for p in current_partitions}
            disconnect_logged = False


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar consumer")
    parser.add_argument("--coordinators-file", required=True, help="Coordinator addresses file")
    parser.add_argument("--id", required=True, help="Consumer id")
    args = parser.parse_args()

    client = CoordinatorClient(args.coordinators_file)
    register_response = register_consumer(client, args.id)
    if not register_response or not register_response.ok or not register_response.node_id:
        log_error("error: coordinator did not return consumer id")
        return
    consumer_id = register_response.node_id
    log_success(f"Consumer registered id={consumer_id}")

    log_event("Commands: list_topics, list_subscriptions, subscribe_topic <topic>, unsubscribe_topic <topic>, exit")
    active_topics = set()
    topic_threads = {}

    os.makedirs("logs", exist_ok=True)
    state_file = os.path.join("logs", f"{args.id}.json")

    def save_state():
        with open(state_file, "w") as f:
            json.dump(list(active_topics), f)

    def do_subscribe(topic):
        if topic in active_topics:
            log_event(f"Already subscribed topic={topic}")
            return
        response = fetch_routes(client, topic)
        if not response:
            log_error("error: coordinator unreachable")
            return
        if response.status != pulsar_pb2.STATUS_OK:
            log_error(f"error {response.error_message}")
            return
        broker_map = group_routes_by_broker(response.routes)
        if not broker_map:
            log_error("error: no brokers available for topic")
            return

        for broker_addr, partitions in broker_map.items():
            start_offsets = {p: 0 for p in partitions}
            stop_event = threading.Event()
            thread = threading.Thread(
                target=consume_from_broker,
                args=(broker_addr, topic, consumer_id, start_offsets, stop_event, client),
                daemon=True,
            )
            thread.start()
            topic_threads.setdefault(topic, []).append((thread, stop_event))
        active_topics.add(topic)
        save_state()

    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                saved_topics = json.load(f)
            log_event(f"Loaded {len(saved_topics)} subscriptions from state")
            for t in saved_topics:
                do_subscribe(t)
        except Exception as e:
            log_error(f"Failed to load state: {e}")

    while True:
        try:
            raw = input("> ").strip()
        except EOFError:
            log_event("Exiting")
            break

        if not raw:
            continue

        parts = shlex.split(raw)
        command = parts[0]

        if command == "exit":
            log_event("Exiting")
            break

        if command == "list_topics":
            response = list_topics(client)
            if not response:
                log_error("error: coordinator unreachable")
                continue
            if response.status != pulsar_pb2.STATUS_OK:
                log_error(f"error {response.error_message}")
                continue
            if not response.topics:
                log_io("Topics: <none>")
                continue
            log_io("Topics: " + ", ".join(response.topics))
            continue

        if command == "list_subscriptions":
            if not active_topics:
                log_io("Subscriptions: <none>")
                continue
            log_io("Subscriptions: " + ", ".join(sorted(active_topics)))
            continue

        if command == "subscribe_topic":
            if len(parts) != 2:
                log_error("usage: subscribe_topic <topic>")
                continue
            topic = parts[1]
            do_subscribe(topic)
            continue

        if command == "unsubscribe_topic":
            if len(parts) != 2:
                log_error("usage: unsubscribe_topic <topic>")
                continue
            topic = parts[1]
            if topic not in active_topics:
                log_event(f"Not subscribed topic={topic}")
                continue
            for thread, stop_event in topic_threads.get(topic, []):
                stop_event.set()
                thread.join(timeout=1)
            topic_threads.pop(topic, None)
            active_topics.discard(topic)
            save_state()
            log_event(f"Unsubscribed topic={topic}")
            continue

        log_error(f"unknown_command {command}")


if __name__ == "__main__":
    main()
