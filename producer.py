import argparse
import shlex
import json
import os

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc
from utils import DEFAULT_PARTITIONS, log_error, log_event, log_success, log_io
from coordinator_client import CoordinatorClient


def publish_with_redirect(broker_addr, topic, key, payload, max_redirects):
    """
    Attempts to publish a message to a broker. Follows redirects if the 
    contacted broker doesn't own the partition
    """
    current_addr = broker_addr
    attempts = 0
    while attempts <= max_redirects:
        channel = grpc.insecure_channel(current_addr)
        stub = pulsar_pb2_grpc.BrokerServiceStub(channel)
        try:
            response = stub.Publish(pulsar_pb2.PublishRequest(topic=topic, key=key, payload=payload,))
        except grpc.RpcError as exc:
            return None, current_addr, exc.details()

        if response.status == pulsar_pb2.STATUS_OK:
            return response, current_addr, None

        if response.status == pulsar_pb2.STATUS_REDIRECT and response.redirect_broker.address:
            current_addr = response.redirect_broker.address
            attempts += 1
            continue

        return response, current_addr, None

    return response, current_addr, None


def create_topic(client, topic):
    """Asks the coordinator to create a topic and returns the default broker for it"""
    response = client.call("CreateTopic", pulsar_pb2.CreateTopicRequest(topic_name=topic))
    if response:
        return response.default_broker
    return None


def list_topics(client):
    return client.call("ListTopics", pulsar_pb2.ListTopicsRequest())


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar producer")
    parser.add_argument("--coordinators-file", required=True, help="Coordinator addresses file")
    parser.add_argument("--max-redirects", type=int, default=2, help="Maximum redirect retries")
    parser.add_argument("--id", required=True, help="Producer id")
    args = parser.parse_args()

    client = CoordinatorClient(args.coordinators_file)

    os.makedirs("logs", exist_ok=True)
    state_file = os.path.join("logs", f"{args.id}.json")
    topic_brokers = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                topic_brokers = json.load(f)
            log_event(f"Loaded {len(topic_brokers)} topics from state")
        except Exception as e:
            log_error(f"Failed to load state: {e}")

    def save_state():
        with open(state_file, "w") as f:
            json.dump(topic_brokers, f)

    log_event("Commands: list_topics, create_topic <topic>, send_message <topic> <message> <key>, exit")
    while True:
        try:
            raw = input("> ").strip()
        except EOFError:
            log_event("exiting")
            break

        if not raw:
            continue

        parts = shlex.split(raw)
        command = parts[0]

        if command == "exit":
            log_event("exiting")
            break

        if command == "create_topic":
            if len(parts) != 2:
                log_error("usage: create_topic <topic>")
                continue
            topic = parts[1]
            broker = create_topic(client, topic)
            if not broker or not broker.address:
                log_error("error: coordinator did not return a broker")
                continue
            topic_brokers[topic] = broker.address
            save_state()
            log_success(f"topic_ready topic={topic} broker={broker.address}")
            continue

        if command == "send_message":
            if len(parts) < 4:
                log_error("usage: send_message <topic> <message> <key>")
                continue
            topic = parts[1]
            message = parts[2]
            key = parts[3]
            if topic not in topic_brokers:
                broker = create_topic(client, topic)
                if not broker or not broker.address:
                    log_error("error: coordinator did not return a broker")
                    continue
                topic_brokers[topic] = broker.address
                save_state()

            payload = message.encode("utf-8")
            response, target, error = publish_with_redirect(topic_brokers[topic], topic, key, payload, args.max_redirects)
            if response is None:
                log_event(f"Publish failed to reach broker {target}: {error}")
            elif response.status == pulsar_pb2.STATUS_OK:
                log_success(f"Published broker={target} partition={response.partition} offset={response.offset}")
                continue

            if response and response.status == pulsar_pb2.STATUS_REDIRECT:
                topic_brokers[topic] = target
                save_state()
                log_event(f"Redirected broker={target}")
                continue

            log_event("Publish failed, refreshing broker from coordinator")
            broker = create_topic(client, topic)
            if not broker or not broker.address:
                log_error("error: coordinator did not return a broker")
                continue
            topic_brokers[topic] = broker.address
            save_state()
            response, target, error = publish_with_redirect(topic_brokers[topic], topic, key, payload, args.max_redirects)
            if response is None:
                log_error(f"error: broker unreachable {target}: {error}")
            elif response.status == pulsar_pb2.STATUS_OK:
                log_success(f"Published broker={target} partition={response.partition} offset={response.offset}")
            elif response.status == pulsar_pb2.STATUS_REDIRECT:
                topic_brokers[topic] = target
                save_state()
                log_event(f"Redirected broker={target}")
            else:
                log_error(f"error {response.error_message}")
            continue

        if command == "list_topics":
            response = list_topics(client)
            if not response:
                log_error("error: coordinator unreachable")
                continue
            if response.status != pulsar_pb2.STATUS_OK:
                log_error(f"error {response.error_message}")
                continue
            if not response.topics:
                log_io("topics: <none>")
                continue
            log_io("topics: " + ", ".join(response.topics))
            continue

        log_error(f"unknown_command {command}")


if __name__ == "__main__":
    main()
