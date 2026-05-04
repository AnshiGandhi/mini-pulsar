import argparse
import shlex

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc
from utils import DEFAULT_PARTITIONS, log_error, log_event, log_success, log_io


def publish_with_redirect(broker_addr, topic, key, payload, max_redirects):
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


def create_topic(coordinator_addr, topic):
    channel = grpc.insecure_channel(coordinator_addr)
    stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
    response = stub.CreateTopic(pulsar_pb2.CreateTopicRequest( topic_name=topic))
    return response.default_broker


def list_topics(coordinator_addr):
    channel = grpc.insecure_channel(coordinator_addr)
    stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
    return stub.ListTopics(pulsar_pb2.ListTopicsRequest())


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar producer")
    parser.add_argument("--coordinator", required=True, help="Coordinator address host:port")
    parser.add_argument("--max-redirects", type=int, default=2, help="Maximum redirect retries")
    args = parser.parse_args()

    topic_brokers = {}

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
            broker = create_topic(args.coordinator, topic)
            if not broker or not broker.address:
                log_error("error: coordinator did not return a broker")
                continue
            topic_brokers[topic] = broker.address
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
                broker = create_topic(args.coordinator, topic)
                if not broker or not broker.address:
                    log_error("error: coordinator did not return a broker")
                    continue
                topic_brokers[topic] = broker.address

            payload = message.encode("utf-8")
            response, target, error = publish_with_redirect(topic_brokers[topic], topic, key, payload, args.max_redirects)
            if response is None:
                log_event(f"Publish failed to reach broker {target}: {error}")
            elif response.status == pulsar_pb2.STATUS_OK:
                log_success(f"Published broker={target} partition={response.partition} offset={response.offset}")
                continue

            if response and response.status == pulsar_pb2.STATUS_REDIRECT:
                topic_brokers[topic] = target
                log_event(f"Redirected broker={target}")
                continue

            log_event("Publish failed, refreshing broker from coordinator")
            broker = create_topic(args.coordinator, topic)
            if not broker or not broker.address:
                log_error("error: coordinator did not return a broker")
                continue
            topic_brokers[topic] = broker.address
            response, target, error = publish_with_redirect(topic_brokers[topic], topic, key, payload, args.max_redirects)
            if response is None:
                log_error(f"error: broker unreachable {target}: {error}")
            elif response.status == pulsar_pb2.STATUS_OK:
                log_success(f"Published broker={target} partition={response.partition} offset={response.offset}")
            elif response.status == pulsar_pb2.STATUS_REDIRECT:
                topic_brokers[topic] = target
                log_event(f"Redirected broker={target}")
            else:
                log_error(f"error {response.error_message}")
            continue

        if command == "list_topics":
            response = list_topics(args.coordinator)
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
