"""
client/producer.py
------------------
Simple Producer client for Increment 1.

Usage
-----
    # publish a single message (default topic & payload)
    python -m client.producer

    # publish with custom topic / payload
    python -m client.producer --topic my-topic --payload "Hello World" --count 5
"""

import sys
import os
import argparse
import logging

import grpc

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTOS_DIR   = os.path.join(PROJECT_ROOT, "protos")
for _p in (PROJECT_ROOT, PROTOS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from protos import pulsar_pb2, pulsar_pb2_grpc

BROKER_ADDRESS = "localhost:50051"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER]  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def publish(topic: str, payload: str, count: int = 1, broker: str = BROKER_ADDRESS):
    with grpc.insecure_channel(broker) as channel:
        stub = pulsar_pb2_grpc.ClientBrokerServiceStub(channel)

        for i in range(count):
            msg_payload = payload if count == 1 else f"{payload} [{i+1}/{count}]"
            request = pulsar_pb2.PublishRequest(
                topic=topic,
                payload=msg_payload.encode("utf-8"),
            )
            try:
                response = stub.Publish(request)
                if response.success:
                    log.info("Published  topic=%-20s  msg_id=%s", topic, response.message)
                else:
                    log.error("Publish failed: %s", response.message)
            except grpc.RpcError as e:
                log.error("gRPC error: %s – %s", e.code(), e.details())
                sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar Producer")
    parser.add_argument("--topic",   default="test-topic",    help="Topic name")
    parser.add_argument("--payload", default="Hello, Pulsar!", help="Message payload")
    parser.add_argument("--count",   type=int, default=1,     help="Number of messages to send")
    parser.add_argument("--broker",  default=BROKER_ADDRESS,   help="Broker address")
    args = parser.parse_args()

    publish(topic=args.topic, payload=args.payload, count=args.count, broker=args.broker)


if __name__ == "__main__":
    main()
