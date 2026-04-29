"""
client/consumer.py
------------------
Simple Consumer client

Usage
-----
    # subscribe to default topic (blocks until Ctrl-C)
    python -m client.consumer

    # subscribe with a custom consumer ID and topic
    python -m client.consumer --topic my-topic --consumer-id consumer-A
"""

import sys
import os
import argparse
import logging
import signal

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
    format="%(asctime)s  [CONSUMER]  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def subscribe(topic: str, consumer_id: str, broker: str = BROKER_ADDRESS):
    log.info("Connecting to broker at %s …", broker)
    log.info("Subscribing  topic=%-20s  consumer=%s", topic, consumer_id)

    with grpc.insecure_channel(broker) as channel:
        stub = pulsar_pb2_grpc.ClientBrokerServiceStub(channel)

        request = pulsar_pb2.SubscribeRequest(
            topic=topic,
            consumer_id=consumer_id,
        )

        try:
            for delivery in stub.Subscribe(request):
                payload_text = delivery.payload.decode("utf-8", errors="replace")
                log.info(
                    "Received  msg_id=%-38s  payload=%r",
                    delivery.message_id,
                    payload_text,
                )
                # Acknowledge immediately
                ack = pulsar_pb2.AckRequest(
                    topic=topic,
                    consumer_id=consumer_id,
                    message_id=delivery.message_id,
                )
                ack_resp = stub.Acknowledge(ack)
                if ack_resp.success:
                    log.info("Acked     msg_id=%s", delivery.message_id)
                else:
                    log.warning("Ack failed for msg_id=%s", delivery.message_id)

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                log.info("Stream cancelled – shutting down.")
            else:
                log.error("gRPC error: %s – %s", e.code(), e.details())
        except KeyboardInterrupt:
            log.info("Consumer interrupted – shutting down.")


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar Consumer")
    parser.add_argument("--topic",       default="test-topic",  help="Topic to subscribe to")
    parser.add_argument("--consumer-id", default="consumer-1",  help="Unique consumer ID")
    parser.add_argument("--broker",      default=BROKER_ADDRESS, help="Broker address")
    args = parser.parse_args()

    # Graceful Ctrl-C
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

    subscribe(
        topic=args.topic,
        consumer_id=args.consumer_id,
        broker=args.broker,
    )


if __name__ == "__main__":
    main()
