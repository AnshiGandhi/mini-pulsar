"""
broker/broker.py
----------------
Single-node, in-memory Pub-Sub broker (Increment 1).

Responsibilities
----------------
* Publish  – stores message in an in-memory log; fans out to every active subscriber.
* Subscribe – opens a server-side streaming RPC; yields queued messages in real time.
* Acknowledge – updates the per-consumer cursor (in-memory only).

No coordinator, no persistent storage – everything lives in dicts / queues.
"""

import sys
import os
import uuid
import time
import queue
import threading
import logging

import grpc

# ---------------------------------------------------------------------------
# Make the project root (one level up from broker/) importable so that the
# generated proto stubs in protos/ can be found with a simple import.
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTOS_DIR   = os.path.join(PROJECT_ROOT, "protos")
for _p in (PROJECT_ROOT, PROTOS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from protos import pulsar_pb2, pulsar_pb2_grpc
from concurrent import futures

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BROKER_ADDRESS = "localhost:50051"
MAX_WORKERS    = 10
CONSUMER_QUEUE_SIZE = 1000   # max buffered messages per subscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [BROKER]  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# In-memory storage primitives
# ---------------------------------------------------------------------------
class InMemoryLog:
    """Append-only log for one topic."""

    def __init__(self):
        self._messages: list[dict] = []   # [{id, payload, ts}, …]
        self._lock = threading.Lock()

    def append(self, payload: bytes) -> str:
        msg_id = str(uuid.uuid4())
        with self._lock:
            self._messages.append({
                "id": msg_id,
                "payload": payload,
                "ts": time.time(),
            })
        log.debug("Log append  msg_id=%s  len(log)=%d", msg_id, len(self._messages))
        return msg_id

    def read_from(self, start_id: str | None, max_msgs: int = 100) -> list[dict]:
        """Return up to *max_msgs* messages starting after *start_id*."""
        with self._lock:
            if not start_id:
                return list(self._messages[:max_msgs])
            for i, m in enumerate(self._messages):
                if m["id"] == start_id:
                    return list(self._messages[i + 1 : i + 1 + max_msgs])
            return []


class InMemoryStore:
    """Central in-memory store: topic logs + consumer cursors."""

    def __init__(self):
        self._logs:    dict[str, InMemoryLog] = {}
        self._cursors: dict[str, dict[str, str]] = {}   # topic → {consumer_id → last_ack_id}
        self._lock = threading.Lock()

    # --- Topic log helpers ---------------------------------------------------

    def _get_log(self, topic: str) -> InMemoryLog:
        with self._lock:
            if topic not in self._logs:
                self._logs[topic] = InMemoryLog()
                log.info("Created topic log: %s", topic)
            return self._logs[topic]

    def append(self, topic: str, payload: bytes) -> str:
        return self._get_log(topic).append(payload)

    def read_from(self, topic: str, start_id: str | None, max_msgs: int = 100):
        return self._get_log(topic).read_from(start_id, max_msgs)

    # --- Cursor helpers ------------------------------------------------------

    def get_cursor(self, topic: str, consumer_id: str) -> str | None:
        return self._cursors.get(topic, {}).get(consumer_id)

    def update_cursor(self, topic: str, consumer_id: str, msg_id: str):
        with self._lock:
            self._cursors.setdefault(topic, {})[consumer_id] = msg_id
        log.debug("Cursor  topic=%s  consumer=%s  offset=%s", topic, consumer_id, msg_id)


# ---------------------------------------------------------------------------
# Subscription registry
# ---------------------------------------------------------------------------
class SubscriptionRegistry:
    """Tracks per-topic active subscriber queues for live fan-out."""

    def __init__(self):
        self._subs: dict[str, dict[str, queue.Queue]] = {}
        self._lock = threading.Lock()

    def add(self, topic: str, consumer_id: str) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=CONSUMER_QUEUE_SIZE)
        with self._lock:
            self._subs.setdefault(topic, {})[consumer_id] = q
        log.info("Subscriber added   topic=%s  consumer=%s", topic, consumer_id)
        return q

    def remove(self, topic: str, consumer_id: str):
        with self._lock:
            if topic in self._subs:
                self._subs[topic].pop(consumer_id, None)
        log.info("Subscriber removed topic=%s  consumer=%s", topic, consumer_id)

    def fan_out(self, topic: str, msg_id: str, payload: bytes):
        """Push message to every active subscriber queue for the topic."""
        with self._lock:
            subscribers = dict(self._subs.get(topic, {}))
        for cid, q in subscribers.items():
            try:
                q.put_nowait({"id": msg_id, "payload": payload})
                log.debug("Fan-out  topic=%s  consumer=%s  msg_id=%s", topic, cid, msg_id)
            except queue.Full:
                log.warning("Queue full for consumer=%s  topic=%s – message dropped", cid, topic)


# ---------------------------------------------------------------------------
# gRPC Servicer
# ---------------------------------------------------------------------------
class ClientBrokerServicer(pulsar_pb2_grpc.ClientBrokerServiceServicer):

    def __init__(self, store: InMemoryStore, registry: SubscriptionRegistry):
        self._store    = store
        self._registry = registry

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------
    def Publish(self, request, context):
        topic   = request.topic
        payload = request.payload

        if not topic:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("topic must not be empty")
            return pulsar_pb2.PublishResponse(success=False, message="topic empty")

        msg_id = self._store.append(topic, payload)
        self._registry.fan_out(topic, msg_id, payload)

        log.info("Publish  topic=%-20s  msg_id=%s", topic, msg_id)
        return pulsar_pb2.PublishResponse(success=True, message=msg_id)

    # ------------------------------------------------------------------
    # Subscribe  (server-side streaming)
    # ------------------------------------------------------------------
    def Subscribe(self, request, context):
        topic       = request.topic
        consumer_id = request.consumer_id

        if not topic or not consumer_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("topic and consumer_id must not be empty")
            return

        log.info("Subscribe  topic=%-20s  consumer=%s", topic, consumer_id)

        # 1. Replay any messages the consumer hasn't seen yet
        cursor = self._store.get_cursor(topic, consumer_id)
        backlog = self._store.read_from(topic, cursor)
        for m in backlog:
            if not context.is_active():
                return
            log.debug("Replay msg_id=%s to consumer=%s", m["id"], consumer_id)
            yield pulsar_pb2.MessageDelivery(
                message_id=m["id"],
                payload=m["payload"],
            )

        # 2. Register for live messages
        q = self._registry.add(topic, consumer_id)
        try:
            while context.is_active():
                try:
                    msg = q.get(timeout=1.0)
                    yield pulsar_pb2.MessageDelivery(
                        message_id=msg["id"],
                        payload=msg["payload"],
                    )
                except queue.Empty:
                    continue   # keep checking context.is_active()
        finally:
            self._registry.remove(topic, consumer_id)

    # ------------------------------------------------------------------
    # Acknowledge
    # ------------------------------------------------------------------
    def Acknowledge(self, request, context):
        self._store.update_cursor(request.topic, request.consumer_id, request.message_id)
        log.info(
            "Ack  topic=%-20s  consumer=%-20s  msg_id=%s",
            request.topic, request.consumer_id, request.message_id,
        )
        return pulsar_pb2.AckResponse(success=True)


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
def serve():
    store    = InMemoryStore()
    registry = SubscriptionRegistry()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    pulsar_pb2_grpc.add_ClientBrokerServiceServicer_to_server(
        ClientBrokerServicer(store, registry), server
    )
    server.add_insecure_port(BROKER_ADDRESS)
    server.start()
    log.info("Broker listening on %s", BROKER_ADDRESS)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Broker shutting down …")
        server.stop(grace=2)


if __name__ == "__main__":
    serve()
