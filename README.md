# Mini-Pulsar

A decoupled distributed publish-subscribe messaging system inspired by Apache Pulsar.
Built with **Python + gRPC** as a local prototype.

---

## Architecture Overview

```
Producer ──► Broker ──► Consumer
               │
          In-Memory Log
          (per-topic append-only store)
```

The system is built in increments:

| Increment | Description | Status |
|-----------|-------------|--------|
| 1 | Core Pub-Sub — single node, in-memory | ✅ Done |
| 2 | Persistent Storage Node | 🔜 |
| 3 | Coordinator + Topic Discovery | 🔜 |
| 4 | Consensus (Phase King) | 🔜 |

---

## Increment 1: Core Pub-Sub (In-Memory / Single Node)

### What's implemented

| Component | File | Description |
|-----------|------|-------------|
| Broker | `broker/broker.py` | gRPC server — Publish, Subscribe (streaming), Acknowledge |
| Producer | `client/producer.py` | CLI client to publish messages |
| Consumer | `client/consumer.py` | CLI client to subscribe and ack messages |
| E2E Tests | `tests/test_e2e.py` | 5 automated tests covering all flows |

### Key design decisions

- **In-memory log** per topic — append-only `list` guarded by a `threading.Lock`
- **Per-consumer cursors** — stored in a dict; survive reconnects within a session
- **Live fan-out** via per-subscriber `queue.Queue` — no polling
- **Backlog replay** — new subscribers receive all unacknowledged messages before live stream
- **Coordinator skipped** — broker address hardcoded to `localhost:50051`

---

## Project Layout

```
mini-pulsar/
├── protos/
│   ├── pulsar.proto          # gRPC service & message definitions
│   ├── pulsar_pb2.py         # generated
│   └── pulsar_pb2_grpc.py    # generated
├── broker/
│   └── broker.py             # Single-node in-memory broker
├── client/
│   ├── producer.py           # Producer CLI
│   └── consumer.py           # Consumer CLI
├── tests/
│   └── test_e2e.py           # End-to-end test suite
├── conftest.py               # pytest sys.path setup
└── README.md
```

---

## Quick Start

### 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install grpcio grpcio-tools pytest
```

### 2. Run the Broker

```bash
venv/bin/python3 -m broker.broker
# 20:51:56  [BROKER]  INFO  Broker listening on localhost:50051
```

### 3. Subscribe (in a new terminal)

```bash
venv/bin/python3 -m client.consumer --topic my-topic --consumer-id consumer-1
```

### 4. Publish (in another terminal)

```bash
venv/bin/python3 -m client.producer --topic my-topic --payload "Hello, Pulsar!" --count 5
```

### Producer options

| Flag | Default | Description |
|------|---------|-------------|
| `--topic` | `test-topic` | Topic name |
| `--payload` | `Hello, Pulsar!` | Message text |
| `--count` | `1` | Number of messages to send |
| `--broker` | `localhost:50051` | Broker address |

### Consumer options

| Flag | Default | Description |
|------|---------|-------------|
| `--topic` | `test-topic` | Topic to subscribe to |
| `--consumer-id` | `consumer-1` | Unique consumer identifier |
| `--broker` | `localhost:50051` | Broker address |

---

## Running Tests

```bash
venv/bin/python3 -m pytest tests/test_e2e.py -v
```

Expected output:

```
tests/test_e2e.py::TestCorePublishSubscribe::test_acknowledge_updates_cursor   PASSED
tests/test_e2e.py::TestCorePublishSubscribe::test_backlog_replay_on_reconnect  PASSED
tests/test_e2e.py::TestCorePublishSubscribe::test_multiple_consumers_same_topic PASSED
tests/test_e2e.py::TestCorePublishSubscribe::test_publish_and_subscribe_live   PASSED
tests/test_e2e.py::TestCorePublishSubscribe::test_publish_single_message       PASSED

5 passed in 3.34s
```

---

## Live Demo Output (Increment 1)

```
20:51:56  [BROKER]    INFO  Broker listening on localhost:50051
20:51:57  [CONSUMER]  INFO  Subscribing  topic=demo-topic  consumer=demo-consumer
20:51:58  [CONSUMER]  INFO  Received  msg_id=a7d79c66-...  payload='Message [1/3]'
20:51:58  [PRODUCER]  INFO  Published  topic=demo-topic    msg_id=a7d79c66-...
20:51:58  [CONSUMER]  INFO  Acked     msg_id=a7d79c66-...
20:51:58  [CONSUMER]  INFO  Received  msg_id=8e6726ae-...  payload='Message [2/3]'
20:51:58  [PRODUCER]  INFO  Published  topic=demo-topic    msg_id=8e6726ae-...
20:51:58  [CONSUMER]  INFO  Acked     msg_id=8e6726ae-...
20:51:58  [CONSUMER]  INFO  Received  msg_id=1604b74d-...  payload='Message [3/3]'
20:51:58  [PRODUCER]  INFO  Published  topic=demo-topic    msg_id=1604b74d-...
20:51:58  [CONSUMER]  INFO  Acked     msg_id=1604b74d-...
```
