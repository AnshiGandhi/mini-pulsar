# Mini-Pulsar

A Decoupled Distributed Publish-Subscribe System built with Python and gRPC, simulating Apache Pulsar's multi-tier architecture.

## Setup Instructions

### 1. Create a Virtual Environment

It is recommended to run this project inside a Python virtual environment to manage dependencies locally.

```bash
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate
```

### 2. Install Dependencies

With the virtual environment activated, install the required gRPC and Protobuf libraries:

```bash
pip3 install -r requirements.txt
```

### 3. Compile Protobuf Files

Whenever you make changes to `protos/pulsar.proto`, you need to recompile the gRPC Python stubs. Run the following command from the root directory (make sure your virtual environment is active):

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./protos/pulsar.proto
```

---

## Run Order (Multi-Process)

Before running the cluster, create a `coordinators.txt` file containing the addresses of your coordinator nodes (one per line). For a local multi-node setup:

```text
127.0.0.1:4000
127.0.0.1:4001
127.0.0.1:4002
```

Open separate terminals for each component and run in this order:

1. Coordinator Cluster (Start multiple instances for Raft leader election)

    ```bash
    python3 coordinator.py --port 4000 --coordinators-file coordinators.txt
    python3 coordinator.py --port 4001 --coordinators-file coordinators.txt
    python3 coordinator.py --port 4002 --coordinators-file coordinators.txt
    ```

2. Storage

    ```bash
    python3 storage.py --coordinators-file coordinators.txt --port 6000 --data-dir data/storage --id storage-1
    ```

3. Broker

    ```bash
    python3 broker.py --coordinators-file coordinators.txt --port 8000 --id broker-1
    ```

4. Producer (interactive)

    ```bash
    python3 producer.py --coordinators-file coordinators.txt
    ```

5. Consumer (interactive)

    ```bash
    python3 consumer.py --coordinators-file coordinators.txt --id consumer-1
    ```

### Producer Commands

- `create_topic <topic>`
- `send_message <topic> <message> <key>`
- `list_topics`
- `exit`

### Consumer Commands

- `subscribe_topic <topic>`
- `unsubscribe_topic <topic>`
- `list_topics`
- `list_subscriptions`
- `exit`
