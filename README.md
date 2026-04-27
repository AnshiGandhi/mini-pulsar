# Mini-Pulsar

A Decoupled Distributed Publish-Subscribe System built with Python and gRPC, simulating Apache Pulsar's multi-tier architecture.

## Setup Instructions

### 1. Create a Virtual Environment

It is recommended to run this project inside a Python virtual environment to manage dependencies locally.

```bash
# Create the virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 2. Install Dependencies

With the virtual environment activated, install the required gRPC and Protobuf libraries:

```bash
pip install grpcio grpcio-tools protobuf
```

### 3. Compile Protobuf Files

Whenever you make changes to `protos/pulsar.proto`, you need to recompile the gRPC Python stubs. Run the following command from the root directory (make sure your virtual environment is active):

```bash
python -m grpc_tools.protoc -I./protos --python_out=./protos --grpc_python_out=./protos ./protos/pulsar.proto
```

---

*Note: Instructions for running the Broker, Coordinator, Storage Nodes, and Clients will be added here as the system components are implemented.*
