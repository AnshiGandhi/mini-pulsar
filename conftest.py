# conftest.py — project-root pytest configuration
# Ensures that both the project root AND the protos directory are on sys.path.
# This satisfies the flat `import pulsar_pb2` inside the generated grpc stub,
# while also allowing `from protos import pulsar_pb2, pulsar_pb2_grpc` everywhere else.

import sys, os

ROOT   = os.path.dirname(__file__)
PROTOS = os.path.join(ROOT, "protos")

for p in (ROOT, PROTOS):
    if p not in sys.path:
        sys.path.insert(0, p)
