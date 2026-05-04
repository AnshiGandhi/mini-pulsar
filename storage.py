import argparse
import base64
from encodings.punycode import T
import json
import os
import threading
from concurrent import futures

import grpc

from protos import pulsar_pb2, pulsar_pb2_grpc
from utils import log_event, log_success


class StorageNode(pulsar_pb2_grpc.StorageServiceServicer):
    def __init__(self, data_dir):
        self._data_dir = data_dir
        self._locks = {}
        self._next_offsets = {}
        self._global_lock = threading.Lock()

    def Append(self, request, context):
        topic = request.topic
        partition = request.partition
        key = request.key
        payload = request.payload

        lock = self._get_lock(topic, partition)
        with lock:
            file_path = self._log_path(topic, partition)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if (topic, partition) not in self._next_offsets:
                self._next_offsets[(topic, partition)] = self._count_lines(file_path)

            offset = self._next_offsets[(topic, partition)]
            record = {
                "key": key,
                "payload_b64": base64.b64encode(payload).decode("ascii"),
            }

            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record))
                f.write("\n")

            self._next_offsets[(topic, partition)] = offset + 1

        log_success(f"Append topic={topic} partition={partition} offset={offset}")

        return pulsar_pb2.AppendResponse(offset=offset)

    def Read(self, request, context):
        topic = request.topic
        partition = request.partition
        offset = request.offset
        batch_size = request.batch_size

        lock = self._get_lock(topic, partition)
        with lock:
            file_path = self._log_path(topic, partition)
            if not os.path.exists(file_path):
                return pulsar_pb2.ReadResponse(messages=[], next_offset=offset)

            messages = []
            next_offset = offset

            with open(file_path, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if idx < offset:
                        continue
                    if len(messages) >= batch_size:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    payload = base64.b64decode(record["payload_b64"].encode("ascii"))
                    msg = pulsar_pb2.Message(
                        topic=topic,
                        partition=partition,
                        offset=idx,
                        key=record.get("key", ""),
                        payload=payload,
                    )
                    messages.append(msg)
                    next_offset = idx + 1

        if messages:
            log_event(f"Read topic={topic} partition={partition} count={len(messages)}")

        return pulsar_pb2.ReadResponse(messages=messages, next_offset=next_offset)

    def _log_path(self, topic, partition):
        safe_topic = topic.replace("/", "_")
        return os.path.join(self._data_dir, safe_topic, f"partition_{partition}.log")

    def _get_lock(self, topic, partition):
        key = (topic, partition)
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def _count_lines(self, file_path):
        if not os.path.exists(file_path):
            return 0
        count = 0
        with open(file_path, "r", encoding="utf-8") as f:
            for _ in f:
                count += 1
        return count


def register_with_coordinator(coordinator_addr, listen_addr, node_id):
    channel = grpc.insecure_channel(coordinator_addr)
    stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
    response = stub.Register(pulsar_pb2.RegisterRequest(node_type=pulsar_pb2.NODE_TYPE_STORAGE, node_id=node_id, address=listen_addr))
    log_success(f"Registered storage_id={response.node_id} coordinator={coordinator_addr}")


def serve(args):
    os.makedirs(args.data_dir, exist_ok=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pulsar_pb2_grpc.add_StorageServiceServicer_to_server(StorageNode(args.data_dir), server)

    listen_addr = f"{args.host}:{args.port}"
    server.add_insecure_port(listen_addr)

    if args.coordinator:
        register_with_coordinator(args.coordinator, listen_addr, args.id)

    server.start()
    log_success(f"Storage listening on {listen_addr}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log_event("Storage shutting down")
    finally:
        server.stop(grace=2)


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar storage node")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--data-dir", required=True, help="Storage data directory")
    parser.add_argument("--coordinator", required=True, help="Coordinator address host:port")
    parser.add_argument("--id", required=True, help="Storage id")
    args = parser.parse_args()

    serve(args)


if __name__ == "__main__":
    main()
