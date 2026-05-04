import argparse
import hashlib
import threading
import time
from concurrent import futures

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc


from utils import DEFAULT_PARTITIONS, log_error, log_event, log_success, log_io


class Broker(pulsar_pb2_grpc.BrokerServiceServicer):
    def __init__(self, broker_id, listen_addr, coordinator_stub=None):
        self._broker_id = broker_id
        self._listen_addr = listen_addr
        self._routes = {}
        self._storage_stubs = {}
        self._default_partitions = DEFAULT_PARTITIONS
        self._coordinator_stub = coordinator_stub

    def Publish(self, request, context):
        topic = request.topic
        if not topic:
            log_error("Publish missing topic")
            return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_ERROR, error_message="missing topic")

        partition = self._select_partition(topic, request.key, request.payload)
        if partition is None:
            log_error(f"Publish unknown topic {topic}")
            return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_ERROR, error_message="unknown topic")

        route = self._routes.get((topic, partition))
        if not route and self._refresh_routes():
            route = self._routes.get((topic, partition))
        if not route:
            log_error(f"Publish no route topic={topic} partition={partition}")
            return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_ERROR, error_message="no route for partition")

        if route.broker.address != self._listen_addr:
            log_event(f"Publish redirect topic={topic} partition={partition} to={route.broker.address}")

            return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_REDIRECT, redirect_broker=route.broker, partition=partition)

        storage_stub = self._get_storage_stub(route.storage.address)
        try:
            append_resp = storage_stub.Append(pulsar_pb2.AppendRequest(topic=topic, partition=partition, key=request.key, payload=request.payload,))
        except grpc.RpcError as exc:
            log_error(f"publish storage error {exc.details()}")
            return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_ERROR, error_message=exc.details(), partition=partition)

        log_success(f"Publish ok topic={topic} partition={partition} offset={append_resp.offset}")
        return pulsar_pb2.PublishResponse(status=pulsar_pb2.STATUS_OK, partition=partition, offset=append_resp.offset)

    def ReadMessage(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "ReadMessage removed; use Subscribe")

    def AssignPartition(self, request, context):
        if request.routes:
            self._routes = {
                (route.topic, route.partition): route for route in request.routes
            }
            log_event(f"Routing table refreshed routes={len(self._routes)}")
        else:
            route = pulsar_pb2.PartitionRoute(
                topic=request.topic,
                partition=request.partition,
                broker=pulsar_pb2.BrokerInfo(
                    broker_id=self._broker_id,
                    address=self._listen_addr,
                ),
                storage=request.storage,
            )
            self._routes[(route.topic, route.partition)] = route

            log_event(f"Assigned topic={route.topic} partition={route.partition} storage={route.storage.address}")

        return pulsar_pb2.AssignPartitionResponse(ok=True, message="assigned")

    def Subscribe(self, request, context):
        topic = request.topic
        requested_offsets = {po.partition: po.offset for po in request.start_offsets}
        owned_partitions = [
            partition
            for (route_topic, partition), route in self._routes.items()
            if route_topic == topic and route.broker.address == self._listen_addr
        ]

        if not owned_partitions and self._refresh_routes():
            owned_partitions = [
                partition
                for (route_topic, partition), route in self._routes.items()
                if route_topic == topic and route.broker.address == self._listen_addr
            ]

        if not owned_partitions:
            redirect = self._redirect_for_topic(topic)
            if redirect:
                log_event(f"subscribe redirect topic={topic} to={redirect.address}")
                yield pulsar_pb2.SubscribeResponse(status=pulsar_pb2.STATUS_REDIRECT, redirect_broker=redirect)
                return
            log_error(f"subscribe no partitions topic={topic}")
            yield pulsar_pb2.SubscribeResponse(status=pulsar_pb2.STATUS_ERROR, error_message="no partitions for topic")
            return

        for partition in requested_offsets:
            if partition not in owned_partitions:
                redirect = self._redirect_for_partition(topic, partition)
                if redirect:
                    log_event(f"subscribe redirect topic={topic} partition={partition} to={redirect.address}")
                    yield pulsar_pb2.SubscribeResponse(status=pulsar_pb2.STATUS_REDIRECT, redirect_broker=redirect)
                    return

        offsets = {
            partition: requested_offsets.get(partition, 0)
            for partition in owned_partitions
        }
        batch_size = 10
        poll_interval = 0.25

        while context.is_active():
            any_sent = False
            for partition in owned_partitions:
                route = self._routes.get((topic, partition))
                if not route:
                    continue
                storage_stub = self._get_storage_stub(route.storage.address)
                try:
                    read_resp = storage_stub.Read(pulsar_pb2.ReadRequest(topic=topic, partition=partition, offset=offsets[partition], batch_size=batch_size))
                except grpc.RpcError as exc:
                    log_error(f"subscribe storage error {exc.details()}")
                    yield pulsar_pb2.SubscribeResponse(status=pulsar_pb2.STATUS_ERROR, error_message=exc.details())
                    return

                if read_resp.messages:
                    offsets[partition] = read_resp.next_offset
                    any_sent = True
                    log_io(
                        f"subscribe batch topic={topic} partition={partition} count={len(read_resp.messages)}"
                    )
                    yield pulsar_pb2.SubscribeResponse(status=pulsar_pb2.STATUS_OK, batch=pulsar_pb2.MessageBatch(messages=read_resp.messages))

            if not any_sent:
                time.sleep(poll_interval)

    def _get_storage_stub(self, address):
        if address not in self._storage_stubs:
            channel = grpc.insecure_channel(address)
            self._storage_stubs[address] = pulsar_pb2_grpc.StorageServiceStub(channel)
        return self._storage_stubs[address]

    def _refresh_routes(self):
        if not self._coordinator_stub:
            return False
        try:
            response = self._coordinator_stub.GetRoutingTable(pulsar_pb2.GetRoutingTableRequest())
        except grpc.RpcError as exc:
            log_error(f"Routing refresh failed {exc.details()}")
            return False

        if not response.routes:
            return False
        self._routes = {
            (route.topic, route.partition): route for route in response.routes
        }
        log_event(f"Routing refreshed routes={len(self._routes)}")
        return True

    def _select_partition(self, topic, key, payload):
        key_bytes = key.encode("utf-8") if key else payload
        digest = hashlib.sha256(key_bytes).hexdigest()
        return int(digest, 16) % self._default_partitions

    def _redirect_for_partition(self, topic, partition):
        route = self._routes.get((topic, partition))
        if route:
            return route.broker
        return None

    def _redirect_for_topic(self, topic):
        for (route_topic, _partition), route in self._routes.items():
            if route_topic == topic:
                return route.broker
        return None


def register_with_coordinator(coordinator_addr, listen_addr):
    channel = grpc.insecure_channel(coordinator_addr)
    stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
    response = stub.Register(pulsar_pb2.RegisterRequest(node_type=pulsar_pb2.NODE_TYPE_BROKER, node_id="", address=listen_addr))
    return stub, response.node_id


def heartbeat_loop(stub, node_id, listen_addr, interval, stop_event):
    while not stop_event.is_set():
        try:
            stub.Heartbeat(pulsar_pb2.HeartbeatRequest(broker_id=node_id, address=listen_addr))
        except grpc.RpcError:
            log_error("Heartbeat failed")
        stop_event.wait(interval)


def serve(args):
    listen_addr = f"{args.host}:{args.port}"
    broker_id = "broker-local"
    stub = None
    stop_event = threading.Event()
    if args.coordinator:
        stub, broker_id = register_with_coordinator(args.coordinator, listen_addr)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    broker = Broker(broker_id, listen_addr, coordinator_stub=stub)
    pulsar_pb2_grpc.add_BrokerServiceServicer_to_server(broker, server)

    server.add_insecure_port(listen_addr)

    if args.coordinator and stub:
        thread = threading.Thread(target=heartbeat_loop, args=(stub, broker_id, listen_addr, 5, stop_event), daemon=True)
        thread.start()
        log_success(f"Registered broker_id={broker_id} coordinator={args.coordinator}")

    server.start()
    log_success(f"Broker listening on {listen_addr}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log_event("Broker shutting down")
    finally:
        stop_event.set()
        server.stop(grace=2)


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar broker")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--coordinator", required=True, help="Coordinator address host:port")
    args = parser.parse_args()

    serve(args)


if __name__ == "__main__":
    main()
