import argparse
import threading
import time
from concurrent import futures

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc

from utils import DEFAULT_PARTITIONS, log_error, log_event, log_success


class Coordinator(pulsar_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._brokers = {}
        self._storages = {}
        self._routes = {}
        self._topic_partitions = {}
        self._broker_stubs = {}
        self._heartbeats = {}

    def Register(self, request, context):
        with self._lock:
            if not request.node_id:
                log_error("Register missing node_id")
                return pulsar_pb2.RegisterResponse(ok=False, message="missing node_id", node_id="")

            node_id = request.node_id
            if request.node_type == pulsar_pb2.NODE_TYPE_BROKER:
                self._brokers[node_id] = request.address
                self._heartbeats[node_id] = {
                    "address": request.address,
                    "last_seen": time.time(),
                    "missed": 0,
                }
                log_success(f"Broker registered id={node_id} addr={request.address}")
            elif request.node_type == pulsar_pb2.NODE_TYPE_STORAGE:
                self._storages[node_id] = request.address
                log_success(f"Storage registered id={node_id} addr={request.address}")
            return pulsar_pb2.RegisterResponse(ok=True, message="registered", node_id=node_id)

    def Heartbeat(self, request, context):
        with self._lock:
            if request.broker_id:
                self._brokers[request.broker_id] = request.address
                heartbeat = self._heartbeats.get(request.broker_id)
                if not heartbeat:
                    heartbeat = {
                        "address": request.address,
                        "last_seen": 0,
                        "missed": 0,
                    }
                    self._heartbeats[request.broker_id] = heartbeat
                heartbeat["address"] = request.address
                heartbeat["last_seen"] = time.time()
                heartbeat["missed"] = 0
        return pulsar_pb2.HeartbeatResponse(ok=True, message="ok")

    def CreateTopic(self, request, context):
        topic = request.topic_name
        num_partitions = DEFAULT_PARTITIONS
        with self._lock:
            if topic in self._topic_partitions:
                routes = self._routes_for_topic(topic)
                default_broker = routes[0].broker if routes else pulsar_pb2.BrokerInfo()
                log_event(f"topic={topic} exists")
                return pulsar_pb2.CreateTopicResponse(default_broker=default_broker, routes=routes)

            if not self._brokers or not self._storages:
                log_error("create_topic missing brokers or storages")
                return pulsar_pb2.CreateTopicResponse(default_broker=pulsar_pb2.BrokerInfo(), routes=[])

            broker_list = self._sorted_nodes(self._brokers)
            storage_list = self._sorted_nodes(self._storages)
            self._topic_partitions[topic] = num_partitions

            new_routes = []
            for partition in range(num_partitions):
                broker_id, broker_addr = broker_list[partition % len(broker_list)]
                storage_id, storage_addr = storage_list[partition % len(storage_list)]
                route = pulsar_pb2.PartitionRoute(
                    topic=topic,
                    partition=partition,
                    broker=pulsar_pb2.BrokerInfo(
                        broker_id=broker_id,
                        address=broker_addr,
                    ),
                    storage=pulsar_pb2.StorageInfo(
                        storage_id=storage_id,
                        address=storage_addr,
                    ),
                )
                self._routes[(topic, partition)] = route
                new_routes.append(route)

            default_broker = new_routes[0].broker if new_routes else pulsar_pb2.BrokerInfo()
            all_routes = list(self._routes.values())

        self._notify_brokers(all_routes)
        log_success(f"Topic={topic} created partitions={num_partitions}")
        return pulsar_pb2.CreateTopicResponse(default_broker=default_broker, routes=new_routes)

    def SubscribeMessage(self, request, context):
        with self._lock:
            routes = self._routes_for_topic(request.topic_name)
        if not routes:
            log_error(f"subscribe topic not found topic={request.topic_name}")
            return pulsar_pb2.SubscribeMessageResponse(status=pulsar_pb2.STATUS_ERROR, error_message="Topic not found", routes=[])

        return pulsar_pb2.SubscribeMessageResponse(status=pulsar_pb2.STATUS_OK, error_message="", routes=routes)

    def ListTopics(self, request, context):
        with self._lock:
            topics = sorted(self._topic_partitions.keys())
        return pulsar_pb2.ListTopicsResponse(status=pulsar_pb2.STATUS_OK, error_message="", topics=topics)

    def GetRoutingTable(self, request, context):
        with self._lock:
            routes = list(self._routes.values())
        return pulsar_pb2.GetRoutingTableResponse(routes=routes)

    def _routes_for_topic(self, topic):
        return [route for (route_topic, _), route in self._routes.items() if route_topic == topic]

    def _sorted_nodes(self, nodes):
        return sorted(nodes.items(), key=lambda item: item[0])

    def _notify_brokers(self, routes):
        for broker_id, broker_addr in self._brokers.items():
            stub = self._get_broker_stub(broker_addr)
            try:
                stub.AssignPartition(pulsar_pb2.AssignPartitionRequest(topic="", partition=0, storage=pulsar_pb2.StorageInfo(), routes=routes))
            except grpc.RpcError:
                log_error(f"Notify failed for broker_id={broker_id}")
                continue

    def _get_broker_stub(self, broker_addr):
        if broker_addr not in self._broker_stubs:
            channel = grpc.insecure_channel(broker_addr)
            self._broker_stubs[broker_addr] = pulsar_pb2_grpc.BrokerServiceStub(channel)
        return self._broker_stubs[broker_addr]

    def monitor_heartbeats(self, interval, max_missed):
        while True:
            time.sleep(interval)
            failed = []
            now = time.time()
            with self._lock:
                for broker_id, heartbeat in list(self._heartbeats.items()):
                    if now - heartbeat["last_seen"] > interval:
                        heartbeat["missed"] += 1
                    else:
                        heartbeat["missed"] = 0

                    if heartbeat["missed"] >= max_missed:
                        failed.append(broker_id)

                for broker_id in failed:
                    self._brokers.pop(broker_id, None)
                    self._heartbeats.pop(broker_id, None)
                    log_error(f"broker failed id={broker_id}")

                if failed:
                    self._reassign_partitions(failed)

    def _reassign_partitions(self, failed_brokers):
        if not self._brokers:
            return

        broker_list = self._sorted_nodes(self._brokers)
        broker_count = len(broker_list)
        if broker_count == 0:
            return

        idx = 0
        for key, route in list(self._routes.items()):
            if route.broker.broker_id in failed_brokers:
                broker_id, broker_addr = broker_list[idx % broker_count]
                idx += 1
                route.broker.broker_id = broker_id
                route.broker.address = broker_addr
                self._routes[key] = route
                log_event(f"Reassigned topic={route.topic} partition={route.partition} broker={broker_id}")

        all_routes = list(self._routes.values())
        self._notify_brokers(all_routes)


def serve(args):
    coordinator = Coordinator()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pulsar_pb2_grpc.add_CoordinatorServiceServicer_to_server(coordinator, server)

    listen_addr = f"{args.host}:{args.port}"
    server.add_insecure_port(listen_addr)
    server.start()
    log_success(f"Coordinator listening on {listen_addr}")

    thread = threading.Thread(
        target=coordinator.monitor_heartbeats,
        args=(5, 3),
        daemon=True,
    )
    thread.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log_event("Coordinator shutting down")
    finally:
        server.stop(grace=2)


def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar coordinator")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=4000, help="Bind port")
    args = parser.parse_args()

    serve(args)


if __name__ == "__main__":
    main()
