import argparse
import threading
import time
import json
import os
from concurrent import futures

import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc
from utils import DEFAULT_PARTITIONS, log_error, log_event, log_success, read_coordinators
from consensus import ConsensusNode


class Coordinator(pulsar_pb2_grpc.CoordinatorServiceServicer):
    """
    Coordinator tracks cluster metadata (topics, routing, node health)
    It acts as a state delegate for the ConsensusNode to persist metadata alongside Raft state
    """
    def __init__(self, node_id, listen_addr, peers, state_file):
        self.lock = threading.Lock()
        self._node_id = node_id
        self._listen_addr = listen_addr
        self._state_file = state_file
        
        self._brokers = {}
        self._storages = {}
        self._routes = {}
        self._topic_partitions = {}
        self._broker_stubs = {}
        self._heartbeats = {}
        
        # Persisted state
        self._term = 0
        self._voted_for = None
        
        self._load_state()

        self._running = True
        
        self.consensus = ConsensusNode(node_id, listen_addr, peers, self)
        
        self._monitor_thread = threading.Thread(target=self._monitor_heartbeats, args=(5, 3), daemon=True)
        self._monitor_thread.start()

    # StateDelegate methods
    def get_term(self):
        return self._term
        
    def set_term(self, term):
        self._term = term
        
    def get_voted_for(self):
        return self._voted_for
        
    def set_voted_for(self, voted_for):
        self._voted_for = voted_for

    def get_state_json(self):
        """Serializes current routing and node state for replication via Raft"""
        routes_data = []
        for r in self._routes.values():
            routes_data.append({
                "topic": r.topic,
                "partition": r.partition,
                "broker": {"broker_id": r.broker.broker_id, "address": r.broker.address},
                "storage": {"storage_id": r.storage.storage_id, "address": r.storage.address}
            })

        data = {
            "brokers": self._brokers,
            "storages": self._storages,
            "topic_partitions": self._topic_partitions,
            "routes": routes_data
        }
        return json.dumps(data)
        
    def apply_state_json(self, state_json):
        """Applies replicated state from the Raft leader"""
        try:
            data = json.loads(state_json)
            self._brokers = data.get("brokers", {})
            self._storages = data.get("storages", {})
            self._topic_partitions = data.get("topic_partitions", {})
            
            self._routes = {}
            routes_data = data.get("routes", [])
            for r in routes_data:
                route = pulsar_pb2.PartitionRoute(
                    topic=r["topic"],
                    partition=r["partition"],
                    broker=pulsar_pb2.BrokerInfo(broker_id=r["broker"]["broker_id"], address=r["broker"]["address"]),
                    storage=pulsar_pb2.StorageInfo(storage_id=r["storage"]["storage_id"], address=r["storage"]["address"])
                )
                self._routes[(r["topic"], r["partition"])] = route
            self.save_state()
        except Exception as e:
            log_error(f"Failed to apply replicated state: {e}")

    def save_state(self):
        """Persists the cluster state, routes, and Raft metadata to a local JSON file"""
        routes_data = []
        for r in self._routes.values():
            routes_data.append({
                "topic": r.topic,
                "partition": r.partition,
                "broker": {"broker_id": r.broker.broker_id, "address": r.broker.address},
                "storage": {"storage_id": r.storage.storage_id, "address": r.storage.address}
            })
            
        data = {
            "term": self._term,
            "voted_for": self._voted_for,
            "brokers": self._brokers,
            "storages": self._storages,
            "topic_partitions": self._topic_partitions,
            "routes": routes_data
        }
        temp_file = self._state_file + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f)
        os.replace(temp_file, self._state_file)

    def _load_state(self):
        """Loads cluster and Raft state from the local file upon startup"""
        if os.path.exists(self._state_file):
            try:
                with open(self._state_file, "r") as f:
                    data = json.load(f)
                    self._term = data.get("term", 0)
                    self._voted_for = data.get("voted_for", None)
                    self._brokers = data.get("brokers", {})
                    self._storages = data.get("storages", {})
                    self._topic_partitions = data.get("topic_partitions", {})
                    
                    routes_data = data.get("routes", [])
                    for r in routes_data:
                        route = pulsar_pb2.PartitionRoute(
                            topic=r["topic"],
                            partition=r["partition"],
                            broker=pulsar_pb2.BrokerInfo(broker_id=r["broker"]["broker_id"], address=r["broker"]["address"]),
                            storage=pulsar_pb2.StorageInfo(storage_id=r["storage"]["storage_id"], address=r["storage"]["address"])
                        )
                        self._routes[(r["topic"], r["partition"])] = route
                log_event(f"Loaded state from {self._state_file}")
            except Exception as e:
                log_error(f"Failed to load state: {e}")

    def Register(self, request, context):
        """Registers a new broker or storage node in the cluster"""
        with self.lock:
            self.consensus.check_leader(context)
            if not request.node_id:
                return pulsar_pb2.RegisterResponse(ok=False, message="missing node_id", node_id="")

            node_id = request.node_id
            if request.node_type == pulsar_pb2.NODE_TYPE_BROKER:
                self._brokers[node_id] = request.address
                self._heartbeats[node_id] = {"address": request.address, "last_seen": time.time(), "missed": 0}
                log_success(f"Broker registered id={node_id} addr={request.address}")
            elif request.node_type == pulsar_pb2.NODE_TYPE_STORAGE:
                self._storages[node_id] = request.address
                log_success(f"Storage registered id={node_id} addr={request.address}")
            
            self.save_state()
            return pulsar_pb2.RegisterResponse(ok=True, message="registered", node_id=node_id)

    def Heartbeat(self, request, context):
        with self.lock:
            self.consensus.check_leader(context)
            if request.broker_id:
                self._brokers[request.broker_id] = request.address
                heartbeat = self._heartbeats.get(request.broker_id)
                if not heartbeat:
                    heartbeat = {"address": request.address, "last_seen": 0, "missed": 0}
                    self._heartbeats[request.broker_id] = heartbeat
                heartbeat["address"] = request.address
                heartbeat["last_seen"] = time.time()
                heartbeat["missed"] = 0
            self.save_state()
        return pulsar_pb2.HeartbeatResponse(ok=True, message="ok")

    def CreateTopic(self, request, context):
        topic = request.topic_name
        num_partitions = DEFAULT_PARTITIONS
        with self.lock:
            self.consensus.check_leader(context)
            if topic in self._topic_partitions:
                routes = self._routes_for_topic(topic)
                default_broker = routes[0].broker if routes else pulsar_pb2.BrokerInfo()
                return pulsar_pb2.CreateTopicResponse(default_broker=default_broker, routes=routes)

            if not self._brokers or not self._storages:
                log_error("create_topic missing brokers or storages")
                return pulsar_pb2.CreateTopicResponse(default_broker=pulsar_pb2.BrokerInfo(), routes=[])

            broker_list = sorted(self._brokers.items())
            storage_list = sorted(self._storages.items())
            self._topic_partitions[topic] = num_partitions

            new_routes = []
            for partition in range(num_partitions):
                broker_id, broker_addr = broker_list[partition % len(broker_list)]
                storage_id, storage_addr = storage_list[partition % len(storage_list)]
                route = pulsar_pb2.PartitionRoute(
                    topic=topic,
                    partition=partition,
                    broker=pulsar_pb2.BrokerInfo(broker_id=broker_id, address=broker_addr),
                    storage=pulsar_pb2.StorageInfo(storage_id=storage_id, address=storage_addr)
                )
                self._routes[(topic, partition)] = route
                new_routes.append(route)

            self.save_state()
            default_broker = new_routes[0].broker if new_routes else pulsar_pb2.BrokerInfo()
            all_routes = list(self._routes.values())

        self._notify_brokers(all_routes)
        log_success(f"Topic={topic} created partitions={num_partitions}")
        return pulsar_pb2.CreateTopicResponse(default_broker=default_broker, routes=new_routes)

    def SubscribeMessage(self, request, context):
        with self.lock:
            routes = self._routes_for_topic(request.topic_name)
        if not routes:
            return pulsar_pb2.SubscribeMessageResponse(status=pulsar_pb2.STATUS_ERROR, error_message="Topic not found", routes=[])
        return pulsar_pb2.SubscribeMessageResponse(status=pulsar_pb2.STATUS_OK, error_message="", routes=routes)

    def ListTopics(self, request, context):
        with self.lock:
            topics = sorted(self._topic_partitions.keys())
        return pulsar_pb2.ListTopicsResponse(status=pulsar_pb2.STATUS_OK, error_message="", topics=topics)

    def GetRoutingTable(self, request, context):
        with self.lock:
            routes = list(self._routes.values())
        return pulsar_pb2.GetRoutingTableResponse(routes=routes)

    def RequestVote(self, request, context):
        """Proxy Raft vote request to consensus module"""
        return self.consensus.RequestVote(request, context)

    def AppendEntries(self, request, context):
        """Proxy Raft replication request to consensus module"""
        return self.consensus.AppendEntries(request, context)

    def _routes_for_topic(self, topic):
        return [route for (route_topic, _), route in self._routes.items() if route_topic == topic]

    def _notify_brokers(self, routes):
        for broker_id, broker_addr in self._brokers.items():
            stub = self._get_broker_stub(broker_addr)
            try:
                stub.AssignPartition(pulsar_pb2.AssignPartitionRequest(topic="", partition=0, storage=pulsar_pb2.StorageInfo(), routes=routes))
            except grpc.RpcError:
                log_error(f"Notify failed for broker_id={broker_id}")

    def _get_broker_stub(self, broker_addr):
        if broker_addr not in self._broker_stubs:
            channel = grpc.insecure_channel(broker_addr)
            self._broker_stubs[broker_addr] = pulsar_pb2_grpc.BrokerServiceStub(channel)
        return self._broker_stubs[broker_addr]

    def _monitor_heartbeats(self, interval, max_missed):
        while self._running:
            time.sleep(interval)
            failed = []
            now = time.time()
            with self.lock:
                if not self.consensus.is_leader():
                    continue
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
                    self.save_state()
                    self.consensus.send_heartbeats_with_state()

    def _reassign_partitions(self, failed_brokers):
        if not self._brokers:
            return

        broker_list = sorted(self._brokers.items())
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
    peers = read_coordinators(args.coordinators_file)
    listen_addr = f"{args.host}:{args.port}"
    
    # State file named based on port to avoid collision when running locally
    state_dir = "state"
    os.makedirs(state_dir, exist_ok=True)
    state_file = os.path.join(state_dir, f"coordinator_{args.port}_state.json")
    
    coordinator = Coordinator(listen_addr, listen_addr, peers, state_file)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pulsar_pb2_grpc.add_CoordinatorServiceServicer_to_server(coordinator, server)

    server.add_insecure_port(listen_addr)
    server.start()
    log_success(f"Coordinator listening on {listen_addr}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log_event("Coordinator shutting down")
    finally:
        coordinator._running = False
        coordinator.consensus.stop()
        server.stop(grace=2)

def main():
    parser = argparse.ArgumentParser(description="Mini-Pulsar coordinator")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=4000, help="Bind port")
    parser.add_argument("--coordinators-file", required=True, help="File containing list of all coordinator addresses")
    args = parser.parse_args()

    serve(args)

if __name__ == "__main__":
    main()
