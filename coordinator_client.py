from utils import read_coordinators


class CoordinatorClient:
    """
    A client wrapper for communicating with a cluster of coordinators
    Automatically handles retries and redirects when a non-leader node is contacted
    """
    def __init__(self, coordinators_file):
        self.coordinators = read_coordinators(coordinators_file)
        self.current_index = 0

    def _get_stub(self):
        import grpc
        from protos import pulsar_pb2_grpc
        if not self.coordinators:
            return None
        channel = grpc.insecure_channel(self.coordinators[self.current_index])
        return pulsar_pb2_grpc.CoordinatorServiceStub(channel)

    def _next_coordinator(self, leader_hint=None):
        """Switches the active coordinator. Optionally uses a leader_hint if provided by a redirect"""
        if leader_hint and leader_hint in self.coordinators:
            self.current_index = self.coordinators.index(leader_hint)
        else:
            self.current_index = (self.current_index + 1) % max(1, len(self.coordinators))

    def call(self, method_name, request):
        """
        Invokes a gRPC method on the active coordinator
        If a NOT_LEADER error is caught, it retries against the hinted leader or the next node
        """
        import grpc
        import time
        attempts = 0
        while attempts < max(1, len(self.coordinators)) * 2:
            stub = self._get_stub()
            if not stub:
                return None
            try:
                method = getattr(stub, method_name)
                return method(request)
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.UNAVAILABLE and "NOT_LEADER:" in exc.details():
                    hint = exc.details().split("NOT_LEADER:")[1].strip()
                    if hint and hint != "None":
                        self._next_coordinator(hint)
                    else:
                        self._next_coordinator()
                else:
                    self._next_coordinator()
            attempts += 1
            time.sleep(0.5)
        return None
