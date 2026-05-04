import threading
import time
import random
from concurrent import futures
import grpc

from protos import pulsar_pb2
from protos import pulsar_pb2_grpc
from utils import log_event, log_success

# Node states for the Raft-like consensus algorithm
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

class ConsensusNode:
    """
    Implements a simplified Raft-like leader election and state replication algorithm.
    Delegates actual state persistence and domain-specific logic to a state_delegate.
    """
    def __init__(self, node_id, listen_addr, peers, state_delegate):
        self.node_id = node_id
        self.listen_addr = listen_addr
        self.peers = peers
        self.state_delegate = state_delegate  
        
        # Initial state is always FOLLOWER on startup
        self.state = FOLLOWER
        self.leader_id = None
        
        self.election_timeout = 0
        self.last_heartbeat = time.time()
        self.reset_election_timeout()
        
        self.running = True
        
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.election_thread.start()
        
        self.heartbeat_thread = threading.Thread(target=self._leader_heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    @property
    def lock(self):
        return self.state_delegate.lock

    def reset_election_timeout(self):
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

    def check_leader(self, context):
        """Aborts the gRPC call with a UNAVAILABLE status if this node is not the leader.
        Clients can parse the error details to find the current leader."""
        if self.state != LEADER:
            msg = f"NOT_LEADER: {self.leader_id}" if self.leader_id else "NOT_LEADER: None"
            context.abort(grpc.StatusCode.UNAVAILABLE, msg)

    def is_leader(self):
        return self.state == LEADER

    def RequestVote(self, request, context):
        """Handles incoming vote requests from candidates."""
        with self.lock:
            term = self.state_delegate.get_term()
            
            # Step down if candidate has a newer term
            if request.term > term:
                self.state_delegate.set_term(request.term)
                self.state = FOLLOWER
                self.state_delegate.set_voted_for(None)
                self.leader_id = None
                self.state_delegate.save_state()
                term = request.term
            
            # Grant vote if term matches and we haven't voted for anyone else
            voted_for = self.state_delegate.get_voted_for()
            if request.term == term and (voted_for is None or voted_for == request.candidate_id):
                self.state_delegate.set_voted_for(request.candidate_id)
                self.state_delegate.save_state()
                self.reset_election_timeout()
                return pulsar_pb2.RequestVoteResponse(term=term, vote_granted=True)
            return pulsar_pb2.RequestVoteResponse(term=term, vote_granted=False)

    def AppendEntries(self, request, context):
        """Handles heartbeats and state replication from the current leader."""
        with self.lock:
            term = self.state_delegate.get_term()
            
            # Catch up to newer term
            if request.term > term:
                self.state_delegate.set_term(request.term)
                self.state_delegate.set_voted_for(None)
                self.state_delegate.save_state()
                term = request.term
            
            # Acknowledge leader and process state update
            if request.term >= term:
                self.state = FOLLOWER
                self.leader_id = request.leader_id
                self.reset_election_timeout()
                
                if request.state_json:
                    self.state_delegate.apply_state_json(request.state_json)
                return pulsar_pb2.AppendEntriesResponse(term=term, success=True)
            
            return pulsar_pb2.AppendEntriesResponse(term=term, success=False)

    def _election_loop(self):
        while self.running:
            time.sleep(0.1)
            with self.lock:
                if self.state == LEADER:
                    continue
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self.state = CANDIDATE
                    new_term = self.state_delegate.get_term() + 1
                    self.state_delegate.set_term(new_term)
                    self.state_delegate.set_voted_for(self.listen_addr)
                    self.state_delegate.save_state()
                    self.reset_election_timeout()
                    current_term = new_term
                    candidate_id = self.listen_addr
                    peers = list(self.peers)
                    log_event(f"Starting election for term {current_term}")
            
            if not peers:
                with self.lock:
                    if self.state == CANDIDATE and self.state_delegate.get_term() == current_term:
                        self.state = LEADER
                        self.leader_id = self.listen_addr
                        log_success(f"Became leader for term {current_term}")
                        self._send_heartbeats(include_state=True)
                continue

            votes = 1
            needed = (len(peers) + 1) // 2 + 1
            
            def request_vote(peer):
                try:
                    channel = grpc.insecure_channel(peer)
                    stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
                    return stub.RequestVote(pulsar_pb2.RequestVoteRequest(term=current_term, candidate_id=candidate_id), timeout=0.5)
                except:
                    return None
            
            with futures.ThreadPoolExecutor(max_workers=len(peers)) as executor:
                future_to_peer = {executor.submit(request_vote, peer): peer for peer in peers}
                for future in futures.as_completed(future_to_peer):
                    resp = future.result()
                    if resp:
                        with self.lock:
                            term = self.state_delegate.get_term()
                            if resp.term > term:
                                self.state_delegate.set_term(resp.term)
                                self.state = FOLLOWER
                                self.state_delegate.set_voted_for(None)
                                self.state_delegate.save_state()
                                break
                            if resp.vote_granted and self.state == CANDIDATE and term == current_term:
                                votes += 1
                                if votes >= needed:
                                    self.state = LEADER
                                    self.leader_id = self.listen_addr
                                    log_success(f"Became leader for term {current_term} with {votes} votes")
                                    self._send_heartbeats(include_state=True)
                                    break

    def _send_heartbeats(self, include_state=False):
        with self.lock:
            if self.state != LEADER:
                return
            term = self.state_delegate.get_term()
            leader_id = self.listen_addr
            peers = list(self.peers)
            state_json = self.state_delegate.get_state_json() if include_state else ""
                
        def send_hb(peer):
            try:
                channel = grpc.insecure_channel(peer)
                stub = pulsar_pb2_grpc.CoordinatorServiceStub(channel)
                return stub.AppendEntries(pulsar_pb2.AppendEntriesRequest(term=term, leader_id=leader_id, state_json=state_json), timeout=0.5)
            except:
                return None

        for peer in peers:
            resp = send_hb(peer)
            if resp:
                with self.lock:
                    if resp.term > self.state_delegate.get_term():
                        self.state_delegate.set_term(resp.term)
                        self.state = FOLLOWER
                        self.state_delegate.set_voted_for(None)
                        self.state_delegate.save_state()
                        break

    def _leader_heartbeat_loop(self):
        while self.running:
            time.sleep(0.5)
            with self.lock:
                if self.state == LEADER:
                    self._send_heartbeats()

    def send_heartbeats_with_state(self):
        self._send_heartbeats(include_state=True)

    def stop(self):
        self.running = False
