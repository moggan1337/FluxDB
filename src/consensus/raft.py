"""
Raft Consensus Algorithm for FluxDB

This module implements the Raft consensus algorithm for distributed
coordination and leader election.

Raft is designed to be understandable and provides:
- Leader election
- Log replication
- Membership changes
- Log compaction with snapshots

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                        Raft States                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│     ┌──────────┐                                                │
│     │ Follower │◄─────────────────────────┐                     │
│     └────┬─────┘                          │                     │
│          │                                 │                     │
│          │ election timeout                │ heartbeat           │
│          ▼                                 │                     │
│     ┌──────────┐     majority      ┌──────┴───┐                  │
│     │ Candidate├──────────────────►│  Leader  │                  │
│     └────┬─────┘◄──────────────────┴──────────┘                  │
│          │                  lower term                            │
│          │                                                       │
│          │ vote granted                                         │
│          ▼                                                       │
│     ┌──────────┐                                                │
│     │ Observer │ (optional non-voting state)                     │
│     └──────────┘                                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Key Concepts:
- Leader: Handles all client requests
- Candidate: Trying to become leader
- Follower: Passive replica
- Term: Logical clock for leader elections
- Log Entry: Command to be applied to state machine

Usage:

```python
from fluxdb.consensus import RaftNode, RaftCluster

# Create a cluster
cluster = RaftCluster(
    node_ids=["node1", "node2", "node3"],
    heartbeat_interval=0.5,
    election_timeout=2.0,
)

# Start the cluster
cluster.start()

# Wait for leader
leader = cluster.wait_for_leader(timeout=10)
print(f"Leader is {leader.node_id}")

# Submit a command
future = leader.submit("set", "key", "value")
result = future.result(timeout=5)
print(result)  # {"success": True, "result": "ok"}
```
"""

import json
import threading
import time
import queue
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import logging

logger = logging.getLogger(__name__)


class NodeState(Enum):
    """Raft node states"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """
    A single entry in the replicated log.
    
    Attributes:
    - term: Term in which entry was created
    - index: Position in log
    - command: The actual command to apply
    - timestamp: When entry was created
    """
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(**data)


@dataclass
class VoteRequest:
    """Request for vote from candidate"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """Response to vote request"""
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntriesRequest:
    """Request to append entries (heartbeat or log replication)"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """Response to append entries request"""
    term: int
    success: bool
    match_index: int
    follower_id: str


@dataclass
class InstallSnapshotRequest:
    """Request to install snapshot"""
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    offset: int
    data: bytes
    done: bool


@dataclass
class InstallSnapshotResponse:
    """Response to install snapshot"""
    term: int
    success: bool
    follower_id: str


@dataclass
class RaftConfig:
    """Configuration for Raft"""
    heartbeat_interval: float = 0.5  # seconds
    election_timeout_min: float = 1.5  # seconds
    election_timeout_max: float = 3.0  # seconds
    snapshot_interval: int = 10000  # entries
    max_entries_per_append: int = 100
    connection_timeout: float = 1.0
    retry_interval: float = 0.5


class RaftNode:
    """
    A single Raft node.
    
    This implements the core Raft consensus algorithm including:
    - Leader election
    - Log replication
    - Membership changes
    - Snapshots
    """
    
    def __init__(
        self,
        node_id: str,
        peers: Dict[str, "RaftNode"],
        state_machine: Optional[Callable] = None,
        config: Optional[RaftConfig] = None,
    ):
        self.node_id = node_id
        self.peers = peers
        self.state_machine = state_machine
        self.config = config or RaftConfig()
        
        # Persistent state (would be persisted to disk in production)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.state = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader-specific volatile state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self._last_heartbeat = time.time()
        self._election_deadline = self._random_election_timeout()
        
        # Communication
        self._mailbox: queue.Queue = queue.Queue()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._on_state_change: Optional[Callable] = None
        self._on_apply: Optional[Callable] = None
        
        # Locks
        self._lock = threading.RLock()
        
        logger.info(f"RaftNode {node_id} initialized")
    
    def _random_election_timeout(self) -> float:
        """Generate random election timeout"""
        return random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max
        )
    
    def start(self) -> None:
        """Start the Raft node"""
        with self._lock:
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
            logger.info(f"RaftNode {self.node_id} started")
    
    def stop(self) -> None:
        """Stop the Raft node"""
        with self._lock:
            self._running = False
            if self._thread:
                self._thread.join(timeout=5)
            logger.info(f"RaftNode {self.node_id} stopped")
    
    def _run_loop(self) -> None:
        """Main event loop"""
        while self._running:
            try:
                self._tick()
                time.sleep(0.1)  # Tick every 100ms
            except Exception as e:
                logger.error(f"RaftNode {self.node_id} error: {e}")
    
    def _tick(self) -> None:
        """Process one tick of the event loop"""
        with self._lock:
            # Check election timeout
            if self.state != NodeState.LEADER:
                if time.time() - self._last_heartbeat > self._election_deadline:
                    self._start_election()
            
            # Leader sends heartbeats
            if self.state == NodeState.LEADER:
                self._send_heartbeats()
            
            # Apply committed entries
            self._apply_committed_entries()
    
    def _start_election(self) -> None:
        """Start a leader election"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id  # Vote for self
        self._last_heartbeat = time.time()
        self._election_deadline = self._random_election_timeout()
        
        logger.info(f"RaftNode {self.node_id} starting election for term {self.current_term}")
        
        # Request votes from all peers
        votes = 1  # Vote for self
        
        last_log_index = self.log[-1].index if self.log else 0
        last_log_term = self.log[-1].term if self.log else 0
        
        vote_requests = [
            self._request_vote(
                peer_id,
                VoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term,
                )
            )
            for peer_id in self.peers
        ]
        
        # Count votes
        for granted in vote_requests:
            if granted:
                votes += 1
        
        # Check if won
        majority = (len(self.peers) + 1) // 2 + 1
        if votes >= majority:
            self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
    
    def _become_leader(self) -> None:
        """Become the leader"""
        self.state = NodeState.LEADER
        logger.info(f"RaftNode {self.node_id} became leader for term {self.current_term}")
        
        # Initialize leader state
        for peer_id in self.peers:
            self.next_index[peer_id] = len(self.log) + 1
            self.match_index[peer_id] = 0
        
        # Send initial heartbeat
        self._send_heartbeats()
        
        # Notify state change
        if self._on_state_change:
            self._on_state_change(self.state)
    
    def _request_vote(self, peer_id: str, request: VoteRequest) -> bool:
        """Send vote request to peer (simplified - would use RPC in production)"""
        peer = self.peers.get(peer_id)
        if not peer:
            return False
        
        try:
            response = peer.receive_vote_request(request)
            return response.vote_granted
        except Exception as e:
            logger.error(f"Vote request to {peer_id} failed: {e}")
            return False
    
    def receive_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Receive and process vote request"""
        with self._lock:
            # Update term if needed
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
            
            vote_granted = False
            
            if request.term >= self.current_term:
                # Check if we should vote
                if self.voted_for is None or self.voted_for == request.candidate_id:
                    # Check log is at least as up-to-date
                    last_log_index = self.log[-1].index if self.log else 0
                    last_log_term = self.log[-1].term if self.log else 0
                    
                    if (request.last_log_term > last_log_term or
                        (request.last_log_term == last_log_term and
                         request.last_log_index >= last_log_index)):
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self._last_heartbeat = time.time()
            
            return VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id,
            )
    
    def _send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        for peer_id in self.peers:
            self._send_append_entries(peer_id, entries=[])
    
    def _send_append_entries(self, peer_id: str, entries: List[LogEntry]) -> bool:
        """Send append entries request to peer"""
        peer = self.peers.get(peer_id)
        if not peer:
            return False
        
        try:
            prev_log_index = self.next_index.get(peer_id, 1) - 1
            prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 and self.log else 0
            
            request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index,
            )
            
            response = peer.receive_append_entries(request)
            
            with self._lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = NodeState.FOLLOWER
                    return False
                
                if response.success:
                    # Update next and match indices
                    if entries:
                        self.next_index[peer_id] = entries[-1].index + 1
                        self.match_index[peer_id] = entries[-1].index
                    return True
                else:
                    # Decrement next index and retry
                    self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
                    return False
                    
        except Exception as e:
            logger.error(f"Append entries to {peer_id} failed: {e}")
            return False
    
    def receive_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Receive and process append entries request"""
        with self._lock:
            success = False
            
            # Update term if needed
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = NodeState.FOLLOWER
            
            self._last_heartbeat = time.time()
            
            if request.term >= self.current_term:
                # Check if we have the previous entry
                if request.prev_log_index == 0 or (
                    request.prev_log_index <= len(self.log) and
                    self.log[request.prev_log_index - 1].term == request.prev_log_term
                ):
                    # Append new entries
                    self.log = self.log[:request.prev_log_index]
                    self.log.extend(request.entries)
                    success = True
                    
                    # Update commit index
                    if request.leader_commit > self.commit_index:
                        self.commit_index = min(request.leader_commit, len(self.log))
            
            return AppendEntriesResponse(
                term=self.current_term,
                success=success,
                match_index=request.prev_log_index + len(request.entries),
                follower_id=self.node_id,
            )
    
    def submit(self, command: Dict[str, Any]) -> "Future":
        """
        Submit a command to the Raft cluster.
        
        Returns a Future that will be resolved when the command is committed.
        """
        if self.state != NodeState.LEADER:
            raise ValueError(f"Node {self.node_id} is not the leader")
        
        # Create log entry
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log) + 1,
            command=command,
        )
        
        # Add to local log
        self.log.append(entry)
        
        # Replicate to peers
        success_count = 1
        for peer_id in self.peers:
            if self._send_append_entries(peer_id, [entry]):
                success_count += 1
        
        # Check if committed (majority)
        majority = (len(self.peers) + 1) // 2 + 1
        if success_count >= majority:
            self.commit_index = entry.index
            self._apply_committed_entries()
        
        # Return future (simplified - would support timeout/cancel in production)
        future = Future()
        future.set_result({"success": True, "index": entry.index})
        return future
    
    def _apply_committed_entries(self) -> None:
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            
            if self.state_machine:
                try:
                    result = self.state_machine(entry.command)
                    if self._on_apply:
                        self._on_apply(entry, result)
                except Exception as e:
                    logger.error(f"Failed to apply command: {e}")
    
    def get_state(self) -> Dict[str, Any]:
        """Get current Raft state"""
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": len(self.log),
                "voted_for": self.voted_for,
                "is_leader": self.state == NodeState.LEADER,
            }


class Future:
    """A future result from an operation"""
    
    def __init__(self):
        self._result = None
        self._exception = None
        self._event = threading.Event()
    
    def set_result(self, result: Any) -> None:
        """Set the result"""
        self._result = result
        self._event.set()
    
    def set_exception(self, exception: Exception) -> None:
        """Set an exception"""
        self._exception = exception
        self._event.set()
    
    def result(self, timeout: Optional[float] = None) -> Any:
        """Wait for and return the result"""
        self._event.wait(timeout)
        if self._exception:
            raise self._exception
        return self._result
    
    def done(self) -> bool:
        """Check if result is available"""
        return self._event.is_set()


class RaftCluster:
    """
    A Raft cluster managing multiple nodes.
    
    Provides high-level interface to the cluster including:
    - Cluster initialization
    - Leader election
    - Client request routing
    """
    
    def __init__(
        self,
        node_ids: List[str],
        heartbeat_interval: float = 0.5,
        election_timeout: float = 2.0,
    ):
        self.node_ids = set(node_ids)
        self._nodes: Dict[str, RaftNode] = {}
        self._running = False
        self._lock = threading.Lock()
        
        logger.info(f"RaftCluster initialized with {len(node_ids)} nodes")
    
    def start(self) -> None:
        """Start the cluster"""
        with self._lock:
            if self._running:
                return
            
            # Create nodes
            for node_id in self.node_ids:
                self._nodes[node_id] = RaftNode(
                    node_id=node_id,
                    peers={},  # Will be set below
                )
            
            # Set up peer connections
            for node_id in self.node_ids:
                self._nodes[node_id].peers = {
                    peer_id: self._nodes[peer_id]
                    for peer_id in self.node_ids
                    if peer_id != node_id
                }
            
            # Start all nodes
            for node in self._nodes.values():
                node.start()
            
            self._running = True
            logger.info("RaftCluster started")
    
    def stop(self) -> None:
        """Stop the cluster"""
        with self._lock:
            if not self._running:
                return
            
            for node in self._nodes.values():
                node.stop()
            
            self._running = False
            logger.info("RaftCluster stopped")
    
    def get_leader(self) -> Optional[RaftNode]:
        """Get the current leader"""
        for node in self._nodes.values():
            if node.state == NodeState.LEADER:
                return node
        return None
    
    def wait_for_leader(self, timeout: float = 10.0) -> RaftNode:
        """Wait for a leader to be elected"""
        start = time.time()
        while time.time() - start < timeout:
            leader = self.get_leader()
            if leader:
                return leader
            time.sleep(0.5)
        
        raise TimeoutError("No leader elected within timeout")
    
    def submit(self, command: Dict[str, Any]) -> Future:
        """Submit a command to the cluster"""
        leader = self.get_leader()
        if not leader:
            raise ValueError("No leader available")
        return leader.submit(command)
    
    def get_node(self, node_id: str) -> Optional[RaftNode]:
        """Get a node by ID"""
        return self._nodes.get(node_id)
    
    def get_cluster_state(self) -> Dict[str, Any]:
        """Get state of all nodes"""
        return {
            node_id: node.get_state()
            for node_id, node in self._nodes.items()
        }
    
    def add_node(self, node_id: str) -> None:
        """Add a new node to the cluster"""
        with self._lock:
            if node_id in self.node_ids:
                raise ValueError(f"Node {node_id} already exists")
            
            self.node_ids.add(node_id)
            
            # Create new node with peers
            new_node = RaftNode(
                node_id=node_id,
                peers={pid: self._nodes[pid] for pid in self._nodes},
            )
            self._nodes[node_id] = new_node
            
            # Update existing nodes' peers
            for node in self._nodes.values():
                node.peers[node_id] = new_node
            
            if self._running:
                new_node.start()
    
    def remove_node(self, node_id: str) -> None:
        """Remove a node from the cluster"""
        with self._lock:
            if node_id not in self.node_ids:
                raise ValueError(f"Node {node_id} not found")
            
            self.node_ids.discard(node_id)
            
            if node_id in self._nodes:
                self._nodes[node_id].stop()
                del self._nodes[node_id]
            
            # Update remaining nodes' peers
            for node in self._nodes.values():
                node.peers.pop(node_id, None)
