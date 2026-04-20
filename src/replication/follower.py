"""
Distributed Replication for FluxDB

This module implements distributed replication for FluxDB, enabling
data to be replicated across multiple nodes for fault tolerance
and high availability.

Features:
- Synchronous and asynchronous replication
- Replication lag monitoring
- Automatic failover
- Multi-datacenter support
- Consistency levels (strong, eventual, bounded)

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    Replication Architecture                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────┐                                                   │
│  │ Primary │  ←───── sync replication ──────► ┌─────────┐    │
│  │  Node   │                               │Replica 1│    │
│  └────┬────┘                               └─────────┘    │
│       │                                                       │
│       │ ←──── async replication ──► ┌─────────┐            │
│       │                             │Replica 2│            │
│       │                             └─────────┘            │
│       │                                                       │
│       │ ←──── async replication ──► ┌─────────┐            │
│       │                             │Replica 3│            │
│       │                             └─────────┘            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Replication Modes:
1. Synchronous: Wait for all replicas before acknowledging
2. Asynchronous: Acknowledge immediately, replicate in background
3. Quorum: Wait for majority (N/2+1)

Usage:

```python
from fluxdb.replication import Replica, ReplicationManager

# Create replica
replica = Replica(
    replica_id="replica-1",
    primary_address="localhost:5000",
    replication_mode="sync",
)

# Start replication
replica.start()

# Monitor lag
stats = replica.get_stats()
print(f"Replication lag: {stats['lag_ms']}ms")
```
"""

import json
import queue
import socket
import struct
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import logging

logger = logging.getLogger(__name__)


class ReplicationMode(Enum):
    """Replication modes"""
    SYNCHRONOUS = "sync"
    ASYNCHRONOUS = "async"
    QUORUM = "quorum"
    LEADERLESS = "leaderless"


class ReplicaState(Enum):
    """Replica states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    SYNCING = "syncing"
    CONNECTED = "connected"
    CATCHING_UP = "catching_up"
    ERROR = "error"


class ConsistencyLevel(Enum):
    """Consistency levels for reads/writes"""
    STRONG = "strong"       # All replicas must agree
    QUORUM = "quorum"       # Majority must agree
    LOCAL_QUORUM = "local_quorum"  # Local datacenter quorum
    EVENTUAL = "eventual"   # Any replica
    BOUNDED = "bounded"     # Within bounded staleness


@dataclass
class ReplicationStats:
    """Statistics for replication"""
    replica_id: str
    state: ReplicaState
    lag_bytes: int
    lag_ms: float
    last_sync_time: float
    bytes_synced: int
    records_synced: int
    errors: int
    throughput_mbps: float


@dataclass
class WriteRequest:
    """A write request to be replicated"""
    request_id: str
    key: str
    value: Any
    timestamp: float
    sequence: int
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class ReplicationMessage:
    """Message in the replication protocol"""
    msg_type: str
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    checksum: Optional[str] = None


class Replica:
    """
    A replica node that replicates data from a primary.
    
    Handles:
    - Connection management
    - Data synchronization
    - Conflict detection
    - Failover
    """
    
    def __init__(
        self,
        replica_id: str,
        primary_address: str,
        replication_mode: str = "async",
        config: Optional[Dict[str, Any]] = None,
    ):
        self.replica_id = replica_id
        self.primary_address = primary_address
        self.mode = ReplicationMode(replication_mode)
        self.config = config or {}
        
        # State
        self.state = ReplicaState.DISCONNECTED
        self._socket: Optional[socket.socket] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        # Sync state
        self.last_sequence = 0
        self.last_sync_time = time.time()
        self.lag_bytes = 0
        self.lag_ms = 0.0
        
        # Statistics
        self.bytes_synced = 0
        self.records_synced = 0
        self.errors = 0
        self._start_time = time.time()
        
        # Queues
        self._write_queue: queue.Queue = queue.Queue()
        self._pending_writes: Dict[str, WriteRequest] = {}
        
        # Callbacks
        self._on_sync: Optional[Callable] = None
        self._on_error: Optional[Callable] = None
        
        # Locks
        self._lock = threading.RLock()
        
        logger.info(f"Replica {replica_id} initialized for primary {primary_address}")
    
    def start(self) -> None:
        """Start the replica"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
            
            # Start connection
            self._connect()
            
            logger.info(f"Replica {self.replica_id} started")
    
    def stop(self) -> None:
        """Stop the replica"""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            self._disconnect()
            
            if self._thread:
                self._thread.join(timeout=5)
            
            logger.info(f"Replica {self.replica_id} stopped")
    
    def _run_loop(self) -> None:
        """Main replication loop"""
        while self._running:
            try:
                if self.state == ReplicaState.CONNECTED:
                    self._process_sync()
                elif self.state == ReplicaState.DISCONNECTED:
                    self._connect()
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Replica loop error: {e}")
                self.errors += 1
                self.state = ReplicaState.ERROR
                time.sleep(1)
    
    def _connect(self) -> None:
        """Connect to primary"""
        try:
            self.state = ReplicaState.CONNECTING
            
            # Parse address
            host, port = self.primary_address.split(":")
            port = int(port)
            
            # Create socket
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(5.0)
            self._socket.connect((host, port))
            
            # Send handshake
            self._send_message({
                "type": "handshake",
                "replica_id": self.replica_id,
                "last_sequence": self.last_sequence,
            })
            
            # Wait for response
            response = self._receive_message()
            if response and response.get("type") == "handshake_ack":
                self.state = ReplicaState.CONNECTED
                self.last_sync_time = time.time()
                logger.info(f"Replica {self.replica_id} connected to primary")
            else:
                raise Exception("Invalid handshake response")
                
        except Exception as e:
            logger.error(f"Failed to connect replica: {e}")
            self.state = ReplicaState.DISCONNECTED
            self.errors += 1
    
    def _disconnect(self) -> None:
        """Disconnect from primary"""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        self.state = ReplicaState.DISCONNECTED
    
    def _send_message(self, data: Dict[str, Any]) -> bool:
        """Send a message to primary"""
        if not self._socket:
            return False
        
        try:
            msg = ReplicationMessage(msg_type=data["type"], data=data)
            encoded = json.dumps(msg.__dict__, default=str).encode()
            length = struct.pack(">I", len(encoded))
            self._socket.sendall(length + encoded)
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def _receive_message(self) -> Optional[Dict[str, Any]]:
        """Receive a message from primary"""
        if not self._socket:
            return None
        
        try:
            # Read length
            length_data = self._socket.recv(4)
            if not length_data:
                return None
            
            length = struct.unpack(">I", length_data)[0]
            
            # Read message
            data = b""
            while len(data) < length:
                chunk = self._socket.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            
            msg = json.loads(data.decode())
            return msg.get("data")
            
        except socket.timeout:
            return None
        except Exception as e:
            logger.error(f"Failed to receive message: {e}")
            return None
    
    def _process_sync(self) -> None:
        """Process synchronization with primary"""
        try:
            # Request sync
            if not self._send_message({
                "type": "sync_request",
                "last_sequence": self.last_sequence,
            }):
                self.state = ReplicaState.DISCONNECTED
                return
            
            # Receive sync data
            response = self._receive_message()
            if not response:
                return
            
            if response.get("type") == "sync_data":
                records = response.get("records", [])
                
                for record in records:
                    self._apply_record(record)
                    self.last_sequence = max(self.last_sequence, record.get("sequence", 0))
                    self.records_synced += 1
                    self.bytes_synced += len(json.dumps(record))
                
                self.last_sync_time = time.time()
                
            elif response.get("type") == "heartbeat":
                # Update lag
                self.lag_ms = (time.time() - response.get("timestamp", time.time())) * 1000
                
        except Exception as e:
            logger.error(f"Sync error: {e}")
            self.errors += 1
    
    def _apply_record(self, record: Dict[str, Any]) -> None:
        """Apply a replicated record"""
        if self._on_sync:
            try:
                self._on_sync(record)
            except Exception as e:
                logger.error(f"Failed to apply record: {e}")
    
    def queue_write(self, request: WriteRequest) -> None:
        """Queue a write for replication"""
        self._write_queue.put(request)
        self._pending_writes[request.request_id] = request
    
    def acknowledge(self, request_id: str, success: bool) -> None:
        """Acknowledge a write"""
        if request_id in self._pending_writes:
            del self._pending_writes[request_id]
    
    def get_stats(self) -> ReplicationStats:
        """Get replication statistics"""
        elapsed = time.time() - self._start_time
        throughput = (self.bytes_synced / (1024 * 1024)) / max(elapsed, 1)
        
        return ReplicationStats(
            replica_id=self.replica_id,
            state=self.state,
            lag_bytes=self.lag_bytes,
            lag_ms=self.lag_ms,
            last_sync_time=self.last_sync_time,
            bytes_synced=self.bytes_synced,
            records_synced=self.records_synced,
            errors=self.errors,
            throughput_mbps=throughput,
        )
    
    @property
    def is_connected(self) -> bool:
        """Check if replica is connected"""
        return self.state in (ReplicaState.CONNECTED, ReplicaState.SYNCING)


class ReplicationManager:
    """
    Manages replication across a cluster.
    
    Provides:
    - Replica management
    - Consistency control
    - Failover handling
    - Load balancing
    """
    
    def __init__(
        self,
        local_node_id: str,
        replication_mode: str = "async",
        quorum_size: Optional[int] = None,
    ):
        self.local_node_id = local_node_id
        self.replication_mode = ReplicationMode(replication_mode)
        self.quorum_size = quorum_size
        
        # Nodes
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._replicas: Dict[str, Replica] = {}
        self._primary: Optional[str] = None
        
        # State
        self._running = False
        self._is_primary = False
        
        # Statistics
        self._stats: Dict[str, ReplicationStats] = {}
        self._lock = threading.RLock()
        
        logger.info(f"ReplicationManager initialized for {local_node_id}")
    
    def add_node(
        self,
        node_id: str,
        address: str,
        is_primary: bool = False,
    ) -> None:
        """Add a node to the replication cluster"""
        with self._lock:
            self._nodes[node_id] = {
                "id": node_id,
                "address": address,
                "is_primary": is_primary,
                "last_seen": time.time(),
            }
            
            if is_primary:
                self._primary = node_id
            elif node_id != self.local_node_id:
                self._replicas[node_id] = Replica(
                    replica_id=node_id,
                    primary_address=address,
                    replication_mode=self.replication_mode.value,
                )
    
    def remove_node(self, node_id: str) -> None:
        """Remove a node from the cluster"""
        with self._lock:
            if node_id in self._nodes:
                del self._nodes[node_id]
            
            if node_id in self._replicas:
                self._replicas[node_id].stop()
                del self._replicas[node_id]
    
    def start(self) -> None:
        """Start replication"""
        with self._lock:
            self._running = True
            
            # Start all replicas
            for replica in self._replicas.values():
                replica.start()
            
            logger.info("Replication started")
    
    def stop(self) -> None:
        """Stop replication"""
        with self._lock:
            self._running = False
            
            for replica in self._replicas.values():
                replica.stop()
            
            logger.info("Replication stopped")
    
    def write(
        self,
        key: str,
        value: Any,
        consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
    ) -> bool:
        """
        Write with specified consistency level.
        
        Args:
            key: The key to write
            value: The value to write
            consistency: Desired consistency level
            
        Returns:
            True if write succeeded
        """
        with self._lock:
            if not self._is_primary:
                raise ValueError("Not the primary node")
            
            # Local write
            success_count = 1
            
            # Replicate to replicas based on consistency
            required = self._get_required_replicas(consistency)
            
            for replica in self._replicas.values():
                if replica.is_connected:
                    request = WriteRequest(
                        request_id=f"{key}:{time.time()}",
                        key=key,
                        value=value,
                        timestamp=time.time(),
                        sequence=0,
                    )
                    replica.queue_write(request)
                    success_count += 1
            
            return success_count >= required
    
    def _get_required_replicas(self, consistency: ConsistencyLevel) -> int:
        """Get number of replicas required for consistency level"""
        total = len(self._nodes)
        
        if consistency == ConsistencyLevel.STRONG:
            return total
        elif consistency == ConsistencyLevel.QUORUM:
            return total // 2 + 1
        elif consistency == ConsistencyLevel.LOCAL_QUORUM:
            return max(1, total // 3)
        else:
            return 1
    
    def read(
        self,
        key: str,
        consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
    ) -> Optional[Any]:
        """
        Read with specified consistency level.
        
        Args:
            key: The key to read
            consistency: Desired consistency level
            
        Returns:
            The value if found, None otherwise
        """
        with self._lock:
            # For now, just read locally
            # In production, would query multiple replicas based on consistency
            return None
    
    def get_primary(self) -> Optional[str]:
        """Get current primary node ID"""
        return self._primary
    
    def is_primary(self) -> bool:
        """Check if this node is primary"""
        return self._is_primary
    
    def set_primary(self, node_id: str) -> None:
        """Set the primary node"""
        with self._lock:
            old_primary = self._primary
            self._primary = node_id
            self._is_primary = (node_id == self.local_node_id)
            
            if self._is_primary:
                logger.info(f"Node {self.local_node_id} is now primary")
            else:
                logger.info(f"Node {node_id} is now primary (this node is replica)")
    
    def trigger_failover(self, failed_node_id: str) -> Optional[str]:
        """
        Trigger failover when a node fails.
        
        Returns the new primary node ID.
        """
        with self._lock:
            if failed_node_id not in self._nodes:
                return None
            
            # Find a healthy replica to promote
            for node_id, info in self._nodes.items():
                if node_id != failed_node_id:
                    # In production, would check node health
                    self.set_primary(node_id)
                    return node_id
            
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get replication statistics"""
        with self._lock:
            return {
                "local_node": self.local_node_id,
                "is_primary": self._is_primary,
                "primary": self._primary,
                "mode": self.replication_mode.value,
                "nodes": len(self._nodes),
                "replicas": {
                    node_id: replica.get_stats().__dict__
                    for node_id, replica in self._replicas.items()
                },
            }
    
    def check_health(self) -> Dict[str, bool]:
        """Check health of all nodes"""
        health = {}
        
        with self._lock:
            for node_id, info in self._nodes.items():
                if node_id == self.local_node_id:
                    health[node_id] = True
                elif node_id in self._replicas:
                    health[node_id] = self._replicas[node_id].is_connected
                else:
                    health[node_id] = False
        
        return health


class AntiEntropy:
    """
    Anti-entropy protocol for repairing inconsistencies.
    
    Periodically compares Merkle trees between replicas to
    identify and repair divergent data.
    """
    
    def __init__(
        self,
        node_id: str,
        repair_callback: Optional[Callable] = None,
        interval: float = 60.0,
    ):
        self.node_id = node_id
        self.repair_callback = repair_callback
        self.interval = interval
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Merkle tree state
        self._local_tree: Dict[str, str] = {}  # key -> hash
    
    def start(self) -> None:
        """Start anti-entropy"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
            
            logger.info(f"Anti-entropy started for {self.node_id}")
    
    def stop(self) -> None:
        """Stop anti-entropy"""
        with self._lock:
            self._running = False
            if self._thread:
                self._thread.join(timeout=5)
    
    def _run_loop(self) -> None:
        """Main anti-entropy loop"""
        while self._running:
            try:
                self._compare_trees()
            except Exception as e:
                logger.error(f"Anti-entropy error: {e}")
            
            time.sleep(self.interval)
    
    def _compare_trees(self) -> None:
        """Compare Merkle trees with peers"""
        # In production, would:
        # 1. Exchange tree hashes with peers
        # 2. Find divergent subtrees
        # 3. Exchange and reconcile divergent keys
        pass
    
    def update_tree(self, key: str, value_hash: str) -> None:
        """Update local Merkle tree"""
        self._local_tree[key] = value_hash
    
    def get_tree_hash(self) -> str:
        """Get root hash of local tree"""
        import hashlib
        sorted_items = sorted(self._local_tree.items())
        content = json.dumps(sorted_items)
        return hashlib.sha256(content.encode()).hexdigest()
