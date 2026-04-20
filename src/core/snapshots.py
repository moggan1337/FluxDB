"""
Snapshots and Compaction for FluxDB

This module implements snapshot creation and compaction strategies for
maintaining efficient storage while preserving the immutable event log.

Key Features:
- Point-in-time snapshots for fast recovery
- Incremental snapshots for large datasets
- Compaction strategies (deadline, size, hybrid)
- Snapshot verification and integrity checks
- Automatic snapshot scheduling

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    Snapshot Manager                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Snapshot 1  │  │ Snapshot 2  │  │ Snapshot N  │             │
│  │ (full)      │  │ (incremental│  │ (incremental│             │
│  │ seq: 1000   │  │ base: 1)    │  │ base: 2000)  │             │
│  │ size: 50MB  │  │ seq: 2000   │  │ seq: 5000   │             │
│  └─────────────┘  │ size: 20MB  │  │ size: 25MB  │             │
│                   └─────────────┘  └─────────────┘             │
├─────────────────────────────────────────────────────────────────┤
│  Compaction Policies                                             │
│  - Time-based: compact after N hours                             │
│  - Size-based: compact when segment > N GB                        │
│  - Count-based: compact after N records                          │
│  - Custom: user-defined predicate                                 │
└─────────────────────────────────────────────────────────────────┘
"""

import hashlib
import json
import os
import time
import threading
import struct
import gzip
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union, Tuple
from enum import Enum
from collections import defaultdict
import logging

from .storage import AppendOnlyStore, Record, RecordType

logger = logging.getLogger(__name__)


class SnapshotType(Enum):
    """Types of snapshots"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DELTA = "delta"
    CHECKPOINT = "checkpoint"


class CompactionStrategy(Enum):
    """Compaction strategies"""
    TIME = "time"
    SIZE = "size"
    COUNT = "count"
    HYBRID = "hybrid"
    CUSTOM = "custom"


@dataclass
class SnapshotMetadata:
    """Metadata for a snapshot"""
    snapshot_id: str
    snapshot_type: SnapshotType
    sequence: int
    timestamp: float
    size_bytes: int
    record_count: int
    key_count: int
    checksum: str
    path: str
    base_snapshot_id: Optional[str] = None
    created_by: str = "system"
    expires_at: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_type": self.snapshot_type.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "size_bytes": self.size_bytes,
            "record_count": self.record_count,
            "key_count": self.key_count,
            "checksum": self.checksum,
            "path": self.path,
            "base_snapshot_id": self.base_snapshot_id,
            "created_by": self.created_by,
            "expires_at": self.expires_at,
            "tags": self.tags,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SnapshotMetadata":
        return cls(
            snapshot_id=data["snapshot_id"],
            snapshot_type=SnapshotType(data["snapshot_type"]),
            sequence=data["sequence"],
            timestamp=data["timestamp"],
            size_bytes=data["size_bytes"],
            record_count=data["record_count"],
            key_count=data["key_count"],
            checksum=data["checksum"],
            path=data["path"],
            base_snapshot_id=data.get("base_snapshot_id"),
            created_by=data.get("created_by", "system"),
            expires_at=data.get("expires_at"),
            tags=data.get("tags", {}),
        )


@dataclass 
class CompactionConfig:
    """Configuration for compaction policies"""
    strategy: CompactionStrategy = CompactionStrategy.HYBRID
    
    # Time-based settings (in seconds)
    min_age_seconds: int = 3600
    max_age_seconds: int = 86400 * 7
    
    # Size-based settings (in bytes)
    min_segment_size: int = 128 * 1024 * 1024
    max_segment_size: int = 1024 * 1024 * 1024
    
    # Count-based settings
    min_records: int = 10000
    max_records: int = 1000000
    
    # Hybrid settings
    size_weight: float = 0.4
    time_weight: float = 0.3
    count_weight: float = 0.3
    
    # Custom predicate
    custom_predicate: Optional[Callable[[Dict], bool]] = None
    
    # Safety
    preserve_recent: int = 3
    dry_run: bool = False


class Snapshot:
    """
    Represents a snapshot of the database state.
    
    Snapshots capture the complete state at a specific sequence number,
    enabling fast recovery and efficient compaction.
    """
    
    MAGIC = b"FLUXSNAP"
    VERSION = 1
    
    def __init__(self, metadata: SnapshotMetadata, data: Dict[str, Any]):
        self.metadata = metadata
        self.data = data
    
    def to_bytes(self, compress: bool = True) -> bytes:
        """Serialize snapshot to bytes"""
        header = struct.pack(
            ">8sBHIII",
            self.MAGIC,
            self.VERSION,
            1 if compress else 0,
            self.metadata.sequence,
            self.metadata.record_count,
            self.metadata.key_count,
        )
        
        json_data = json.dumps(self.data, default=str).encode("utf-8")
        
        if compress:
            json_data = gzip.compress(json_data)
        
        payload_length = len(json_data)
        checksum = hashlib.sha256(header + json_data).hexdigest()
        
        return header + struct.pack(">I", payload_length) + json_data + checksum.encode()
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "Snapshot":
        """Deserialize snapshot from bytes"""
        magic, version, compressed, sequence, record_count, key_count = struct.unpack(
            ">8sBHIII", data[:20]
        )
        
        if magic != cls.MAGIC:
            raise ValueError("Invalid snapshot magic")
        
        payload_length = struct.unpack(">I", data[20:24])[0]
        json_data = data[24:24 + payload_length]
        stored_checksum = data[24 + payload_length:24 + payload_length + 64].decode()
        
        # Verify checksum
        header = data[:20]
        computed_checksum = hashlib.sha256(header + json_data).hexdigest()
        if computed_checksum != stored_checksum:
            raise ValueError("Snapshot checksum mismatch")
        
        if compressed:
            json_data = gzip.decompress(json_data)
        
        snapshot_data = json.loads(json_data.decode("utf-8"))
        
        metadata = SnapshotMetadata(
            snapshot_id="",
            snapshot_type=SnapshotType.FULL,
            sequence=sequence,
            timestamp=time.time(),
            size_bytes=len(data),
            record_count=record_count,
            key_count=key_count,
            checksum=stored_checksum,
            path="",
        )
        
        return cls(metadata, snapshot_data)


class SnapshotManager:
    """
    Manages snapshot creation, restoration, and lifecycle.
    
    The SnapshotManager provides:
    - Automatic and manual snapshot creation
    - Incremental snapshot chains
    - Snapshot verification
    - Automatic cleanup of old snapshots
    - Compaction orchestration
    """
    
    def __init__(
        self,
        store: AppendOnlyStore,
        path: Union[str, Path],
        config: Optional[CompactionConfig] = None,
    ):
        """
        Initialize the snapshot manager.
        
        Args:
            store: The AppendOnlyStore to snapshot
            path: Directory to store snapshots
            config: Compaction configuration
        """
        self.store = store
        self.snapshot_path = Path(path)
        self.config = config or CompactionConfig()
        
        if self.create_if_missing:
            self.snapshot_path.mkdir(parents=True, exist_ok=True)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Snapshot registry
        self._snapshots: Dict[str, SnapshotMetadata] = {}
        self._latest_by_key: Dict[str, SnapshotMetadata] = {}
        
        # Chains for incremental snapshots
        self._snapshot_chains: Dict[str, List[str]] = defaultdict(list)
        
        # Load existing snapshots
        self._load_snapshot_index()
        
        logger.info(f"SnapshotManager initialized at {self.snapshot_path}")
    
    @property
    def create_if_missing(self) -> bool:
        return True
    
    def _load_snapshot_index(self) -> None:
        """Load snapshot index from disk"""
        index_path = self.snapshot_path / "index.json"
        if index_path.exists():
            with open(index_path, "r") as f:
                data = json.load(f)
                for snapshot_id, snapshot_data in data.items():
                    metadata = SnapshotMetadata.from_dict(snapshot_data)
                    self._snapshots[snapshot_id] = metadata
                    
                    if metadata.path and Path(metadata.path).exists():
                        if metadata.key not in self._latest_by_key or \
                           metadata.sequence > self._latest_by_key[metadata.key].sequence:
                            self._latest_by_key[metadata.key] = metadata
    
    def _save_snapshot_index(self) -> None:
        """Save snapshot index to disk"""
        index_path = self.snapshot_path / "index.json"
        data = {
            snapshot_id: metadata.to_dict()
            for snapshot_id, metadata in self._snapshots.items()
        }
        with open(index_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def create_snapshot(
        self,
        snapshot_id: Optional[str] = None,
        snapshot_type: SnapshotType = SnapshotType.FULL,
        sequence: Optional[int] = None,
        tags: Optional[Dict[str, str]] = None,
        created_by: str = "system",
    ) -> SnapshotMetadata:
        """
        Create a new snapshot of the current state.
        
        Args:
            snapshot_id: Optional custom snapshot ID
            snapshot_type: Type of snapshot (FULL or INCREMENTAL)
            sequence: Optional specific sequence to snapshot (default: current)
            tags: Optional tags for the snapshot
            created_by: Who/what created this snapshot
            
        Returns:
            SnapshotMetadata for the created snapshot
        """
        with self._lock:
            snapshot_id = snapshot_id or f"snap_{int(time.time() * 1000)}"
            sequence = sequence or self.store._sequence
            
            # Determine if this should be incremental
            should_incremental = (
                snapshot_type == SnapshotType.INCREMENTAL or
                (snapshot_type == SnapshotType.FULL and self._snapshots)
            )
            
            # Find base snapshot for incremental
            base_snapshot = None
            if should_incremental and self._snapshots:
                # Find the latest snapshot with lower sequence
                candidates = [
                    (sid, meta) for sid, meta in self._snapshots.items()
                    if meta.sequence < sequence and meta.snapshot_type == SnapshotType.FULL
                ]
                if candidates:
                    base_snapshot = max(candidates, key=lambda x: x[1].sequence)[1]
            
            # Capture state
            snapshot_data = self._capture_state(sequence, base_snapshot)
            
            # Create snapshot file
            snapshot_path = self.snapshot_path / f"{snapshot_id}.snap"
            
            # Create metadata
            metadata = SnapshotMetadata(
                snapshot_id=snapshot_id,
                snapshot_type=SnapshotType.INCREMENTAL if base_snapshot else SnapshotType.FULL,
                sequence=sequence,
                timestamp=time.time(),
                size_bytes=0,
                record_count=snapshot_data.get("_record_count", 0),
                key_count=snapshot_data.get("_key_count", 0),
                checksum="",
                path=str(snapshot_path),
                base_snapshot_id=base_snapshot.snapshot_id if base_snapshot else None,
                created_by=created_by,
                tags=tags or {},
            )
            
            # Create Snapshot object and serialize
            snapshot = Snapshot(metadata, snapshot_data)
            snapshot_bytes = snapshot.to_bytes()
            
            # Write to disk
            with open(snapshot_path, "wb") as f:
                f.write(snapshot_bytes)
            
            # Update metadata with actual size and checksum
            metadata.size_bytes = len(snapshot_bytes)
            metadata.checksum = hashlib.sha256(snapshot_bytes).hexdigest()
            
            # Register snapshot
            self._snapshots[snapshot_id] = metadata
            
            # Update chains
            if base_snapshot:
                self._snapshot_chains[base_snapshot.snapshot_id].append(snapshot_id)
            
            # Update latest by key
            for key in snapshot_data.keys():
                if key not in ("_record_count", "_key_count", "_sequence"):
                    self._latest_by_key[key] = metadata
            
            self._save_snapshot_index()
            
            logger.info(f"Created snapshot {snapshot_id} at sequence {sequence}")
            return metadata
    
    def _capture_state(
        self,
        sequence: int,
        base_snapshot: Optional[SnapshotMetadata] = None,
    ) -> Dict[str, Any]:
        """Capture the current state up to the given sequence"""
        data = {
            "_sequence": sequence,
            "_record_count": 0,
            "_key_count": 0,
        }
        
        # Load base state if incremental
        if base_snapshot:
            base_data = self._load_snapshot_data(base_snapshot)
            data.update(base_data)
        
        # Apply all records up to sequence
        record_count = 0
        for key in self.store.keys():
            history = self.store.get_history(key)
            for record in history:
                if record.sequence <= sequence:
                    data[key] = {
                        "value": record.value,
                        "sequence": record.sequence,
                        "timestamp": record.timestamp,
                        "record_type": record.record_type.value,
                    }
                    record_count += 1
        
        data["_record_count"] = record_count
        data["_key_count"] = len([k for k in data.keys() if not k.startswith("_")])
        
        return data
    
    def _load_snapshot_data(self, metadata: SnapshotMetadata) -> Dict[str, Any]:
        """Load snapshot data from disk"""
        if not metadata.path:
            return {}
        
        snapshot_path = Path(metadata.path)
        if not snapshot_path.exists():
            logger.warning(f"Snapshot file not found: {snapshot_path}")
            return {}
        
        with open(snapshot_path, "rb") as f:
            data = f.read()
        
        snapshot = Snapshot.from_bytes(data)
        return snapshot.data
    
    def restore_snapshot(self, snapshot_id: str) -> None:
        """
        Restore the database to a snapshot's state.
        
        Args:
            snapshot_id: ID of the snapshot to restore
        """
        with self._lock:
            if snapshot_id not in self._snapshots:
                raise ValueError(f"Snapshot {snapshot_id} not found")
            
            metadata = self._snapshots[snapshot_id]
            
            # Load snapshot data
            data = self._load_snapshot_data(metadata)
            
            # Build state reconstruction chain
            chain = self._reconstruct_chain(metadata)
            
            # Apply state from chain
            final_state = {}
            for snap_meta in reversed(chain):
                snap_data = self._load_snapshot_data(snap_meta)
                # Filter out metadata keys
                state_keys = [k for k in snap_data.keys() if not k.startswith("_")]
                for key in state_keys:
                    if key not in final_state:
                        final_state[key] = snap_data[key]
            
            # Note: This is a simplified restore. In production, you'd need
            # to handle this more carefully to avoid data loss.
            logger.info(f"Would restore snapshot {snapshot_id}, sequence {metadata.sequence}")
            
    def _reconstruct_chain(self, metadata: SnapshotMetadata) -> List[SnapshotMetadata]:
        """Reconstruct the chain of snapshots leading to the given one"""
        chain = [metadata]
        current = metadata
        
        while current.base_snapshot_id:
            if current.base_snapshot_id not in self._snapshots:
                break
            current = self._snapshots[current.base_snapshot_id]
            chain.append(current)
        
        return chain
    
    def list_snapshots(
        self,
        snapshot_type: Optional[SnapshotType] = None,
        min_sequence: Optional[int] = None,
        max_sequence: Optional[int] = None,
    ) -> List[SnapshotMetadata]:
        """List snapshots matching criteria"""
        results = list(self._snapshots.values())
        
        if snapshot_type:
            results = [s for s in results if s.snapshot_type == snapshot_type]
        if min_sequence is not None:
            results = [s for s in results if s.sequence >= min_sequence]
        if max_sequence is not None:
            results = [s for s in results if s.sequence <= max_sequence]
        
        return sorted(results, key=lambda s: s.sequence, reverse=True)
    
    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot and update chains"""
        with self._lock:
            if snapshot_id not in self._snapshots:
                raise ValueError(f"Snapshot {snapshot_id} not found")
            
            metadata = self._snapshots[snapshot_id]
            
            # Remove from disk
            if metadata.path:
                path = Path(metadata.path)
                if path.exists():
                    path.unlink()
            
            # Update chains
            if metadata.base_snapshot_id:
                chain = self._snapshot_chains[metadata.base_snapshot_id]
                if snapshot_id in chain:
                    chain.remove(snapshot_id)
            
            # Update index
            del self._snapshots[snapshot_id]
            self._save_snapshot_index()
            
            logger.info(f"Deleted snapshot {snapshot_id}")
    
    def compact(
        self,
        strategy: Optional[CompactionStrategy] = None,
        dry_run: bool = False,
    ) -> List[str]:
        """
        Run compaction using the specified strategy.
        
        Args:
            strategy: Compaction strategy to use
            dry_run: If True, only return what would be deleted
            
        Returns:
            List of deleted snapshot IDs
        """
        strategy = strategy or self.config.strategy
        deleted = []
        
        with self._lock:
            snapshots = self.list_snapshots()
            
            # Score each snapshot
            scores = []
            for snapshot in snapshots:
                score = self._calculate_compaction_score(snapshot, strategy)
                scores.append((snapshot, score))
            
            # Sort by score (lower = better candidate for deletion)
            scores.sort(key=lambda x: x[1])
            
            # Determine what to delete
            preserve_count = self.config.preserve_recent
            candidates = [s for s, _ in scores[:-preserve_count] if _ > 0.5]
            
            if dry_run:
                return [s.snapshot_id for s in candidates]
            
            for snapshot in candidates:
                self.delete_snapshot(snapshot.snapshot_id)
                deleted.append(snapshot.snapshot_id)
            
            logger.info(f"Compaction deleted {len(deleted)} snapshots")
            return deleted
    
    def _calculate_compaction_score(
        self,
        snapshot: SnapshotMetadata,
        strategy: CompactionStrategy,
    ) -> float:
        """Calculate compaction score (0-1, higher = better to keep)"""
        if strategy == CompactionStrategy.TIME:
            age_hours = (time.time() - snapshot.timestamp) / 3600
            if age_hours < self.config.min_age_seconds / 3600:
                return 0.0
            if age_hours > self.config.max_age_seconds / 3600:
                return 1.0
            return age_hours / (self.config.max_age_seconds / 3600)
        
        elif strategy == CompactionStrategy.SIZE:
            size_mb = snapshot.size_bytes / (1024 * 1024)
            max_mb = self.config.max_segment_size / (1024 * 1024)
            return min(size_mb / max_mb, 1.0)
        
        elif strategy == CompactionStrategy.COUNT:
            if snapshot.record_count < self.config.min_records:
                return 0.0
            if snapshot.record_count > self.config.max_records:
                return 1.0
            return snapshot.record_count / self.config.max_records
        
        elif strategy == CompactionStrategy.HYBRID:
            time_score = self._calculate_compaction_score(snapshot, CompactionStrategy.TIME)
            size_score = self._calculate_compaction_score(snapshot, CompactionStrategy.SIZE)
            count_score = self._calculate_compaction_score(snapshot, CompactionStrategy.COUNT)
            
            return (
                time_score * self.config.time_weight +
                size_score * self.config.size_weight +
                count_score * self.config.count_weight
            )
        
        return 0.5
    
    def verify_snapshot(self, snapshot_id: str) -> bool:
        """
        Verify snapshot integrity.
        
        Args:
            snapshot_id: ID of snapshot to verify
            
        Returns:
            True if snapshot is valid, False otherwise
        """
        if snapshot_id not in self._snapshots:
            return False
        
        metadata = self._snapshots[snapshot_id]
        
        if not metadata.path:
            return False
        
        path = Path(metadata.path)
        if not path.exists():
            return False
        
        try:
            with open(path, "rb") as f:
                data = f.read()
            
            # Verify checksum
            stored = metadata.checksum
            computed = hashlib.sha256(data).hexdigest()
            
            if stored != computed:
                logger.warning(f"Snapshot {snapshot_id} checksum mismatch")
                return False
            
            # Try to deserialize
            Snapshot.from_bytes(data)
            
            return True
            
        except Exception as e:
            logger.error(f"Snapshot {snapshot_id} verification failed: {e}")
            return False
    
    def get_snapshot_stats(self) -> Dict[str, Any]:
        """Get statistics about snapshots"""
        snapshots = list(self._snapshots.values())
        
        if not snapshots:
            return {
                "total_snapshots": 0,
                "total_size_bytes": 0,
                "by_type": {},
            }
        
        by_type = defaultdict(lambda: {"count": 0, "size_bytes": 0})
        for s in snapshots:
            by_type[s.snapshot_type.value]["count"] += 1
            by_type[s.snapshot_type.value]["size_bytes"] += s.size_bytes
        
        return {
            "total_snapshots": len(snapshots),
            "total_size_bytes": sum(s.size_bytes for s in snapshots),
            "earliest_sequence": min(s.sequence for s in snapshots),
            "latest_sequence": max(s.sequence for s in snapshots),
            "by_type": dict(by_type),
        }
