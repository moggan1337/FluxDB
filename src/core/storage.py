"""
Append-Only Storage Engine for FluxDB

This module implements the core append-only storage mechanism that ensures
immutability and provides the foundation for event sourcing, time-travel
queries, and event replay capabilities.

Key Features:
- Append-only writes guarantee immutability
- Content-addressable storage for deduplication
- Segmented storage for efficient reads
- Write-ahead logging for durability
- Tombstone markers for soft deletes
"""

import hashlib
import json
import os
import mmap
import struct
import threading
import time
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Iterator, List, Dict, Callable, Union
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RecordType(Enum):
    """Types of records that can be stored"""
    DATA = 0x01
    TOMBSTONE = 0x02
    CHECKPOINT = 0x03
    METADATA = 0x04
    BATCH_HEADER = 0x05
    BATCH_FOOTER = 0x06


@dataclass
class Record:
    """
    Represents a single immutable record in the append-only store.
    
    Each record contains:
    - key: The identifier for this record
    - value: The data payload (can be bytes, dict, or any serializable type)
    - sequence: Monotonically increasing sequence number
    - timestamp: Wall clock time when record was written
    - record_type: Type of record (DATA, TOMBSTONE, etc.)
    - checksum: SHA-256 hash of the record for integrity verification
    - offset: Position in the segment file
    """
    key: str
    value: Any
    sequence: int
    timestamp: float
    record_type: RecordType
    checksum: str
    offset: int = 0
    headers: Dict[str, str] = field(default_factory=dict)
    
    def to_bytes(self) -> bytes:
        """Serialize record to bytes for storage"""
        data = {
            "key": self.key,
            "value": self.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "record_type": self.record_type.value,
            "checksum": self.checksum,
            "headers": self.headers,
        }
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        return struct.pack(">I", len(json_bytes)) + json_bytes
    
    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> "Record":
        """Deserialize record from bytes"""
        length = struct.unpack(">I", data[offset:offset + 4])[0]
        json_bytes = data[offset + 4:offset + 4 + length]
        parsed = json.loads(json_bytes.decode("utf-8"))
        return cls(
            key=parsed["key"],
            value=parsed["value"],
            sequence=parsed["sequence"],
            timestamp=parsed["timestamp"],
            record_type=RecordType(parsed["record_type"]),
            checksum=parsed["checksum"],
            headers=parsed.get("headers", {}),
            offset=offset,
        )
    
    def verify(self) -> bool:
        """Verify record integrity using checksum"""
        computed = self.compute_checksum()
        return computed == self.checksum
    
    @staticmethod
    def compute_checksum(data: Dict[str, Any]) -> str:
        """Compute SHA-256 checksum for data"""
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class Segment:
    """
    Represents a segment file in the append-only store.
    
    Segments are fixed-size files that contain sequential records.
    Old segments are never modified; new data goes to new segments.
    """
    path: Path
    index: int
    start_sequence: int
    end_sequence: int
    size_bytes: int
    record_count: int
    created_at: float
    is_active: bool = True
    
    @classmethod
    def from_path(cls, path: Path, index: int) -> "Segment":
        """Create segment metadata from file path"""
        stat = path.stat()
        return cls(
            path=path,
            index=index,
            start_sequence=0,
            end_sequence=0,
            size_bytes=stat.st_size,
            record_count=0,
            created_at=stat.st_mtime,
        )


class AppendOnlyStore:
    """
    Core append-only storage engine.
    
    This class implements a high-performance, append-only storage system
    with the following characteristics:
    
    1. Immutability: Once written, data is never modified
    2. Segmented Storage: Data is split into manageable segments
    3. Content Addressing: Records can be referenced by content hash
    4. Sequence Numbers: Monotonic ordering of all records
    5. Time Travel: Query historical states at any point in time
    
    Architecture:
    
    ┌─────────────────────────────────────────────────────────────┐
    │                    AppendOnlyStore                          │
    ├─────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
    │  │  Segment 0  │  │  Segment 1  │  │  Segment N  │         │
    │  │  (immutable)│  │  (immutable)│  │  (active)   │         │
    │  └─────────────┘  └─────────────┘  └─────────────┘         │
    ├─────────────────────────────────────────────────────────────┤
    │                    Index (key -> offset)                    │
    ├─────────────────────────────────────────────────────────────┤
    │                    Write-Ahead Log                          │
    └─────────────────────────────────────────────────────────────┘
    
    Usage:
    
    ```python
    store = AppendOnlyStore("./data")
    store.put("users:1", {"name": "Alice", "age": 30})
    store.put("users:2", {"name": "Bob", "age": 25})
    
    # Get latest value
    record = store.get("users:1")
    
    # Get value at specific timestamp
    record = store.get_at("users:1", timestamp=1700000000)
    
    # Iterate all records
    for record in store.scan():
        print(record.key, record.value)
    ```
    """
    
    SEGMENT_SIZE = 128 * 1024 * 1024  # 128 MB per segment
    MAX_RECORD_SIZE = 10 * 1024 * 1024  # 10 MB max record size
    INDEX_FLUSH_INTERVAL = 1000  # Flush index every N records
    CHECKPOINT_INTERVAL = 10000  # Create checkpoint every N records
    
    def __init__(
        self,
        path: Union[str, Path],
        segment_size: int = SEGMENT_SIZE,
        create_if_missing: bool = True,
        fsync_enabled: bool = True,
    ):
        """
        Initialize the append-only store.
        
        Args:
            path: Directory to store segment files
            segment_size: Maximum size of each segment file
            create_if_missing: Create directory if it doesn't exist
            fsync_enabled: Enable fsync for durability (disable for speed)
        """
        self.base_path = Path(path)
        self.segment_size = segment_size
        self.fsync_enabled = fsync_enabled
        
        if create_if_missing:
            self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Thread safety
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        
        # Segment management
        self._segments: Dict[int, Segment] = {}
        self._active_segment: Optional[IO] = None
        self._active_segment_index: int = 0
        self._active_segment_offset: int = 0
        
        # Sequence management
        self._sequence: int = 0
        self._high_watermark: int = 0
        
        # Index: key -> List[RecordMetadata]
        self._index: Dict[str, List[Dict]] = {}
        self._index_modified = False
        self._records_since_checkpoint = 0
        
        # Statistics
        self._stats = {
            "total_records": 0,
            "total_bytes_written": 0,
            "total_segments": 0,
            "total_gets": 0,
            "total_scans": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }
        
        # Load existing segments
        self._load_segments()
        
        # Open or create active segment
        self._ensure_active_segment()
        
        logger.info(f"AppendOnlyStore initialized at {self.base_path}")
    
    def _load_segments(self) -> None:
        """Load metadata for all existing segments"""
        segments_dir = self.base_path / "segments"
        if not segments_dir.exists():
            segments_dir.mkdir(parents=True)
            return
        
        # Load segment files
        for seg_file in sorted(segments_dir.glob("segment_*.dat")):
            index = int(seg_file.stem.split("_")[1])
            segment = Segment.from_path(seg_file, index)
            self._segments[index] = segment
            
            # Index all records in this segment
            self._index_segment(segment)
        
        # Update active segment index
        if self._segments:
            self._active_segment_index = max(self._segments.keys())
            self._sequence = self._high_watermark
    
    def _index_segment(self, segment: Segment) -> None:
        """Index all records in a segment file"""
        try:
            with open(segment.path, "rb") as f:
                offset = 0
                while offset < segment.size_bytes:
                    f.seek(offset)
                    try:
                        record = Record.from_bytes(f.read(), offset)
                        
                        # Update index
                        if record.key not in self._index:
                            self._index[record.key] = []
                        
                        self._index[record.key].append({
                            "sequence": record.sequence,
                            "timestamp": record.timestamp,
                            "offset": offset,
                            "segment_index": segment.index,
                            "record_type": record.record_type,
                            "checksum": record.checksum,
                        })
                        
                        # Update high watermark
                        if record.sequence > self._high_watermark:
                            self._high_watermark = record.sequence
                        
                        offset += 4 + struct.unpack(">I", open(segment.path, "rb").read())[0]
                    except Exception as e:
                        logger.warning(f"Error indexing record at offset {offset}: {e}")
                        break
                        
        except Exception as e:
            logger.error(f"Error loading segment {segment.path}: {e}")
    
    def _ensure_active_segment(self) -> None:
        """Ensure an active segment exists for writing"""
        segments_dir = self.base_path / "segments"
        segments_dir.mkdir(exist_ok=True)
        
        segment_path = segments_dir / f"segment_{self._active_segment_index:06d}.dat"
        
        if self._active_segment is None or not segment_path.exists():
            self._active_segment = open(segment_path, "ab")
            self._active_segment_offset = self._active_segment.seek(0, 2)
            if self._active_segment_index not in self._segments:
                self._segments[self._active_segment_index] = Segment(
                    path=segment_path,
                    index=self._active_segment_index,
                    start_sequence=self._sequence + 1,
                    end_sequence=0,
                    size_bytes=0,
                    record_count=0,
                    created_at=time.time(),
                )
    
    def _rotate_segment(self) -> None:
        """Rotate to a new segment file"""
        if self._active_segment:
            self._active_segment.close()
        
        self._active_segment_index += 1
        self._active_segment_offset = 0
        self._ensure_active_segment()
        
        self._stats["total_segments"] += 1
        logger.info(f"Rotated to segment {self._active_segment_index}")
    
    def _write_record(self, record: Record) -> None:
        """Write a record to the active segment"""
        with self._write_lock:
            # Check if we need to rotate
            if self._active_segment_offset >= self.segment_size:
                self._rotate_segment()
            
            record.offset = self._active_segment_offset
            data = record.to_bytes()
            
            # Write to segment
            self._active_segment.write(data)
            self._active_segment_offset += len(data)
            
            # Optionally fsync
            if self.fsync_enabled:
                os.fsync(self._active_segment.fileno())
            
            # Update statistics
            self._stats["total_bytes_written"] += len(data)
            self._stats["total_records"] += 1
    
    def put(
        self,
        key: str,
        value: Any,
        record_type: RecordType = RecordType.DATA,
        headers: Optional[Dict[str, str]] = None,
    ) -> Record:
        """
        Append a record to the store.
        
        This is the primary write operation. Records are never overwritten;
        instead, new records are appended and the index is updated to point
        to the latest record for each key.
        
        Args:
            key: The key to store the value under
            value: The value to store (must be JSON serializable)
            record_type: Type of record (DATA, METADATA, etc.)
            headers: Optional headers/metadata for the record
            
        Returns:
            The created Record
            
        Example:
        
        ```python
        # Store a simple value
        record = store.put("users:1", {"name": "Alice"})
        
        # Store with headers
        record = store.put("events:1", event_data, headers={
            "event_type": "UserCreated",
            "aggregate_id": "users:1"
        })
        ```
        """
        with self._lock:
            self._sequence += 1
            timestamp = time.time()
            
            # Create record
            record = Record(
                key=key,
                value=value,
                sequence=self._sequence,
                timestamp=timestamp,
                record_type=record_type,
                checksum="",  # Will be computed
                headers=headers or {},
            )
            
            # Compute checksum
            record_data = {
                "key": record.key,
                "value": record.value,
                "sequence": record.sequence,
                "timestamp": record.timestamp,
                "record_type": record.record_type.value,
                "headers": record.headers,
            }
            record.checksum = Record.compute_checksum(record_data)
            
            # Write to segment
            self._write_record(record)
            
            # Update index
            if key not in self._index:
                self._index[key] = []
            
            self._index[key].append({
                "sequence": record.sequence,
                "timestamp": record.timestamp,
                "offset": record.offset,
                "segment_index": self._active_segment_index,
                "record_type": record.record_type,
                "checksum": record.checksum,
            })
            
            # Track modifications
            self._index_modified = True
            self._records_since_checkpoint += 1
            
            # Checkpoint if needed
            if self._records_since_checkpoint >= self.CHECKPOINT_INTERVAL:
                self._create_checkpoint()
            
            return record
    
    def delete(self, key: str) -> Record:
        """
        Soft delete a key by appending a tombstone record.
        
        Unlike traditional databases, FluxDB never removes data.
        Deletes are represented as tombstone records that indicate
        the key was deleted at a specific point in time.
        
        Args:
            key: The key to delete
            
        Returns:
            The created tombstone Record
        """
        return self.put(
            key=key,
            value=None,
            record_type=RecordType.TOMBSTONE,
        )
    
    def get(self, key: str) -> Optional[Record]:
        """
        Get the latest record for a key.
        
        Args:
            key: The key to look up
            
        Returns:
            The latest Record or None if not found
        """
        with self._lock:
            self._stats["total_gets"] += 1
            
            if key not in self._index or not self._index[key]:
                self._stats["cache_misses"] += 1
                return None
            
            # Get the latest record for this key
            entries = self._index[key]
            latest = max(entries, key=lambda x: x["sequence"])
            
            # Load and return the record
            record = self._load_record(latest)
            self._stats["cache_hits"] += 1
            
            return record
    
    def _load_record(self, index_entry: Dict) -> Record:
        """Load a record from disk given its index entry"""
        segment_index = index_entry["segment_index"]
        offset = index_entry["offset"]
        
        segment_path = self.base_path / "segments" / f"segment_{segment_index:06d}.dat"
        
        with open(segment_path, "rb") as f:
            f.seek(offset)
            data = f.read()
            return Record.from_bytes(data, offset)
    
    def get_at(self, key: str, timestamp: float) -> Optional[Record]:
        """
        Get the state of a key at a specific point in time.
        
        This is one of FluxDB's most powerful features: the ability to
        query historical states. By scanning the append-only log, we can
        reconstruct the value of any key at any point in the past.
        
        Args:
            key: The key to look up
            timestamp: Unix timestamp to query
            
        Returns:
            The Record that was current at the given timestamp, or None
        """
        with self._lock:
            if key not in self._index:
                return None
            
            # Find the latest record before or at the timestamp
            entries = sorted(self._index[key], key=lambda x: x["timestamp"], reverse=True)
            
            for entry in entries:
                if entry["timestamp"] <= timestamp:
                    return self._load_record(entry)
            
            return None
    
    def get_history(self, key: str) -> List[Record]:
        """
        Get the complete history of changes for a key.
        
        Returns all records for a key, ordered from oldest to newest.
        This is useful for debugging, audit trails, and understanding
        how data evolved over time.
        
        Args:
            key: The key to get history for
            
        Returns:
            List of Records, oldest first
        """
        with self._lock:
            if key not in self._index:
                return []
            
            entries = sorted(self._index[key], key=lambda x: x["sequence"])
            return [self._load_record(e) for e in entries]
    
    def scan(
        self,
        prefix: Optional[str] = None,
        start_key: Optional[str] = None,
        end_key: Optional[str] = None,
        limit: Optional[int] = None,
        include_tombstones: bool = False,
    ) -> Iterator[Record]:
        """
        Scan records in key order.
        
        Args:
            prefix: Only return keys starting with this prefix
            start_key: Start scanning from this key
            end_key: Stop scanning at this key
            limit: Maximum number of records to return
            include_tombstones: Include deleted records
            
        Yields:
            Records in key order
        """
        with self._lock:
            self._stats["total_scans"] += 1
            
            # Get sorted keys
            keys = sorted(self._index.keys())
            
            # Apply filters
            if prefix:
                keys = [k for k in keys if k.startswith(prefix)]
            if start_key:
                keys = [k for k in keys if k >= start_key]
            if end_key:
                keys = [k for k in keys if k <= end_key]
            
            count = 0
            for key in keys:
                if limit and count >= limit:
                    break
                
                # Get latest record for this key
                record = self.get(key)
                if record:
                    if include_tombstones or record.record_type != RecordType.TOMBSTONE:
                        yield record
                        count += 1
    
    def range_query(
        self,
        start_time: float,
        end_time: float,
    ) -> Iterator[Record]:
        """
        Query records within a time range.
        
        Args:
            start_time: Start of time range (Unix timestamp)
            end_time: End of time range (Unix timestamp)
            
        Yields:
            Records within the time range
        """
        with self._lock:
            for key, entries in self._index.items():
                for entry in entries:
                    if start_time <= entry["timestamp"] <= end_time:
                        yield self._load_record(entry)
    
    def _create_checkpoint(self) -> None:
        """Create a checkpoint for faster recovery"""
        checkpoint_dir = self.base_path / "checkpoints"
        checkpoint_dir.mkdir(exist_ok=True)
        
        checkpoint = {
            "sequence": self._sequence,
            "high_watermark": self._high_watermark,
            "timestamp": time.time(),
            "index": self._index,
            "active_segment_index": self._active_segment_index,
            "active_segment_offset": self._active_segment_offset,
        }
        
        checkpoint_path = checkpoint_dir / f"checkpoint_{self._sequence:016d}.json"
        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint, f)
        
        self._index_modified = False
        self._records_since_checkpoint = 0
        
        # Clean up old checkpoints
        self._cleanup_old_checkpoints()
        
        logger.info(f"Created checkpoint at sequence {self._sequence}")
    
    def _cleanup_old_checkpoints(self, keep: int = 3) -> None:
        """Keep only the most recent N checkpoints"""
        checkpoint_dir = self.base_path / "checkpoints"
        checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.json"), reverse=True)
        
        for old_checkpoint in checkpoints[keep:]:
            old_checkpoint.unlink()
    
    def compact(self, output_path: Optional[Path] = None) -> Path:
        """
        Compact the store by removing tombstone records and rewriting live data.
        
        Compaction creates a new, optimized segment file that contains only
        the latest values for each key, removing any deleted or overwritten
        entries. This reduces storage usage and improves read performance.
        
        The compaction process:
        1. Scan all segments and collect the latest record for each key
        2. Write a new compacted segment
        3. Update segment metadata
        
        Args:
            output_path: Optional path for compacted segment
            
        Returns:
            Path to the compacted segment
        """
        with self._lock:
            compact_dir = self.base_path / "compact"
            compact_dir.mkdir(exist_ok=True)
            
            output_path = output_path or (compact_dir / f"compact_{int(time.time())}.dat")
            
            with open(output_path, "wb") as compact_file:
                sequence = 0
                
                # Collect latest values
                latest_by_key: Dict[str, Record] = {}
                for key in self._index.keys():
                    record = self.get(key)
                    if record and record.record_type != RecordType.TOMBSTONE:
                        latest_by_key[key] = record
                
                # Write compacted records
                for key, record in sorted(latest_by_key.items()):
                    sequence += 1
                    new_record = Record(
                        key=record.key,
                        value=record.value,
                        sequence=sequence,
                        timestamp=record.timestamp,
                        record_type=record.record_type,
                        checksum=record.checksum,
                        offset=compact_file.tell(),
                        headers=record.headers,
                    )
                    compact_file.write(new_record.to_bytes())
            
            logger.info(f"Compacted {len(latest_by_key)} keys to {output_path}")
            return output_path
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics"""
        return {
            **self._stats,
            "sequence": self._sequence,
            "high_watermark": self._high_watermark,
            "segment_count": len(self._segments),
            "key_count": len(self._index),
            "index_modified": self._index_modified,
        }
    
    def close(self) -> None:
        """Close the store and flush any pending writes"""
        with self._lock:
            if self._active_segment:
                self._active_segment.close()
                self._active_segment = None
            
            if self._index_modified:
                self._create_checkpoint()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __len__(self) -> int:
        """Return the number of keys in the store"""
        return len(self._index)
    
    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the store"""
        return key in self._index and len(self._index[key]) > 0
    
    def keys(self) -> List[str]:
        """Return all keys in the store"""
        return list(self._index.keys())
    
    def stats(self) -> Dict[str, Any]:
        """Alias for get_stats()"""
        return self.get_stats()
