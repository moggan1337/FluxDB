"""
Time-Travel Queries for FluxDB

This module provides point-in-time query capabilities, allowing you to
view the state of the database at any point in history.

Key Features:
- Point-in-time queries (state at timestamp)
- Time-range queries (state changes in range)
- History queries (all changes for a key)
- Diff queries (changes between two points)
- Temporal aggregations
- As-of queries with versioning

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    Time Travel Engine                            │
├─────────────────────────────────────────────────────────────────┤
│  Query Types:                                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ as_of(timestamp)     - State at point in time           │   │
│  │ between(start, end)  - Changes in time range            │   │
│  │ history(key)         - All changes for key              │   │
│  │ diff(t1, t2)         - Changes between two times        │   │
│  │ versions(key)        - All versions of a key            │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  Query Optimization:                                             │
│  - Index-based lookups for common time ranges                   │
│  - Binary search on sorted timestamps                           │
│  - Materialized time slices                                     │
│  - Cached historical snapshots                                  │
└─────────────────────────────────────────────────────────────────┘

Example Usage:

```python
from fluxdb import TimeTravelStore

store = TimeTravelStore("./data")

# Store some data
store.put("user:1", {"name": "Alice", "age": 30})
time.sleep(1)
store.put("user:1", {"name": "Alice", "age": 31})
time.sleep(1)
store.put("user:1", {"name": "Alice", "age": 32})

# Query state at specific time
past_state = store.as_of("user:1", timestamp=1700000000)

# Get complete history
history = store.history("user:1")
for entry in history:
    print(f"At {entry.timestamp}: {entry.value}")

# Find changes in time range
changes = store.between("user:1", start=1700000000, end=1700000100)

# Compare two points in time
diff = store.diff("user:1", t1=1700000000, t2=1700000100)
```

Terminology:
- Point-in-time: A specific moment in time (timestamp)
- Time range: An interval between two timestamps
- Version: A specific state of a key at a sequence number
- History: The complete sequence of changes to a key
- Diff: The differences between two states
"""

import bisect
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union

from .storage import AppendOnlyStore, Record, RecordType

import logging

logger = logging.getLogger(__name__)


@dataclass
class TemporalEntry:
    """
    Represents a point-in-time entry in temporal queries.
    
    Contains the value and metadata about when it was valid.
    """
    key: str
    value: Any
    timestamp: float
    sequence: int
    valid_from: float
    valid_to: Optional[float] = None
    is_deleted: bool = False
    
    def duration(self) -> Optional[float]:
        """Get the duration this entry was valid"""
        if self.valid_to is None:
            return None
        return self.valid_to - self.valid_from
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "is_deleted": self.is_deleted,
        }


@dataclass
class DiffEntry:
    """
    Represents a difference between two points in time.
    """
    key: str
    change_type: str  # "added", "removed", "modified"
    old_value: Any = None
    new_value: Any = None
    old_timestamp: Optional[float] = None
    new_timestamp: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "change_type": self.change_type,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "old_timestamp": self.old_timestamp,
            "new_timestamp": self.new_timestamp,
        }


@dataclass
class TimeRange:
    """Represents a time range for queries"""
    start: float
    end: float
    
    def contains(self, timestamp: float) -> bool:
        """Check if timestamp falls within range"""
        return self.start <= timestamp <= self.end
    
    def overlaps(self, other: "TimeRange") -> bool:
        """Check if this range overlaps with another"""
        return self.start <= other.end and self.end >= other.start
    
    @property
    def duration(self) -> float:
        """Get duration of the range"""
        return self.end - self.start


class TimeSliceCache:
    """
    Cache for materialized time slices.
    
    Stores snapshots of the database state at specific points in time
    for fast point-in-time queries.
    """
    
    def __init__(self, max_entries: int = 100):
        self.max_entries = max_entries
        self._cache: Dict[float, Dict[str, Any]] = {}
        self._access_times: Dict[float, float] = {}
        self._lock = threading.RLock()
    
    def get(self, timestamp: float, tolerance: float = 1.0) -> Optional[Dict[str, Any]]:
        """Get cached state for timestamp (with tolerance)"""
        with self._lock:
            for cached_ts, state in self._cache.items():
                if abs(cached_ts - timestamp) <= tolerance:
                    self._access_times[timestamp] = time.time()
                    return state
            return None
    
    def put(self, timestamp: float, state: Dict[str, Any]) -> None:
        """Cache a state snapshot"""
        with self._lock:
            # Evict if necessary
            if len(self._cache) >= self.max_entries:
                self._evict_oldest()
            
            self._cache[timestamp] = state
            self._access_times[timestamp] = time.time()
    
    def _evict_oldest(self) -> None:
        """Evict least recently accessed entry"""
        if not self._access_times:
            return
        
        oldest = min(self._access_times.items(), key=lambda x: x[1])
        del self._cache[oldest[0]]
        del self._access_times[oldest[0]]
    
    def invalidate(self, timestamp: Optional[float] = None) -> None:
        """Invalidate cache entries"""
        with self._lock:
            if timestamp is None:
                self._cache.clear()
                self._access_times.clear()
            elif timestamp in self._cache:
                del self._cache[timestamp]
                del self._access_times[timestamp]
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            return {
                "entries": len(self._cache),
                "max_entries": self.max_entries,
                "timestamps": sorted(self._cache.keys()),
            }


class TimeTravelStore:
    """
    Time-travel capable store built on AppendOnlyStore.
    
    Provides point-in-time query capabilities by leveraging the
    immutable append-only nature of the underlying storage.
    
    The TimeTravelStore adds a temporal layer on top of the base
    store, enabling:
    - Point-in-time queries (as_of)
    - Time-range queries (between)
    - Change history (history)
    - State comparison (diff)
    - Version tracking (versions)
    
    Usage:
    
    ```python
    # Create time-travel store
    store = TimeTravelStore("./data")
    
    # Basic operations (same as AppendOnlyStore)
    store.put("user:1", {"name": "Alice"})
    store.get("user:1")
    
    # Time-travel queries
    past = store.as_of("user:1", timestamp=1700000000)
    history = store.history("user:1")
    changes = store.diff("user:1", t1=1700000000, t2=1700000100)
    ```
    """
    
    def __init__(
        self,
        path: Union[str, Path],
        cache_enabled: bool = True,
        cache_size: int = 100,
        create_if_missing: bool = True,
    ):
        """
        Initialize the time-travel store.
        
        Args:
            path: Directory for storage
            cache_enabled: Enable time slice caching
            cache_size: Maximum cache entries
            create_if_missing: Create directory if missing
        """
        from pathlib import Path as PP
        self.base_path = PP(path) if isinstance(path, str) else path
        
        # Initialize base store
        self.store = AppendOnlyStore(
            self.base_path / "store",
            create_if_missing=create_if_missing,
        )
        
        # Time slice cache
        self.cache_enabled = cache_enabled
        self._cache = TimeSliceCache(max_entries=cache_size) if cache_enabled else None
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        logger.info(f"TimeTravelStore initialized at {self.base_path}")
    
    def put(
        self,
        key: str,
        value: Any,
        timestamp: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Record:
        """
        Store a value (alias for store.put).
        
        Args:
            key: The key to store
            value: The value to store
            timestamp: Optional timestamp (default: now)
            headers: Optional headers
        """
        timestamp = timestamp or time.time()
        
        record = self.store.put(
            key=key,
            value=value,
            record_type=RecordType.DATA,
            headers=headers,
        )
        
        # Invalidate cache
        if self._cache:
            self._cache.invalidate()
        
        return record
    
    def delete(self, key: str, timestamp: Optional[float] = None) -> Record:
        """
        Delete a key (soft delete with tombstone).
        
        Args:
            key: The key to delete
            timestamp: Optional timestamp
        """
        timestamp = timestamp or time.time()
        record = self.store.delete(key)
        
        if self._cache:
            self._cache.invalidate()
        
        return record
    
    def get(self, key: str) -> Optional[Record]:
        """Get the latest value for a key"""
        return self.store.get(key)
    
    def as_of(
        self,
        key: str,
        timestamp: Optional[float] = None,
        sequence: Optional[int] = None,
    ) -> Optional[TemporalEntry]:
        """
        Get the state of a key at a specific point in time.
        
        This is one of the most powerful features of FluxDB. Given a
        timestamp or sequence number, this method returns what the
        value of the key was at that moment.
        
        Args:
            key: The key to query
            timestamp: Unix timestamp to query (optional if sequence provided)
            sequence: Specific sequence number to query
            
        Returns:
            TemporalEntry with the historical state, or None if not found
            
        Example:
        
        ```python
        # Get state at specific timestamp
        past = store.as_of("user:1", timestamp=1700000000)
        print(f"User was: {past.value}")
        
        # Get state at specific sequence
        past = store.as_of("user:1", sequence=1500)
        print(f"User was: {past.value}")
        ```
        """
        with self._lock:
            # Resolve timestamp
            if timestamp is None and sequence is None:
                timestamp = time.time()
            
            if sequence is not None:
                record = self._get_by_sequence(key, sequence)
            else:
                record = self.store.get_at(key, timestamp)
            
            if record is None:
                return None
            
            # Calculate valid_to (next change or None)
            valid_to = None
            history = self.store.get_history(key)
            for r in history:
                if r.sequence > record.sequence:
                    valid_to = r.timestamp
                    break
            
            return TemporalEntry(
                key=key,
                value=record.value,
                timestamp=record.timestamp,
                sequence=record.sequence,
                valid_from=record.timestamp,
                valid_to=valid_to,
                is_deleted=record.record_type == RecordType.TOMBSTONE,
            )
    
    def _get_by_sequence(self, key: str, sequence: int) -> Optional[Record]:
        """Get record by sequence number"""
        history = self.store.get_history(key)
        for record in history:
            if record.sequence == sequence:
                return record
        return None
    
    def history(
        self,
        key: str,
        include_tombstones: bool = False,
    ) -> List[TemporalEntry]:
        """
        Get the complete history of changes for a key.
        
        Returns all versions of the key, from oldest to newest,
        with metadata about when each version was valid.
        
        Args:
            key: The key to get history for
            include_tombstones: Include deleted entries
            
        Returns:
            List of TemporalEntry objects, oldest first
            
        Example:
        
        ```python
        history = store.history("user:1")
        for entry in history:
            print(f"At {entry.timestamp}: {entry.value}")
            print(f"  Valid from {entry.valid_from} to {entry.valid_to}")
        ```
        """
        records = self.store.get_history(key)
        entries = []
        
        for i, record in enumerate(records):
            if record.record_type == RecordType.TOMBSTONE and not include_tombstones:
                continue
            
            valid_to = None
            if i + 1 < len(records):
                valid_to = records[i + 1].timestamp
            
            entries.append(TemporalEntry(
                key=key,
                value=record.value,
                timestamp=record.timestamp,
                sequence=record.sequence,
                valid_from=record.timestamp,
                valid_to=valid_to,
                is_deleted=record.record_type == RecordType.TOMBSTONE,
            ))
        
        return entries
    
    def versions(self, key: str) -> List[int]:
        """
        Get all version (sequence) numbers for a key.
        
        Args:
            key: The key to get versions for
            
        Returns:
            List of sequence numbers
        """
        history = self.store.get_history(key)
        return [r.sequence for r in history]
    
    def between(
        self,
        key: str,
        start: float,
        end: float,
    ) -> List[TemporalEntry]:
        """
        Get all changes to a key within a time range.
        
        Args:
            key: The key to query
            start: Start timestamp
            end: End timestamp
            
        Returns:
            List of TemporalEntry objects within the range
        """
        history = self.history(key, include_tombstones=True)
        return [
            entry for entry in history
            if start <= entry.timestamp <= end
        ]
    
    def diff(
        self,
        key: str,
        t1: float,
        t2: float,
    ) -> List[DiffEntry]:
        """
        Compare the state of a key at two points in time.
        
        Returns the differences between the two states, including
        what changed, was added, or was removed.
        
        Args:
            key: The key to compare
            t1: First timestamp
            t2: Second timestamp
            
        Returns:
            List of DiffEntry objects describing the changes
            
        Example:
        
        ```python
        diff = store.diff("user:1", t1=1700000000, t2=1700000100)
        for change in diff:
            print(f"{change.change_type}: {change.old_value} -> {change.new_value}")
        ```
        """
        state1 = self.as_of(key, timestamp=t1)
        state2 = self.as_of(key, timestamp=t2)
        
        if state1 is None and state2 is None:
            return []
        
        if state1 is None:
            return [DiffEntry(
                key=key,
                change_type="added",
                new_value=state2.value,
                new_timestamp=state2.timestamp,
            )]
        
        if state2 is None:
            return [DiffEntry(
                key=key,
                change_type="removed",
                old_value=state1.value,
                old_timestamp=state1.timestamp,
            )]
        
        if state1.value != state2.value:
            return [DiffEntry(
                key=key,
                change_type="modified",
                old_value=state1.value,
                new_value=state2.value,
                old_timestamp=state1.timestamp,
                new_timestamp=state2.timestamp,
            )]
        
        return []
    
    def diff_all(
        self,
        t1: float,
        t2: float,
        prefix: Optional[str] = None,
    ) -> List[DiffEntry]:
        """
        Diff all keys between two timestamps.
        
        Args:
            t1: First timestamp
            t2: Second timestamp
            prefix: Optional key prefix filter
            
        Returns:
            List of all changes between the two times
        """
        keys = self.store.keys()
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        
        all_diffs = []
        for key in keys:
            all_diffs.extend(self.diff(key, t1, t2))
        
        return all_diffs
    
    def snapshot_at(
        self,
        timestamp: float,
        prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get a complete snapshot of all data at a point in time.
        
        Args:
            timestamp: The timestamp to snapshot
            prefix: Optional key prefix filter
            
        Returns:
            Dictionary of key -> value for all keys at that time
        """
        # Check cache first
        if self._cache:
            cached = self._cache.get(timestamp)
            if cached is not None:
                if prefix:
                    return {k: v for k, v in cached.items() if k.startswith(prefix)}
                return cached
        
        # Build snapshot
        snapshot = {}
        keys = self.store.keys()
        
        for key in keys:
            if prefix and not key.startswith(prefix):
                continue
            
            entry = self.as_of(key, timestamp=timestamp)
            if entry and not entry.is_deleted:
                snapshot[key] = entry.value
        
        # Cache if enabled
        if self._cache:
            self._cache.put(timestamp, snapshot)
        
        return snapshot
    
    def temporal_aggregate(
        self,
        key: str,
        window: str = "1h",
        aggregator: Callable[[List[Any]], Any] = sum,
    ) -> List[Tuple[float, Any]]:
        """
        Aggregate values over time windows.
        
        Args:
            key: The key to aggregate
            window: Time window (e.g., "1h", "30m", "1d")
            aggregator: Function to aggregate values
            
        Returns:
            List of (timestamp, aggregated_value) tuples
        """
        history = self.history(key)
        
        # Parse window
        window_seconds = self._parse_window(window)
        
        # Group by window
        windows: Dict[int, List[Any]] = defaultdict(list)
        for entry in history:
            window_start = int(entry.timestamp / window_seconds) * window_seconds
            if not entry.is_deleted:
                windows[window_start].append(entry.value)
        
        # Aggregate
        results = []
        for window_start in sorted(windows.keys()):
            if windows[window_start]:
                aggregated = aggregator(windows[window_start])
                results.append((window_start, aggregated))
        
        return results
    
    def _parse_window(self, window: str) -> float:
        """Parse time window string to seconds"""
        units = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
        }
        
        for unit, seconds in units.items():
            if window.endswith(unit):
                value = int(window[:-1])
                return value * seconds
        
        return 3600  # Default to 1 hour
    
    def changes_since(
        self,
        key: str,
        since: float,
    ) -> List[TemporalEntry]:
        """
        Get all changes to a key since a timestamp.
        
        Args:
            key: The key to track
            since: Timestamp to get changes from
            
        Returns:
            List of TemporalEntry objects
        """
        return self.between(key, since, time.time())
    
    def until_changed(
        self,
        key: str,
        from_timestamp: float,
        timeout: float = 30.0,
    ) -> Optional[TemporalEntry]:
        """
        Wait until a key changes from a given state.
        
        Args:
            key: The key to watch
            from_timestamp: Current timestamp
            timeout: Maximum time to wait
            
        Returns:
            The new TemporalEntry when changed, or None on timeout
        """
        start = time.time()
        
        while time.time() - start < timeout:
            current = self.as_of(key)
            
            if current and current.timestamp > from_timestamp:
                return current
            
            time.sleep(0.1)  # Poll interval
        
        return None
    
    def revert_to(
        self,
        key: str,
        timestamp: float,
    ) -> Record:
        """
        Revert a key to its state at a specific timestamp.
        
        This creates a new record with the historical value,
        effectively "undoing" changes.
        
        Args:
            key: The key to revert
            timestamp: The timestamp to revert to
            
        Returns:
            The new record
        """
        historical = self.as_of(key, timestamp=timestamp)
        
        if historical is None:
            raise ValueError(f"No data found for {key} at timestamp {timestamp}")
        
        return self.put(key, historical.value)
    
    def until(
        self,
        key: str,
        predicate: Callable[[Any], bool],
        from_timestamp: Optional[float] = None,
        timeout: float = 30.0,
    ) -> Optional[TemporalEntry]:
        """
        Wait until a predicate is satisfied.
        
        Args:
            key: The key to watch
            predicate: Function that returns True when condition is met
            from_timestamp: Start from this timestamp
            timeout: Maximum time to wait
            
        Returns:
            The TemporalEntry when predicate is satisfied
        """
        start = time.time()
        from_ts = from_timestamp or time.time()
        
        while time.time() - start < timeout:
            current = self.as_of(key)
            
            if current and predicate(current.value):
                return current
            
            time.sleep(0.1)
        
        return None
    
    def time_range_of_value(
        self,
        key: str,
        value: Any,
    ) -> Optional[TimeRange]:
        """
        Find when a key had a specific value.
        
        Args:
            key: The key to search
            value: The value to find
            
        Returns:
            TimeRange when the key had this value, or None
        """
        history = self.history(key)
        
        for entry in history:
            if entry.value == value:
                start = entry.timestamp
                end = entry.valid_to or time.time()
                return TimeRange(start=start, end=end)
        
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics"""
        return {
            "store_stats": self.store.get_stats(),
            "cache_stats": self._cache.stats() if self._cache else None,
        }
    
    def close(self) -> None:
        """Close the store"""
        self.store.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Backwards compatibility alias
from pathlib import Path
TimeTravelStore.__init__.__doc__ = """
Initialize the time-travel store.

Args:
    path: Directory for storage (str or Path)
    cache_enabled: Enable time slice caching
    cache_size: Maximum cache entries
    create_if_missing: Create directory if missing
"""
