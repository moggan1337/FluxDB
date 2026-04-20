"""
CRDT Registers for FluxDB

This module implements CRDT (Conflict-free Replicated Data Type) registers,
which provide eventually consistent key-value storage with automatic conflict
resolution.

Types of Registers:
1. LWW-Register (Last-Writer-Wins): Simple timestamp-based resolution
2. MV-Register (Multi-Value Register): Preserves all conflicting values
3. OR-Set (Observed-Remove Set): Add-only set with removes

Key Features:
- Automatic merge without coordination
- Configurable timestamp providers
- Tombstone-based deletes
- Vector clocks for causality

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    CRDT Register Types                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  LWW-Register (Last-Writer-Wins):                                │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Node A: value="x" @ t=100                                  ││
│  │  Node B: value="y" @ t=101  ──►  Merge: value="y" (t=101)   ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  OR-Set (Observed-Remove):                                       │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Node A: {a, b}         ┌──────────────────────────────────┐││
│  │  Node B: {a, c}  ──►   │  Merge: {a, b, c} - all preserved │││
│  │                        └──────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Usage:

```python
from fluxdb.crdt import LWWRegister, ORSet, CRDTStore

# Last-Writer-Wins Register
register = LWWRegister()
register.set("value1", node_id="A", timestamp=100)
register.set("value2", node_id="B", timestamp=101)
print(register.get())  # "value2" - later timestamp wins

# Merge registers from different nodes
reg1 = LWWRegister()
reg1.set("hello", node_id="A", timestamp=100)

reg2 = LWWRegister()
reg2.set("world", node_id="B", timestamp=101)

reg1.merge(reg2)
print(reg1.get())  # "world"
```
"""

import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import logging

logger = logging.getLogger(__name__)


@dataclass
class Timestamp:
    """
    Hybrid logical clock timestamp.
    
    Combines physical time with logical counter for ordering.
    """
    physical: float
    logical: int
    node_id: str
    
    def __lt__(self, other: "Timestamp") -> bool:
        """Compare timestamps"""
        if self.physical != other.physical:
            return self.physical < other.physical
        if self.logical != other.logical:
            return self.logical < other.logical
        return self.node_id < other.node_id
    
    def __le__(self, other: "Timestamp") -> bool:
        return self == other or self < other
    
    def __gt__(self, other: "Timestamp") -> bool:
        return other < self
    
    def __ge__(self, other: "Timestamp") -> bool:
        return self == other or other < self
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return False
        return (self.physical == other.physical and 
                self.logical == other.logical and 
                self.node_id == other.node_id)
    
    def __hash__(self) -> int:
        return hash((self.physical, self.logical, self.node_id))
    
    def __repr__(self) -> str:
        return f"Timestamp({self.physical}, {self.logical}, {self.node_id})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "physical": self.physical,
            "logical": self.logical,
            "node_id": self.node_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Timestamp":
        return cls(
            physical=data["physical"],
            logical=data["logical"],
            node_id=data["node_id"],
        )
    
    @classmethod
    def now(cls, node_id: str, logical: int = 0) -> "Timestamp":
        """Create timestamp for current time"""
        return cls(
            physical=time.time(),
            logical=logical,
            node_id=node_id,
        )


class VectorClock:
    """
    Vector clock for tracking causality.
    
    Each node maintains its own counter. Vector clocks allow
    detection of concurrent updates and causal ordering.
    """
    
    def __init__(self, clocks: Optional[Dict[str, int]] = None):
        self._clocks: Dict[str, int] = clocks or {}
    
    def increment(self, node_id: str) -> "VectorClock":
        """Increment counter for a node"""
        self._clocks[node_id] = self._clocks.get(node_id, 0) + 1
        return self
    
    def get(self, node_id: str) -> int:
        """Get counter for a node"""
        return self._clocks.get(node_id, 0)
    
    def merge(self, other: "VectorClock") -> "VectorClock":
        """Merge with another vector clock"""
        for node_id, clock in other._clocks.items():
            self._clocks[node_id] = max(self._clocks.get(node_id, 0), clock)
        return self
    
    def happens_before(self, other: "VectorClock") -> bool:
        """Check if this clock happens before another"""
        dominated = False
        for node_id in set(list(self._clocks.keys()) + list(other._clocks.keys())):
            self_clock = self._clocks.get(node_id, 0)
            other_clock = other._clocks.get(node_id, 0)
            
            if self_clock > other_clock:
                return False
            if self_clock < other_clock:
                dominated = True
        
        return dominated
    
    def is_concurrent(self, other: "VectorClock") -> bool:
        """Check if two clocks are concurrent (neither happens-before)"""
        return not self.happens_before(other) and not other.happens_before(self)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return False
        return self._clocks == other._clocks
    
    def __repr__(self) -> str:
        return f"VectorClock({self._clocks})"
    
    def to_dict(self) -> Dict[str, int]:
        return self._clocks.copy()
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "VectorClock":
        return cls(clocks=data)


@dataclass
class RegisterEntry:
    """Entry in a register with metadata"""
    value: Any
    timestamp: Timestamp
    tombstone: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "timestamp": self.timestamp.to_dict(),
            "tombstone": self.tombstone,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RegisterEntry":
        return cls(
            value=data["value"],
            timestamp=Timestamp.from_dict(data["timestamp"]),
            tombstone=data.get("tombstone", False),
        )


class CRDTRegister(ABC):
    """
    Base class for CRDT registers.
    """
    
    @abstractmethod
    def merge(self, other: "CRDTRegister") -> None:
        """Merge with another register"""
        pass
    
    @abstractmethod
    def get(self) -> Optional[Any]:
        """Get current value"""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CRDTRegister":
        """Deserialize from dictionary"""
        pass


class LWWRegister(CRDTRegister):
    """
    Last-Writer-Wins Register.
    
    The most recent write (by timestamp) wins. Simple but may lose
    updates if clocks are not synchronized.
    
    Properties:
    - Convergent: All replicas converge to same value
    - Monotonic: Values only increase in timestamp
    - Simple: Easy to implement and understand
    
    Example:
    
    ```python
    reg = LWWRegister(node_id="node1")
    reg.set("hello")
    print(reg.get())  # "hello"
    
    # Concurrent updates
    reg2 = LWWRegister(node_id="node2")
    reg2.set("world")
    
    reg.merge(reg2)
    print(reg.get())  # Either "hello" or "world" based on timestamps
    ```
    """
    
    def __init__(
        self,
        value: Any = None,
        timestamp: Optional[Timestamp] = None,
        node_id: Optional[str] = None,
    ):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self._entry = None
        
        if value is not None:
            self.set(value, timestamp)
    
    def set(
        self,
        value: Any,
        timestamp: Optional[Timestamp] = None,
        node_id: Optional[str] = None,
    ) -> None:
        """
        Set the value with timestamp.
        
        Args:
            value: The value to set
            timestamp: Optional timestamp (generates current time if not provided)
            node_id: Optional node ID (uses instance node_id if not provided)
        """
        if timestamp is None:
            timestamp = Timestamp.now(node_id or self.node_id)
        
        self._entry = RegisterEntry(
            value=value,
            timestamp=timestamp,
            tombstone=False,
        )
    
    def delete(self, timestamp: Optional[Timestamp] = None) -> None:
        """Mark the register as deleted (tombstone)"""
        self.set(None, timestamp)
        if self._entry:
            self._entry.tombstone = True
    
    def get(self) -> Optional[Any]:
        """Get the current value (None if deleted)"""
        if self._entry is None or self._entry.tombstone:
            return None
        return self._entry.value
    
    def get_with_metadata(self) -> Optional[RegisterEntry]:
        """Get value with timestamp metadata"""
        return self._entry
    
    def merge(self, other: "LWWRegister") -> None:
        """
        Merge with another LWW register.
        
        The register with the later timestamp wins.
        """
        if not isinstance(other, LWWRegister):
            raise TypeError("Can only merge with LWWRegister")
        
        if self._entry is None:
            self._entry = other._entry
            return
        
        if other._entry is None:
            return
        
        # Compare timestamps
        if other._entry.timestamp > self._entry.timestamp:
            self._entry = other._entry
    
    def __repr__(self) -> str:
        value = self.get()
        ts = self._entry.timestamp if self._entry else None
        return f"LWWRegister(value={value!r}, timestamp={ts})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "lww_register",
            "node_id": self.node_id,
            "entry": self._entry.to_dict() if self._entry else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LWWRegister":
        register = cls(node_id=data["node_id"])
        if data.get("entry"):
            register._entry = RegisterEntry.from_dict(data["entry"])
        return register


class MVRegister(CRDTRegister):
    """
    Multi-Value Register.
    
    Preserves all concurrent values instead of choosing one.
    Useful when you need to see all possible values and resolve
    conflicts later (e.g., showing users both options).
    
    Example:
    
    ```python
    reg = MVRegister()
    reg.add("value1", node_id="A", timestamp=100)
    reg.add("value2", node_id="B", timestamp=101)
    
    print(reg.get())  # {"value1", "value2"} - both values
    ```
    """
    
    def __init__(self):
        self._values: Dict[Any, RegisterEntry] = {}
    
    def add(
        self,
        value: Any,
        timestamp: Optional[Timestamp] = None,
        node_id: Optional[str] = None,
    ) -> None:
        """Add a value to the register"""
        if timestamp is None:
            timestamp = Timestamp.now(node_id or "default")
        
        # Check if this value exists with different timestamp
        if value in self._values:
            existing = self._values[value]
            if timestamp > existing.timestamp:
                self._values[value] = RegisterEntry(value, timestamp)
        else:
            self._values[value] = RegisterEntry(value, timestamp)
    
    def remove(self, value: Any) -> None:
        """Remove a value from the register"""
        if value in self._values:
            del self._values[value]
    
    def get(self) -> Set[Any]:
        """Get all values"""
        return set(self._values.keys())
    
    def merge(self, other: "MVRegister") -> None:
        """Merge with another MV register"""
        if not isinstance(other, MVRegister):
            raise TypeError("Can only merge with MVRegister")
        
        # Union of all values
        for value, entry in other._values.items():
            if value not in self._values:
                self._values[value] = entry
            elif entry.timestamp > self._values[value].timestamp:
                self._values[value] = entry
    
    def __repr__(self) -> str:
        return f"MVRegister(values={self.get()})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "mv_register",
            "values": {k: v.to_dict() for k, v in self._values.items()},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MVRegister":
        reg = cls()
        for value, entry_data in data["values"].items():
            reg._values[value] = RegisterEntry.from_dict(entry_data)
        return reg


class ORSet(CRDTRegister):
    """
    Observed-Remove Set.
    
    An add-only set where elements can be removed, but removals
    are only effective for elements that were observed before removal.
    
    Properties:
    - Adds are always visible immediately
    - Removes only affect elements seen before the remove
    - Concurrent adds of same element preserve the element
    - Concurrent remove + add may preserve element (depends on timing)
    
    Example:
    
    ```python
    s = ORSet(node_id="A")
    s.add("item1")
    s.add("item2")
    
    s2 = ORSet(node_id="B")
    s2.add("item2")
    s2.add("item3")
    s2.remove("item1")
    
    s.merge(s2)
    print(s.get())  # {"item2", "item3"}
    ```
    """
    
    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        # tag -> (value, timestamp)
        self._tags: Dict[str, Tuple[Any, Timestamp]] = {}
        # value -> Set of tags
        self._value_tags: Dict[Any, Set[str]] = {}
        # tombstones for observed removes
        self._tombstones: Set[str] = set()
    
    def _generate_tag(self, value: Any) -> str:
        """Generate unique tag for value"""
        return f"{self.node_id}:{uuid.uuid4().hex[:8]}"
    
    def add(
        self,
        value: Any,
        timestamp: Optional[Timestamp] = None,
        tag: Optional[str] = None,
    ) -> None:
        """
        Add an element to the set.
        
        Args:
            value: Element to add
            timestamp: Optional timestamp
            tag: Optional custom tag (auto-generated if not provided)
        """
        if timestamp is None:
            timestamp = Timestamp.now(self.node_id)
        
        tag = tag or self._generate_tag(value)
        
        self._tags[tag] = (value, timestamp)
        
        if value not in self._value_tags:
            self._value_tags[value] = set()
        self._value_tags[value].add(tag)
    
    def remove(
        self,
        value: Any,
        timestamp: Optional[Timestamp] = None,
    ) -> None:
        """
        Remove an element from the set.
        
        Only removes tags that were observed before this remove.
        """
        if timestamp is None:
            timestamp = Timestamp.now(self.node_id)
        
        if value not in self._value_tags:
            return
        
        # Remove all tags for this value and add to tombstones
        for tag in list(self._value_tags[value]):
            _, ts = self._tags.get(tag, (None, None))
            if ts and ts <= timestamp:
                self._tombstones.add(tag)
                del self._tags[tag]
        
        del self._value_tags[value]
    
    def get(self) -> Set[Any]:
        """Get all elements currently in the set"""
        result = set()
        
        for tag, (value, timestamp) in self._tags.items():
            if tag not in self._tombstones:
                result.add(value)
        
        return result
    
    def contains(self, value: Any) -> bool:
        """Check if value is in the set"""
        return value in self.get()
    
    def merge(self, other: "ORSet") -> None:
        """Merge with another OR-Set"""
        if not isinstance(other, ORSet):
            raise TypeError("Can only merge with ORSet")
        
        # Merge tags
        for tag, (value, timestamp) in other._tags.items():
            if tag not in self._tags:
                self._tags[tag] = (value, timestamp)
                
                if value not in self._value_tags:
                    self._value_tags[value] = set()
                self._value_tags[value].add(tag)
            else:
                existing_value, existing_ts = self._tags[tag]
                if timestamp > existing_ts:
                    self._tags[tag] = (value, timestamp)
        
        # Merge tombstones
        self._tombstones.update(other._tombstones)
        
        # Rebuild value_tags
        self._rebuild_value_tags()
    
    def _rebuild_value_tags(self) -> None:
        """Rebuild value_tags from tags"""
        self._value_tags.clear()
        for tag, (value, _) in self._tags.items():
            if value not in self._value_tags:
                self._value_tags[value] = set()
            self._value_tags[value].add(tag)
    
    def __repr__(self) -> str:
        return f"ORSet({self.get()})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "or_set",
            "node_id": self.node_id,
            "tags": {k: {"value": v[0], "timestamp": v[1].to_dict()} for k, v in self._tags.items()},
            "tombstones": list(self._tombstones),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ORSet":
        s = cls(node_id=data["node_id"])
        for tag, entry in data["tags"].items():
            s._tags[tag] = (entry["value"], Timestamp.from_dict(entry["timestamp"]))
        s._tombstones = set(data.get("tombstones", []))
        s._rebuild_value_tags()
        return s


class TwoPhaseSet(CRDTRegister):
    """
    Two-Phase Set (U-Set).
    
    A simpler set that supports only add and remove of known elements.
    Elements can only be removed if they were previously added.
    Once removed, elements can never be re-added.
    
    Phase 1 (Add): G-Set (Grow-only Set)
    Phase 2 (Remove): Remove only from current set
    
    Example:
    
    ```python
    s = TwoPhaseSet()
    s.add("item1")
    s.add("item2")
    s.remove("item1")
    print(s.get())  # {"item2"}
    ```
    """
    
    def __init__(self):
        self._added: Set[Any] = set()
        self._removed: Set[Any] = set()
    
    def add(self, value: Any) -> None:
        """Add an element (only if not removed)"""
        if value not in self._removed:
            self._added.add(value)
    
    def remove(self, value: Any) -> None:
        """Remove an element (only if added)"""
        if value in self._added:
            self._removed.add(value)
    
    def get(self) -> Set[Any]:
        """Get all elements"""
        return self._added - self._removed
    
    def merge(self, other: "TwoPhaseSet") -> None:
        """Merge with another Two-Phase Set"""
        if not isinstance(other, TwoPhaseSet):
            raise TypeError("Can only merge with TwoPhaseSet")
        
        self._added.update(other._added)
        self._removed.update(other._removed)
    
    def __repr__(self) -> str:
        return f"TwoPhaseSet(added={self._added}, removed={self._removed})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "two_phase_set",
            "added": list(self._added),
            "removed": list(self._removed),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TwoPhaseSet":
        s = cls()
        s._added = set(data["added"])
        s._removed = set(data["removed"])
        return s


class LWWMap(CRDTRegister):
    """
    Last-Writer-Wins Map.
    
    A map where each key-value pair is an LWW-Register.
    
    Example:
    
    ```python
    m = LWWMap()
    m.put("name", "Alice")
    m.put("age", 30)
    print(m.get("name"))  # "Alice"
    ```
    """
    
    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self._registers: Dict[str, LWWRegister] = {}
    
    def put(
        self,
        key: str,
        value: Any,
        timestamp: Optional[Timestamp] = None,
    ) -> None:
        """Put a key-value pair"""
        if key not in self._registers:
            self._registers[key] = LWWRegister(node_id=self.node_id)
        self._registers[key].set(value, timestamp)
    
    def remove(self, key: str, timestamp: Optional[Timestamp] = None) -> None:
        """Remove a key"""
        if key in self._registers:
            self._registers[key].delete(timestamp)
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        if key not in self._registers:
            return None
        return self._registers[key].get()
    
    def keys(self) -> Set[str]:
        """Get all keys"""
        return {k for k in self._registers if self.get(k) is not None}
    
    def items(self) -> Dict[str, Any]:
        """Get all key-value pairs"""
        return {k: self.get(k) for k in self.keys()}
    
    def merge(self, other: "LWWMap") -> None:
        """Merge with another LWW-Map"""
        if not isinstance(other, LWWMap):
            raise TypeError("Can only merge with LWWMap")
        
        for key, register in other._registers.items():
            if key not in self._registers:
                self._registers[key] = register
            else:
                self._registers[key].merge(register)
    
    def __repr__(self) -> str:
        return f"LWWMap({self.items()})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "lww_map",
            "node_id": self.node_id,
            "registers": {k: v.to_dict() for k, v in self._registers.items()},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LWWMap":
        m = cls(node_id=data["node_id"])
        for key, reg_data in data["registers"].items():
            m._registers[key] = LWWRegister.from_dict(reg_data)
        return m
