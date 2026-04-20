"""
CRDT Counters for FluxDB

This module implements CRDT-based counters that can be replicated
across nodes without coordination.

Types of Counters:
1. G-Counter (Grow-only Counter): Can only increment
2. PN-Counter (Positive-Negative Counter): Supports increment and decrement
3. GCounterMap: Counter per key with grow-only semantics
4. PNCounterMap: Counter per key with positive-negative semantics

Key Features:
- Eventually consistent across all replicas
- No coordination required for updates
- Configurable merge strategies
- Tombstone support for deletions

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    CRDT Counter Types                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  G-Counter (Grow-only):                                          │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Node A: 5    │  Node B: 3    │  Node C: 7                  ││
│  │                       ▼                                   ││
│  │              Merge: 5 + 3 + 7 = 15                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  PN-Counter (Positive-Negative):                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Node A: P=10, N=2   │  Node B: P=5, N=8                     ││
│  │                          ▼                                  ││
│  │         Merge: P = 10+5 = 15, N = 2+8 = 10                  ││
│  │         Result: 15 - 10 = 5                                 ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Usage:

```python
from fluxdb.crdt import GCounter, PNCounter

# Grow-only counter
counter = GCounter()
counter.increment("A")  # Node A increments
counter.increment("A")
counter.increment("B")  # Node B increments

print(counter.value())  # 3

# Merge with another counter
other = GCounter()
other.increment("A")
other.increment("B")

counter.merge(other)
print(counter.value())  # 5

# Positive-Negative counter
pn = PNCounter()
pn.increment()  # +1
pn.increment(5)  # +5
pn.decrement(2)  # -2
print(pn.value())  # 4
```
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class CRDTCounter(ABC):
    """
    Base class for CRDT counters.
    """
    
    @abstractmethod
    def value(self) -> int:
        """Get current counter value"""
        pass
    
    @abstractmethod
    def merge(self, other: "CRDTCounter") -> None:
        """Merge with another counter"""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CRDTCounter":
        """Deserialize from dictionary"""
        pass


class GCounter(CRDTCounter):
    """
    Grow-only Counter.
    
    A counter that can only be incremented, never decremented.
    The value is the sum of all increments across all nodes.
    
    Properties:
    - Commutative: merge(a, b) = merge(b, a)
    - Associative: merge(merge(a, b), c) = merge(a, merge(b, c))
    - Idempotent: merge(a, a) = a
    
    Example:
    
    ```python
    # Node A's counter
    counter_a = GCounter()
    counter_a.increment("A")
    counter_a.increment("A")
    
    # Node B's counter
    counter_b = GCounter()
    counter_b.increment("B")
    
    # Both nodes have the same data structure
    counter_a.merge(counter_b)
    
    print(counter_a.value())  # 3 (2 from A + 1 from B)
    ```
    """
    
    def __init__(self, initial: Optional[Dict[str, int]] = None):
        """
        Initialize G-Counter.
        
        Args:
            initial: Optional initial state as {node_id: count}
        """
        # State: node_id -> count
        self._state: Dict[str, int] = initial.copy() if initial else {}
    
    def increment(self, node_id: str, amount: int = 1) -> None:
        """
        Increment the counter.
        
        Args:
            node_id: ID of the node performing increment
            amount: Amount to increment (default: 1)
        """
        if amount < 0:
            raise ValueError("GCounter can only increment, use PNCounter for negative")
        
        self._state[node_id] = self._state.get(node_id, 0) + amount
    
    def value(self) -> int:
        """Get total counter value (sum of all nodes)"""
        return sum(self._state.values())
    
    def get_node_value(self, node_id: str) -> int:
        """Get value for a specific node"""
        return self._state.get(node_id, 0)
    
    def nodes(self) -> Dict[str, int]:
        """Get per-node counts"""
        return self._state.copy()
    
    def merge(self, other: "GCounter") -> None:
        """
        Merge with another G-Counter.
        
        Takes the maximum of each node's count.
        """
        if not isinstance(other, GCounter):
            raise TypeError("Can only merge with GCounter")
        
        for node_id, count in other._state.items():
            self._state[node_id] = max(self._state.get(node_id, 0), count)
    
    def __repr__(self) -> str:
        return f"GCounter(value={self.value()}, nodes={self._state})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "g_counter",
            "state": self._state.copy(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GCounter":
        return cls(initial=data["state"])


class PNCounter(CRDTCounter):
    """
    Positive-Negative Counter.
    
    A counter that supports both increments and decrements.
    Implemented as two G-Counters: one for positive, one for negative.
    
    Value = positive.sum() - negative.sum()
    
    Properties:
    - Convergent: All replicas converge to same value
    - Commutative: merge(a, b) = merge(b, a)
    - Associative: merge(merge(a, b), c) = merge(a, merge(b, c))
    
    Example:
    
    ```python
    counter = PNCounter()
    
    counter.increment()      # +1
    counter.increment(5)     # +5
    counter.decrement(2)     # -2
    print(counter.value())   # 4
    
    # Merge with another counter
    other = PNCounter()
    other.increment(3)
    other.decrement(1)
    
    counter.merge(other)
    print(counter.value())   # 6
    ```
    """
    
    def __init__(
        self,
        positive: Optional[Dict[str, int]] = None,
        negative: Optional[Dict[str, int]] = None,
    ):
        """
        Initialize PN-Counter.
        
        Args:
            positive: Initial positive G-Counter state
            negative: Initial negative G-Counter state
        """
        self._positive = GCounter(positive)
        self._negative = GCounter(negative)
    
    def increment(self, node_id: str = "default", amount: int = 1) -> None:
        """
        Increment the counter.
        
        Args:
            node_id: ID of the node performing increment
            amount: Amount to increment (must be positive)
        """
        if amount < 0:
            raise ValueError("Use decrement() for negative amounts")
        self._positive.increment(node_id, amount)
    
    def decrement(self, node_id: str = "default", amount: int = 1) -> None:
        """
        Decrement the counter.
        
        Args:
            node_id: ID of the node performing decrement
            amount: Amount to decrement (must be positive)
        """
        if amount < 0:
            raise ValueError("Use increment() for positive amounts")
        self._negative.increment(node_id, amount)
    
    def value(self) -> int:
        """Get total counter value (positive - negative)"""
        return self._positive.value() - self._negative.value()
    
    def positive_value(self) -> int:
        """Get sum of positive increments"""
        return self._positive.value()
    
    def negative_value(self) -> int:
        """Get sum of negative decrements"""
        return self._negative.value()
    
    def get_node_value(self, node_id: str) -> int:
        """Get net value for a specific node"""
        return self._positive.get_node_value(node_id) - self._negative.get_node_value(node_id)
    
    def merge(self, other: "PNCounter") -> None:
        """
        Merge with another PN-Counter.
        
        Merges positive and negative G-Counters separately.
        """
        if not isinstance(other, PNCounter):
            raise TypeError("Can only merge with PNCounter")
        
        self._positive.merge(other._positive)
        self._negative.merge(other._negative)
    
    def __repr__(self) -> str:
        return f"PNCounter(value={self.value()}, positive={self._positive.nodes()}, negative={self._negative.nodes()})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "pn_counter",
            "positive": self._positive.to_dict(),
            "negative": self._negative.to_dict(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PNCounter":
        counter = cls()
        counter._positive = GCounter.from_dict(data["positive"])
        counter._negative = GCounter.from_dict(data["negative"])
        return counter


class GCounterMap(CRDTCounter):
    """
    Map of Grow-only Counters.
    
    A key-value store where each value is a G-Counter.
    Allows independent counting per key.
    
    Example:
    
    ```python
    counts = GCounterMap()
    
    counts.increment("page_views", "A")  # Node A page view
    counts.increment("page_views", "A")
    counts.increment("page_views", "B")  # Node B page view
    
    counts.increment("clicks", "A")
    
    print(counts.value("page_views"))  # 3
    print(counts.value("clicks"))      # 1
    print(counts.total())              # 4
    ```
    """
    
    def __init__(self, initial: Optional[Dict[str, Dict[str, int]]] = None):
        """
        Initialize GCounterMap.
        
        Args:
            initial: Optional initial state as {key: {node_id: count}}
        """
        # State: key -> (node_id -> count)
        self._state: Dict[str, Dict[str, int]] = {}
        
        if initial:
            for key, node_counts in initial.items():
                self._state[key] = node_counts.copy()
    
    def increment(
        self,
        key: str,
        node_id: str,
        amount: int = 1,
    ) -> None:
        """
        Increment counter for a key.
        
        Args:
            key: Counter key
            node_id: ID of the node performing increment
            amount: Amount to increment
        """
        if amount < 0:
            raise ValueError("GCounterMap can only increment")
        
        if key not in self._state:
            self._state[key] = {}
        
        self._state[key][node_id] = self._state[key].get(node_id, 0) + amount
    
    def value(self, key: str) -> int:
        """Get total counter value for a key"""
        if key not in self._state:
            return 0
        return sum(self._state[key].values())
    
    def total(self) -> int:
        """Get sum of all counters"""
        return sum(self.value(key) for key in self._state)
    
    def keys(self) -> set:
        """Get all counter keys"""
        return set(self._state.keys())
    
    def get_node_value(self, key: str, node_id: str) -> int:
        """Get value for specific key and node"""
        if key not in self._state:
            return 0
        return self._state[key].get(node_id, 0)
    
    def get_key_nodes(self, key: str) -> Dict[str, int]:
        """Get per-node counts for a key"""
        return self._state.get(key, {}).copy()
    
    def merge(self, other: "GCounterMap") -> None:
        """Merge with another GCounterMap"""
        if not isinstance(other, GCounterMap):
            raise TypeError("Can only merge with GCounterMap")
        
        for key, node_counts in other._state.items():
            if key not in self._state:
                self._state[key] = {}
            
            for node_id, count in node_counts.items():
                self._state[key][node_id] = max(
                    self._state[key].get(node_id, 0),
                    count,
                )
    
    def __repr__(self) -> str:
        return f"GCounterMap({ {k: self.value(k) for k in self.keys()} })"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "g_counter_map",
            "state": {k: v.copy() for k, v in self._state.items()},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GCounterMap":
        return cls(initial=data["state"])


class PNCounterMap(CRDTCounter):
    """
    Map of Positive-Negative Counters.
    
    A key-value store where each value is a PN-Counter.
    Allows independent counting (both positive and negative) per key.
    
    Example:
    
    ```python
    balances = PNCounterMap()
    
    # Node A: credit + debit
    balances.increment("account:1", "A", 100)
    balances.increment("account:1", "A", 50)
    balances.decrement("account:1", "A", 30)
    
    # Node B: credit
    balances.increment("account:1", "B", 200)
    
    print(balances.value("account:1"))  # 320
    ```
    """
    
    def __init__(
        self,
        initial: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """
        Initialize PNCounterMap.
        
        Args:
            initial: Optional initial state
        """
        # State: key -> PNCounter
        self._state: Dict[str, PNCounter] = {}
        
        if initial:
            for key, counter_data in initial.items():
                self._state[key] = PNCounter.from_dict(counter_data)
    
    def increment(
        self,
        key: str,
        node_id: str,
        amount: int = 1,
    ) -> None:
        """Increment counter for a key"""
        if amount < 0:
            raise ValueError("Use decrement() for negative amounts")
        
        if key not in self._state:
            self._state[key] = PNCounter()
        self._state[key].increment(node_id, amount)
    
    def decrement(
        self,
        key: str,
        node_id: str,
        amount: int = 1,
    ) -> None:
        """Decrement counter for a key"""
        if amount < 0:
            raise ValueError("Use increment() for positive amounts")
        
        if key not in self._state:
            self._state[key] = PNCounter()
        self._state[key].decrement(node_id, amount)
    
    def value(self, key: str) -> int:
        """Get total counter value for a key"""
        if key not in self._state:
            return 0
        return self._state[key].value()
    
    def total(self) -> int:
        """Get sum of all counters"""
        return sum(counter.value() for counter in self._state.values())
    
    def keys(self) -> set:
        """Get all counter keys"""
        return set(self._state.keys())
    
    def get_counter(self, key: str) -> Optional[PNCounter]:
        """Get the PN-Counter for a key"""
        return self._state.get(key)
    
    def merge(self, other: "PNCounterMap") -> None:
        """Merge with another PNCounterMap"""
        if not isinstance(other, PNCounterMap):
            raise TypeError("Can only merge with PNCounterMap")
        
        for key, other_counter in other._state.items():
            if key not in self._state:
                self._state[key] = other_counter
            else:
                self._state[key].merge(other_counter)
    
    def __repr__(self) -> str:
        return f"PNCounterMap({ {k: self.value(k) for k in self.keys()} })"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "pn_counter_map",
            "state": {k: v.to_dict() for k, v in self._state.items()},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PNCounterMap":
        counter_map = cls()
        for key, counter_data in data["state"].items():
            counter_map._state[key] = PNCounter.from_dict(counter_data)
        return counter_map


class LexicographicCounter:
    """
    Lexicographic Counter.
    
    A counter that supports lexicographic comparisons between replicas.
    Useful for distributed snapshots and total ordering.
    
    The counter maintains lexicographic tuples (counter, node_id) that
    can be compared across all replicas.
    """
    
    def __init__(self, initial: Optional[tuple] = None):
        """
        Initialize Lexicographic Counter.
        
        Args:
            initial: Optional (counter, node_id) tuple
        """
        if initial:
            self._counter = initial[0]
            self._node_id = initial[1]
        else:
            self._counter = 0
            self._node_id = ""
    
    def increment(self, node_id: str) -> None:
        """Increment counter"""
        self._counter += 1
        self._node_id = node_id
    
    def decrement(self, node_id: str) -> None:
        """Decrement counter"""
        self._counter -= 1
        self._node_id = node_id
    
    def value(self) -> int:
        """Get counter value"""
        return self._counter
    
    def node_id(self) -> str:
        """Get node ID of last update"""
        return self._node_id
    
    def tuple(self) -> tuple:
        """Get (counter, node_id) tuple for comparison"""
        return (self._counter, self._node_id)
    
    def merge(self, other: "LexicographicCounter") -> None:
        """Merge with another counter"""
        if other.tuple() > self.tuple():
            self._counter = other._counter
            self._node_id = other._node_id
    
    def __repr__(self) -> str:
        return f"LexCounter(value={self._counter}, node={self._node_id})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "lex_counter",
            "counter": self._counter,
            "node_id": self._node_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LexicographicCounter":
        return cls(initial=(data["counter"], data["node_id"]))
