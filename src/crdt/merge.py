"""
CRDT Merge Operations for FluxDB

This module provides utilities for merging CRDT data structures
and handling complex merge scenarios.

Key Features:
- Generic merge operations
- Conflict resolution strategies
- Merge diagnostics and debugging
- Automatic type detection and dispatch
- Composite CRDT support

Usage:

```python
from fluxdb.crdt.merge import merge, merge_all, ConflictResolver

# Simple merge
result = merge(reg1, reg2)

# Merge with conflict resolution
result = merge(
    map1, 
    map2, 
    resolver=ConflictResolver.last_writer_wins
)

# Merge multiple CRDTs
result = merge_all([counter1, counter2, counter3])

# Custom merge logic
result = merge(
    reg1, 
    reg2, 
    resolver=lambda a, b: custom_resolve(a, b)
)
```
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union, TypeVar
import logging

from .registers import (
    CRDTRegister, LWWRegister, MVRegister, ORSet, 
    TwoPhaseSet, LWWMap, RegisterEntry, Timestamp
)
from .counters import (
    CRDTCounter, GCounter, PNCounter, GCounterMap, PNCounterMap
)

logger = logging.getLogger(__name__)


T = TypeVar("T", bound=CRDTRegister)


class ConflictResolver(ABC):
    """
    Base class for conflict resolution strategies.
    """
    
    @abstractmethod
    def resolve(self, values: List[Any], metadata: List[Any]) -> Any:
        """Resolve conflict between values"""
        pass


class LastWriterWins(ConflictResolver):
    """Last-writer-wins conflict resolution"""
    
    def resolve(self, values: List[Any], metadata: List[Any]) -> Any:
        """Pick the value with the latest timestamp"""
        if not values:
            return None
        
        if len(values) == 1:
            return values[0]
        
        # Find latest timestamp
        latest_idx = 0
        latest_ts = None
        
        for i, meta in enumerate(metadata):
            if isinstance(meta, Timestamp):
                if latest_ts is None or meta > latest_ts:
                    latest_ts = meta
                    latest_idx = i
        
        return values[latest_idx]


class FirstWriterWins(ConflictResolver):
    """First-writer-wins conflict resolution"""
    
    def resolve(self, values: List[Any], metadata: List[Any]) -> Any:
        """Pick the value with the earliest timestamp"""
        if not values:
            return None
        
        if len(values) == 1:
            return values[0]
        
        # Find earliest timestamp
        earliest_idx = 0
        earliest_ts = None
        
        for i, meta in enumerate(metadata):
            if isinstance(meta, Timestamp):
                if earliest_ts is None or meta < earliest_ts:
                    earliest_ts = meta
                    earliest_idx = i
        
        return values[earliest_idx]


class MergeAll(ConflictResolver):
    """Merge all values (for multi-value registers)"""
    
    def resolve(self, values: List[Any], metadata: List[Any]) -> Any:
        """Return all values"""
        return set(values)


class CustomResolver(ConflictResolver):
    """Custom conflict resolution function"""
    
    def __init__(self, func: Callable[[List[Any]], Any]):
        self.func = func
    
    def resolve(self, values: List[Any], metadata: List[Any]) -> Any:
        return self.func(values)


# Factory for built-in resolvers
def create_resolver(strategy: str) -> ConflictResolver:
    """Create a conflict resolver by name"""
    resolvers = {
        "last_writer_wins": LastWriterWins,
        "first_writer_wins": FirstWriterWins,
        "merge_all": MergeAll,
    }
    
    if strategy not in resolvers:
        raise ValueError(f"Unknown resolver: {strategy}")
    
    return resolvers[strategy]()


def merge(
    crdt1: CRDTRegister,
    crdt2: CRDTRegister,
    resolver: Optional[ConflictResolver] = None,
) -> CRDTRegister:
    """
    Merge two CRDTs of the same type.
    
    Args:
        crdt1: First CRDT
        crdt2: Second CRDT
        resolver: Optional conflict resolver for value conflicts
        
    Returns:
        Merged CRDT (modifies crdt1 in place and returns it)
    """
    # Get the more specific type
    if type(crdt1) != type(crdt2):
        raise TypeError(f"Cannot merge {type(crdt1)} with {type(crdt2)}")
    
    # Use built-in merge
    crdt1.merge(crdt2)
    return crdt1


def merge_all(
    crdts: List[CRDTRegister],
    resolver: Optional[ConflictResolver] = None,
) -> CRDTRegister:
    """
    Merge multiple CRDTs of the same type.
    
    Args:
        crdts: List of CRDTs to merge
        resolver: Optional conflict resolver
        
    Returns:
        Merged CRDT
    """
    if not crdts:
        raise ValueError("Cannot merge empty list")
    
    if len(crdts) == 1:
        return crdts[0]
    
    # Start with first CRDT and merge rest
    result = crdts[0]
    for crdt in crdts[1:]:
        result.merge(crdt)
    
    return result


def merge_map(
    map1: LWWMap,
    map2: LWWMap,
    resolver: Optional[ConflictResolver] = None,
) -> LWWMap:
    """
    Merge two LWW-Maps with custom value resolution.
    
    When keys conflict, uses the resolver to determine the final value.
    """
    # Start with standard merge
    map1.merge(map2)
    
    # If resolver provided, check for conflicts and resolve
    if resolver is not None:
        for key in set(map1.keys()) & set(map2.keys()):
            entry1 = map1._registers[key]._entry
            entry2 = map2._registers.get(key)
            
            if entry2:
                entry2 = entry2._entry
                
                if entry1 and entry2:
                    # Both have values, check for conflict
                    if entry1.value != entry2.value:
                        values = [entry1.value, entry2.value]
                        metadata = [entry1.timestamp, entry2.timestamp]
                        resolved = resolver.resolve(values, metadata)
                        
                        # Update with resolved value
                        if resolved != entry1.value:
                            map1.put(key, resolved)
    
    return map1


def merge_gcounter_map(
    map1: GCounterMap,
    map2: GCounterMap,
) -> GCounterMap:
    """Merge two GCounterMaps"""
    map1.merge(map2)
    return map1


def merge_pncounter_map(
    map1: PNCounterMap,
    map2: PNCounterMap,
) -> PNCounterMap:
    """Merge two PNCounterMaps"""
    map1.merge(map2)
    return map1


class CRDTOperation:
    """Represents a CRDT operation for replication"""
    
    def __init__(
        self,
        operation_type: str,
        crdt_type: str,
        key: str,
        value: Any,
        timestamp: Optional[Timestamp] = None,
        node_id: Optional[str] = None,
    ):
        self.operation_type = operation_type
        self.crdt_type = crdt_type
        self.key = key
        self.value = value
        self.timestamp = timestamp or Timestamp.now(node_id or "unknown")
        self.node_id = node_id or self.timestamp.node_id
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation_type": self.operation_type,
            "crdt_type": self.crdt_type,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.to_dict(),
            "node_id": self.node_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CRDTOperation":
        return cls(
            operation_type=data["operation_type"],
            crdt_type=data["crdt_type"],
            key=data["key"],
            value=data["value"],
            timestamp=Timestamp.from_dict(data["timestamp"]),
            node_id=data["node_id"],
        )


class OperationLog:
    """
    Log of CRDT operations for debugging and replay.
    """
    
    def __init__(self):
        self._operations: List[CRDTOperation] = []
    
    def add(self, operation: CRDTOperation) -> None:
        """Add an operation to the log"""
        self._operations.append(operation)
    
    def get_operations(
        self,
        since: Optional[int] = None,
        until: Optional[int] = None,
    ) -> List[CRDTOperation]:
        """Get operations in a range"""
        start = since or 0
        end = until or len(self._operations)
        return self._operations[start:end]
    
    def replay(
        self,
        crdt: CRDTRegister,
        from_index: int = 0,
    ) -> CRDTRegister:
        """Replay operations onto a CRDT"""
        for op in self._operations[from_index:]:
            if op.crdt_type == "lww_register":
                crdt.set(op.value, op.timestamp)
            elif op.crdt_type == "or_set":
                if op.operation_type == "add":
                    crdt.add(op.value, op.timestamp)
                elif op.operation_type == "remove":
                    crdt.remove(op.value, op.timestamp)
            elif op.crdt_type == "g_counter":
                if op.operation_type == "increment":
                    crdt.increment(op.node_id, op.value)
            elif op.crdt_type == "pn_counter":
                if op.operation_type == "increment":
                    crdt.increment(op.node_id, op.value)
                elif op.operation_type == "decrement":
                    crdt.decrement(op.node_id, op.value)
        
        return crdt
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "operations": [op.to_dict() for op in self._operations],
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OperationLog":
        log = cls()
        for op_data in data["operations"]:
            log.add(CRDTOperation.from_dict(op_data))
        return log


class MergeResult:
    """Result of a merge operation with diagnostics"""
    
    def __init__(
        self,
        success: bool,
        merged: Optional[CRDTRegister] = None,
        conflicts: Optional[List[Dict[str, Any]]] = None,
        resolved: Optional[List[Any]] = None,
        errors: Optional[List[str]] = None,
    ):
        self.success = success
        self.merged = merged
        self.conflicts = conflicts or []
        self.resolved = resolved or []
        self.errors = errors or []
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "conflicts": self.conflicts,
            "resolved": self.resolved,
            "errors": self.errors,
            "merged_type": type(self.merged).__name__ if self.merged else None,
        }


def diagnose_merge(
    crdt1: CRDTRegister,
    crdt2: CRDTRegister,
) -> Dict[str, Any]:
    """
    Diagnose potential issues before merging.
    
    Returns information about:
    - Type compatibility
    - Potential conflicts
    - Timestamp ordering
    """
    result = {
        "compatible": type(crdt1) == type(crdt2),
        "type1": type(crdt1).__name__,
        "type2": type(crdt2).__name__,
        "potential_conflicts": [],
    }
    
    if result["compatible"]:
        # Check for specific conflict types
        if isinstance(crdt1, LWWRegister):
            e1 = crdt1._entry
            e2 = crdt2._entry
            
            if e1 and e2 and e1.value != e2.value:
                result["potential_conflicts"].append({
                    "type": "value_mismatch",
                    "value1": e1.value,
                    "value2": e2.value,
                    "timestamp1": e1.timestamp,
                    "timestamp2": e2.timestamp,
                })
        
        elif isinstance(crdt1, GCounter):
            for node in set(crdt1.nodes()) & set(crdt2.nodes()):
                if crdt1.get_node_value(node) != crdt2.get_node_value(node):
                    result["potential_conflicts"].append({
                        "type": "counter_mismatch",
                        "node": node,
                        "value1": crdt1.get_node_value(node),
                        "value2": crdt2.get_node_value(node),
                    })
    
    return result


def auto_merge(
    crdt1: CRDTRegister,
    crdt2: CRDTRegister,
    strategy: str = "auto",
) -> MergeResult:
    """
    Automatically merge CRDTs with diagnostics.
    
    Args:
        crdt1: First CRDT
        crdt2: Second CRDT
        strategy: Merge strategy ("auto", "lww", "mv", "union")
        
    Returns:
        MergeResult with merged value and diagnostics
    """
    result = MergeResult(success=False)
    
    try:
        # Check compatibility
        if type(crdt1) != type(crdt2):
            result.errors.append(f"Type mismatch: {type(crdt1)} vs {type(crdt2)}")
            return result
        
        # Perform merge based on type
        merged = merge(crdt1, crdt2)
        result.success = True
        result.merged = merged
        
    except Exception as e:
        result.errors.append(str(e))
        logger.error(f"Merge failed: {e}")
    
    return result
