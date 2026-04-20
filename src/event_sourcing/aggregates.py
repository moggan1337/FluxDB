"""
Event Sourcing Aggregates for FluxDB

This module implements the event sourcing pattern, where state is stored
as a sequence of events rather than current state. This provides:
- Complete audit trail
- Temporal queries
- Easy replay and reprocessing
- Eventual consistency support
- CQRS (Command Query Responsibility Segregation)

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    Event Sourcing Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Command ──► Aggregate ──► Event ──► Event Store                 │
│                              │                                   │
│                              ▼                                   │
│                         Projections ◄─── Events                  │
│                              │                                   │
│                              ▼                                   │
│                         Read Models                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Key Concepts:
- Aggregate: A cluster of objects that are treated as a unit
- Command: A request to change state
- Event: A record of something that happened
- Projection: A way to derive current state from events
- Command Handler: Processes commands and generates events
"""

import json
import time
import uuid
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union, TypeVar

import logging

logger = logging.getLogger(__name__)


T = TypeVar("T", bound="AggregateRoot")


class EventType(Enum):
    """Base event types"""
    DOMAIN_EVENT = "domain_event"
    COMMAND = "command"
    QUERY = "query"
    SYSTEM_EVENT = "system_event"


@dataclass
class Event:
    """
    Base class for all events in the system.
    
    Events are immutable records of something that happened.
    They are the source of truth in an event sourcing system.
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    aggregate_id: str = ""
    aggregate_type: str = ""
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
            "version": self.version,
            "metadata": self.metadata,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create event from dictionary"""
        return cls(**data)
    
    def to_bytes(self) -> bytes:
        """Serialize to bytes"""
        return json.dumps(self.to_dict(), default=str).encode("utf-8")
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "Event":
        """Deserialize from bytes"""
        return cls.from_dict(json.loads(data.decode("utf-8")))


@dataclass
class DomainEvent(Event):
    """Base class for domain events"""
    event_type: str = "domain_event"
    
    def to_domain_event(self) -> Dict[str, Any]:
        """Convert to domain event format"""
        return {
            "type": self.event_type,
            "data": self.to_dict(),
        }


class EventEnvelope:
    """
    Wraps an event with envelope information for routing and handling.
    """
    
    def __init__(self, event: Event, handler: Optional[str] = None):
        self.event = event
        self.handler = handler
        self.received_at = time.time()
        self.processed_at: Optional[float] = None
        self.error: Optional[str] = None
        self.retry_count = 0
    
    def mark_processed(self) -> None:
        """Mark the event as processed"""
        self.processed_at = time.time()
    
    def mark_failed(self, error: str) -> None:
        """Mark the event as failed"""
        self.error = error
        self.retry_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event.to_dict(),
            "handler": self.handler,
            "received_at": self.received_at,
            "processed_at": self.processed_at,
            "error": self.error,
            "retry_count": self.retry_count,
        }


class Command:
    """
    Base class for commands.
    
    Commands are requests to change state. They are processed by
    aggregates which generate events.
    """
    
    def __init__(
        self,
        command_type: str,
        aggregate_id: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.command_id = str(uuid.uuid4())
        self.command_type = command_type
        self.aggregate_id = aggregate_id
        self.payload = payload
        self.metadata = metadata or {}
        self.timestamp = time.time()


class CommandResult:
    """
    Result of command processing.
    """
    
    def __init__(
        self,
        success: bool,
        events: Optional[List[Event]] = None,
        error: Optional[str] = None,
        aggregate_version: int = 0,
    ):
        self.success = success
        self.events = events or []
        self.error = error
        self.aggregate_version = aggregate_version
    
    @classmethod
    def ok(cls, events: List[Event] = None, version: int = 0) -> "CommandResult":
        return cls(success=True, events=events, aggregate_version=version)
    
    @classmethod
    def fail(cls, error: str) -> "CommandResult":
        return cls(success=False, error=error)


class AggregateRoot(ABC):
    """
    Base class for aggregates in event sourcing.
    
    An aggregate is a cluster of related entities and value objects
    that are treated as a single unit for data changes.
    
    Aggregates:
    - Maintain invariants (business rules)
    - Generate events when state changes
    - Can be reconstructed from events
    - Are the consistency boundary
    
    Example:
    
    ```python
    class Account(AggregateRoot):
        def __init__(self, account_id: str):
            self.account_id = account_id
            self.balance = 0
            self._version = 0
            self._pending_events: List[Event] = []
        
        def deposit(self, amount: float) -> None:
            if amount <= 0:
                raise ValueError("Deposit must be positive")
            
            self.balance += amount
            self._version += 1
            
            self._pending_events.append(DomainEvent(
                event_type="MoneyDeposited",
                aggregate_id=self.account_id,
                aggregate_type="Account",
                version=self._version,
                metadata={"amount": amount, "new_balance": self.balance},
            ))
        
        def withdraw(self, amount: float) -> None:
            if amount <= 0:
                raise ValueError("Withdrawal must be positive")
            if amount > self.balance:
                raise ValueError("Insufficient funds")
            
            self.balance -= amount
            self._version += 1
            
            self._pending_events.append(DomainEvent(
                event_type="MoneyWithdrawn",
                aggregate_id=self.account_id,
                aggregate_type="Account",
                version=self._version,
                metadata={"amount": amount, "new_balance": self.balance},
            ))
        
        def pull_events(self) -> List[Event]:
            events = self._pending_events.copy()
            self._pending_events.clear()
            return events
        
        def apply(self, event: Event) -> None:
            if event.event_type == "MoneyDeposited":
                self.balance = event.metadata["new_balance"]
            elif event.event_type == "MoneyWithdrawn":
                self.balance = event.metadata["new_balance"]
            self._version = event.version
    ```
    """
    
    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.aggregate_type = self.__class__.__name__
        self._version = 0
        self._pending_events: List[Event] = []
        self._created_at = time.time()
        self._updated_at = time.time()
    
    @abstractmethod
    def apply(self, event: Event) -> None:
        """
        Apply an event to reconstruct state.
        
        This method is called when replaying events to reconstruct
        the aggregate's state.
        """
        pass
    
    def _add_event(self, event: Event) -> None:
        """Add an event to the pending events list"""
        self._version += 1
        event.version = self._version
        event.sequence = self._version
        event.aggregate_id = self.aggregate_id
        event.aggregate_type = self.aggregate_type
        self._pending_events.append(event)
        self._updated_at = time.time()
    
    def pull_events(self) -> List[Event]:
        """Get and clear pending events"""
        events = self._pending_events.copy()
        self._pending_events.clear()
        return events
    
    def peek_events(self) -> List[Event]:
        """Peek at pending events without clearing"""
        return self._pending_events.copy()
    
    def replay(self, events: List[Event]) -> None:
        """Replay events to reconstruct state"""
        for event in sorted(events, key=lambda e: e.sequence):
            self.apply(event)
    
    @property
    def version(self) -> int:
        """Get current version"""
        return self._version
    
    @property
    def is_new(self) -> bool:
        """Check if aggregate is newly created"""
        return self._version == 0
    
    def to_snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of current state"""
        return {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "version": self._version,
            "created_at": self._created_at,
            "updated_at": self._updated_at,
            "state": self._get_state(),
        }
    
    @abstractmethod
    def _get_state(self) -> Dict[str, Any]:
        """Get current state for snapshotting"""
        pass


class AggregateRepository(ABC):
    """
    Repository for managing aggregates.
    
    Provides methods for saving and loading aggregates using
    the event store.
    """
    
    @abstractmethod
    def save(self, aggregate: AggregateRoot) -> None:
        """Save an aggregate's pending events"""
        pass
    
    @abstractmethod
    def load(self, aggregate_id: str) -> Optional[AggregateRoot]:
        """Load an aggregate by ID"""
        pass
    
    @abstractmethod
    def exists(self, aggregate_id: str) -> bool:
        """Check if an aggregate exists"""
        pass


class EventStore:
    """
    Event store for persisting and retrieving events.
    
    The event store is the core of event sourcing. It provides:
    - Appending events
    - Reading events by aggregate ID
    - Reading all events
    - Snapshots for faster loading
    
    Usage:
    
    ```python
    store = EventStore("./events")
    
    # Append events
    store.append(aggregate_id, [event1, event2])
    
    # Read events for an aggregate
    events = store.get_events(aggregate_id)
    
    # Read all events
    for event in store.get_all_events():
        print(event)
    ```
    """
    
    def __init__(
        self,
        path: Union[str, Any],
        create_if_missing: bool = True,
    ):
        """
        Initialize the event store.
        
        Args:
            path: Directory to store events
            create_if_missing: Create directory if missing
        """
        from pathlib import Path
        from ..core.storage import AppendOnlyStore
        
        self.base_path = Path(path) if isinstance(path, str) else path
        self.create_if_missing = create_if_missing
        
        if create_if_missing:
            self.base_path.mkdir(parents=True, exist_ok=True)
        
        self._store = AppendOnlyStore(
            self.base_path / "events",
            create_if_missing=create_if_missing,
        )
        
        self._lock = threading.RLock()
        self._event_handlers: Dict[str, List[Callable]] = {}
        
        logger.info(f"EventStore initialized at {self.base_path}")
    
    def append(
        self,
        aggregate_id: str,
        events: List[Event],
        expected_version: Optional[int] = None,
    ) -> None:
        """
        Append events to the store.
        
        Args:
            aggregate_id: ID of the aggregate
            events: List of events to append
            expected_version: Expected version for optimistic concurrency
        """
        with self._lock:
            # Check version if provided
            if expected_version is not None:
                current_events = self.get_events(aggregate_id)
                if current_events:
                    current_version = max(e.version for e in current_events)
                    if current_version != expected_version:
                        raise ConcurrencyError(
                            f"Version mismatch: expected {expected_version}, "
                            f"current {current_version}"
                        )
            
            # Append each event
            for event in events:
                event.aggregate_id = aggregate_id
                self._store.put(
                    key=f"{aggregate_id}:{event.event_type}:{event.event_id}",
                    value=event.to_dict(),
                    headers={
                        "aggregate_id": aggregate_id,
                        "event_type": event.event_type,
                        "event_id": event.event_id,
                    },
                )
                
                # Dispatch to handlers
                self._dispatch(event)
    
    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
    ) -> List[Event]:
        """
        Get all events for an aggregate.
        
        Args:
            aggregate_id: ID of the aggregate
            from_version: Get events from this version onwards
            
        Returns:
            List of events, sorted by version
        """
        events = []
        
        for key in self._store.keys():
            if key.startswith(f"{aggregate_id}:"):
                record = self._store.get(key)
                if record:
                    event = Event.from_dict(record.value)
                    if event.version > from_version:
                        events.append(event)
        
        return sorted(events, key=lambda e: e.version)
    
    def get_all_events(
        self,
        from_sequence: int = 0,
        limit: Optional[int] = None,
    ) -> Iterator[Event]:
        """
        Get all events in the store.
        
        Args:
            from_sequence: Start from this sequence
            limit: Maximum events to return
            
        Yields:
            Events
        """
        count = 0
        for record in self._store.scan():
            if record.sequence > from_sequence:
                yield Event.from_dict(record.value)
                count += 1
                if limit and count >= limit:
                    break
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> None:
        """
        Subscribe to events of a specific type.
        
        Args:
            event_type: Type of event to subscribe to
            handler: Callback function
        """
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)
    
    def _dispatch(self, event: Event) -> None:
        """Dispatch event to registered handlers"""
        handlers = self._event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Handler error for {event.event_type}: {e}")
    
    def get_event_count(self, aggregate_id: Optional[str] = None) -> int:
        """Get count of events"""
        if aggregate_id:
            return len(self.get_events(aggregate_id))
        return len(self._store)
    
    def close(self) -> None:
        """Close the event store"""
        self._store.close()


class ConcurrencyError(Exception):
    """Raised when there's a concurrency conflict"""
    pass


class AggregateFactory:
    """
    Factory for creating aggregates from events.
    """
    
    _registry: Dict[str, Type[AggregateRoot]] = {}
    
    @classmethod
    def register(cls, aggregate_type: str, factory: Type[AggregateRoot]) -> None:
        """Register an aggregate factory"""
        cls._registry[aggregate_type] = factory
    
    @classmethod
    def create(cls, aggregate_type: str, aggregate_id: str) -> AggregateRoot:
        """Create a new aggregate instance"""
        if aggregate_type not in cls._registry:
            raise ValueError(f"Unknown aggregate type: {aggregate_type}")
        return cls._registry[aggregate_type](aggregate_id)
    
    @classmethod
    def reconstruct(
        cls,
        aggregate_type: str,
        aggregate_id: str,
        events: List[Event],
    ) -> AggregateRoot:
        """Reconstruct an aggregate from events"""
        aggregate = cls.create(aggregate_type, aggregate_id)
        aggregate.replay(events)
        return aggregate


# Decorator for registering aggregates
def register_aggregate(aggregate_type: str):
    """Decorator for registering aggregate factories"""
    def decorator(cls: Type[AggregateRoot]) -> Type[AggregateRoot]:
        AggregateFactory.register(aggregate_type, cls)
        return cls
    return decorator
