"""
Materialized Views and Projections for FluxDB

This module implements the projection pattern for event sourcing, where
read models (projections) are built from the event stream.

Key Features:
- Asynchronous projection updates
- Checkpoint-based recovery
- Parallel projection execution
- Projection versioning and migration
- Snapshots for fast projection rebuild

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                     Event Store                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Event 1 ─► Event 2 ─► Event 3 ─► Event 4 ─► Event 5 ...   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Projection Manager                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  UserProjection │  │OrderProjection  │  │AnalyticsProj    │  │
│  │  checkpoint: 50 │  │ checkpoint: 120 │  │ checkpoint: 30  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Read Models (Snapshots)                   ││
│  │  users.json  │  orders.json  │  analytics.json             ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

Usage:

```python
from fluxdb.projections import Projection, ProjectionManager

class UserProjection(Projection):
    name = "users"
    
    def init(self):
        self.users = {}  # user_id -> user_data
    
    def when(self, event):
        if event.event_type == "UserCreated":
            self.users[event.aggregate_id] = {
                "id": event.aggregate_id,
                "name": event.payload["name"],
                "email": event.payload["email"],
            }
        elif event.event_type == "UserUpdated":
            self.users[event.aggregate_id].update(event.payload)

manager = ProjectionManager(event_store)
manager.register(UserProjection())
manager.start()

# Query the projection
users = manager.get_projection("users").users
```
"""

import json
import threading
import time
import queue
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Type, Union

import logging

from .aggregates import Event, EventStore

logger = logging.getLogger(__name__)


class ProjectionState(Enum):
    """States of a projection"""
    INITIAL = "initial"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ProjectionCheckpoint:
    """Checkpoint for tracking projection progress"""
    projection_name: str
    sequence: int
    last_event_id: Optional[str] = None
    last_updated: float = field(default_factory=time.time)
    events_processed: int = 0
    errors: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "projection_name": self.projection_name,
            "sequence": self.sequence,
            "last_event_id": self.last_event_id,
            "last_updated": self.last_updated,
            "events_processed": self.events_processed,
            "errors": self.errors,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProjectionCheckpoint":
        return cls(**data)


class Projection(ABC):
    """
    Base class for projections.
    
    A projection is a way to build a read model from events.
    It maintains its own state and can be rebuilt from events.
    
    Subclasses should:
    1. Define `name` - unique projection name
    2. Implement `init()` - initialize projection state
    3. Implement `when()` - handle events
    
    Example:
    
    ```python
    class UserProjection(Projection):
        name = "users"
        
        def init(self):
            self.users = {}
            self.user_count = 0
        
        def when(self, event):
            if event.event_type == "UserCreated":
                self.users[event.aggregate_id] = event.payload
                self.user_count += 1
            elif event.event_type == "UserDeleted":
                del self.users[event.aggregate_id]
                self.user_count -= 1
    ```
    """
    
    # Subclasses should set these
    name: str = ""
    version: int = 1
    
    def __init__(self):
        self.state = ProjectionState.INITIAL
        self.checkpoint: Optional[ProjectionCheckpoint] = None
        self._state_lock = threading.RLock()
        self._event_count = 0
        self._error_count = 0
        self._last_event: Optional[Event] = None
        self._handlers: Dict[str, Callable] = {}
        
        # Auto-register @when methods
        self._register_handlers()
    
    def _register_handlers(self) -> None:
        """Auto-register @when decorated methods"""
        for attr_name in dir(self):
            if attr_name.startswith("when_"):
                event_type = attr_name[5:]  # Remove "when_" prefix
                handler = getattr(self, attr_name)
                if callable(handler):
                    self._handlers[event_type] = handler
    
    def init(self) -> None:
        """
        Initialize projection state.
        
        Called once when the projection is created or reset.
        """
        pass
    
    def when(self, event: Event) -> None:
        """
        Handle an event.
        
        Default implementation dispatches to registered handlers.
        Can be overridden for custom handling.
        """
        handler = self._handlers.get(event.event_type)
        if handler:
            handler(event)
    
    def get_state(self) -> Dict[str, Any]:
        """Get current projection state for serialization"""
        return {}
    
    def load_state(self, state: Dict[str, Any]) -> None:
        """Load projection state from serialized data"""
        pass
    
    def update_checkpoint(self, sequence: int, event: Optional[Event] = None) -> None:
        """Update the projection checkpoint"""
        self.checkpoint = ProjectionCheckpoint(
            projection_name=self.name,
            sequence=sequence,
            last_event_id=event.event_id if event else None,
            last_updated=time.time(),
            events_processed=self._event_count,
            errors=self._error_count,
        )
    
    def mark_processed(self, event: Event) -> None:
        """Mark an event as processed"""
        self._event_count += 1
        self._last_event = event
        self.update_checkpoint(event.sequence, event)
    
    def mark_error(self, error: Exception) -> None:
        """Mark that an error occurred"""
        self._error_count += 1
        logger.error(f"Projection {self.name} error: {error}")


class ReadModelProjection(Projection):
    """
    Projection that maintains a read model.
    
    Automatically persists the read model to disk.
    """
    
    def __init__(self, path: Optional[Path] = None):
        super().__init__()
        self.path = path
        self.read_model: Dict[str, Any] = {}
    
    def get_state(self) -> Dict[str, Any]:
        return {"read_model": self.read_model}
    
    def load_state(self, state: Dict[str, Any]) -> None:
        self.read_model = state.get("read_model", {})
    
    def save(self) -> None:
        """Save read model to disk"""
        if self.path:
            with open(self.path, "w") as f:
                json.dump(self.read_model, f, indent=2, default=str)
    
    def load(self) -> None:
        """Load read model from disk"""
        if self.path and Path(self.path).exists():
            with open(self.path, "r") as f:
                self.read_model = json.load(f)


class ProjectionManager:
    """
    Manages multiple projections and their execution.
    
    The manager:
    - Registers projections
    - Processes events through projections
    - Maintains checkpoints
    - Handles parallel execution
    """
    
    def __init__(
        self,
        event_store: EventStore,
        checkpoint_path: Optional[Path] = None,
    ):
        self.event_store = event_store
        self.checkpoint_path = checkpoint_path or Path("./checkpoints")
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        
        self._projections: Dict[str, Projection] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._event_queue: queue.Queue = queue.Queue()
        self._pending_checkpoint_save = False
        
        # Load existing checkpoints
        self._checkpoints: Dict[str, ProjectionCheckpoint] = {}
        self._load_checkpoints()
    
    def _load_checkpoints(self) -> None:
        """Load checkpoints from disk"""
        for checkpoint_file in self.checkpoint_path.glob("*.json"):
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                    checkpoint = ProjectionCheckpoint.from_dict(data)
                    self._checkpoints[checkpoint.projection_name] = checkpoint
            except Exception as e:
                logger.warning(f"Failed to load checkpoint {checkpoint_file}: {e}")
    
    def _save_checkpoint(self, checkpoint: ProjectionCheckpoint) -> None:
        """Save checkpoint to disk"""
        path = self.checkpoint_path / f"{checkpoint.projection_name}.json"
        with open(path, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)
    
    def register(self, projection: Projection) -> None:
        """
        Register a projection.
        
        Args:
            projection: Projection instance to register
        """
        with self._lock:
            if not projection.name:
                raise ValueError("Projection must have a name")
            
            if projection.name in self._projections:
                raise ValueError(f"Projection {projection.name} already registered")
            
            # Initialize projection
            projection.init()
            
            # Load checkpoint if exists
            if projection.name in self._checkpoints:
                projection.checkpoint = self._checkpoints[projection.name]
                logger.info(f"Resuming {projection.name} from sequence {projection.checkpoint.sequence}")
            
            self._projections[projection.name] = projection
            logger.info(f"Registered projection: {projection.name}")
    
    def unregister(self, name: str) -> None:
        """Unregister a projection"""
        with self._lock:
            if name in self._projections:
                del self._projections[name]
    
    def get_projection(self, name: str) -> Optional[Projection]:
        """Get a projection by name"""
        return self._projections.get(name)
    
    def list_projections(self) -> List[str]:
        """List all registered projection names"""
        return list(self._projections.keys())
    
    def start(self) -> None:
        """Start processing events through projections"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            
            for projection in self._projections.values():
                projection.state = ProjectionState.RUNNING
            
            self._thread = threading.Thread(target=self._process_loop, daemon=True)
            self._thread.start()
            
            logger.info("Projection manager started")
    
    def stop(self) -> None:
        """Stop processing events"""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            
            for projection in self._projections.values():
                projection.state = ProjectionState.STOPPED
            
            # Save all checkpoints
            for name, projection in self._projections.items():
                if projection.checkpoint:
                    self._save_checkpoint(projection.checkpoint)
            
            if self._thread:
                self._thread.join(timeout=5)
            
            logger.info("Projection manager stopped")
    
    def _process_loop(self) -> None:
        """Main processing loop"""
        while self._running:
            try:
                # Get events from store
                for event in self.event_store.get_all_events():
                    self._process_event(event)
                    
            except Exception as e:
                logger.error(f"Error in projection process loop: {e}")
            
            time.sleep(0.1)  # Polling interval
    
    def _process_event(self, event: Event) -> None:
        """Process a single event through all projections"""
        with self._lock:
            for name, projection in self._projections.items():
                # Check if we should process this event
                if projection.checkpoint and event.sequence <= projection.checkpoint.sequence:
                    continue
                
                try:
                    projection.when(event)
                    projection.mark_processed(event)
                    
                    # Save checkpoint periodically
                    if projection._event_count % 100 == 0:
                        self._save_checkpoint(projection.checkpoint)
                        
                except Exception as e:
                    projection.mark_error(e)
                    projection.state = ProjectionState.ERROR
    
    def process_event(self, event: Event) -> None:
        """
        Process a single event (for synchronous processing).
        
        Args:
            event: Event to process
        """
        self._process_event(event)
    
    def rebuild(self, projection_name: str) -> None:
        """
        Rebuild a projection from scratch.
        
        Args:
            projection_name: Name of projection to rebuild
        """
        with self._lock:
            projection = self._projections.get(projection_name)
            if not projection:
                raise ValueError(f"Projection {projection_name} not found")
            
            # Reset projection
            projection.init()
            projection._event_count = 0
            projection._error_count = 0
            
            # Process all events
            for event in self.event_store.get_all_events():
                try:
                    projection.when(event)
                    projection.mark_processed(event)
                except Exception as e:
                    projection.mark_error(e)
            
            logger.info(f"Rebuilt projection {projection_name}")
    
    def rebuild_all(self) -> None:
        """Rebuild all projections"""
        for name in self._projections:
            self.rebuild(name)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all projections"""
        stats = {}
        for name, projection in self._projections.items():
            stats[name] = {
                "state": projection.state.value,
                "version": projection.version,
                "events_processed": projection._event_count,
                "errors": projection._error_count,
                "checkpoint": projection.checkpoint.to_dict() if projection.checkpoint else None,
            }
        return stats


class AsyncProjectionManager(ProjectionManager):
    """
    Async projection manager with event queue.
    
    Events are queued and processed asynchronously for better throughput.
    """
    
    def __init__(
        self,
        event_store: EventStore,
        checkpoint_path: Optional[Path] = None,
        queue_size: int = 10000,
    ):
        super().__init__(event_store, checkpoint_path)
        self._queue_size = queue_size
        self._batch_size = 100
        self._batch_timeout = 1.0
    
    def _process_loop(self) -> None:
        """Async processing loop with batching"""
        batch: List[Event] = []
        last_batch_time = time.time()
        
        while self._running:
            try:
                # Collect events into batch
                try:
                    event = self._event_queue.get(timeout=0.1)
                    batch.append(event)
                except queue.Empty:
                    pass
                
                # Process batch if full or timeout
                should_process = (
                    len(batch) >= self._batch_size or
                    (batch and time.time() - last_batch_time > self._batch_timeout)
                )
                
                if should_process:
                    for event in batch:
                        self._process_event(event)
                    batch.clear()
                    last_batch_time = time.time()
                    
            except Exception as e:
                logger.error(f"Error in async projection loop: {e}")
    
    def enqueue(self, event: Event) -> None:
        """Add event to processing queue"""
        self._event_queue.put(event, block=False)
    
    def enqueue_many(self, events: List[Event]) -> None:
        """Add multiple events to processing queue"""
        for event in events:
            self.enqueue(event)


# Decorator for defining event handlers
def on(event_type: str):
    """Decorator for registering event handlers in projections"""
    def decorator(func: Callable) -> Callable:
        func._event_type = event_type
        return func
    return decorator
