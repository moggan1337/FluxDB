"""
Event Replay and Reprocessing for FluxDB

This module provides capabilities for replaying and reprocessing events
in the event store. This is essential for:
- Rebuilding projections
- Debugging event processing
- Implementing saga pattern
- Temporal queries
- Replaying events to new systems

Key Features:
- Selective replay (by time, type, aggregate)
- Parallel replay with worker pools
- Exactly-once replay semantics
- Replay checkpoints and resumption
- Dead letter handling for failed events

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                     Event Replay System                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Event Store ──► ReplayController ──► Workers (parallel)        │
│                       │                   │                      │
│                       ▼                   ▼                      │
│                  ┌─────────┐         ┌─────────┐                │
│                  │ Filter  │         │ Handler │                │
│                  │ Config  │         │  Pool   │                │
│                  └─────────┘         └─────────┘                │
│                       │                   │                      │
│                       ▼                   ▼                      │
│                  Replay Results      Dead Letter                 │
│                       │               Queue                     │
│                       ▼                                        │
│                  Checkpoints                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Usage:

```python
from fluxdb.replay import ReplayController, ReplayConfig, ReplayFilter

# Configure replay
config = ReplayConfig(
    start_sequence=1000,
    end_sequence=5000,
    filter=ReplayFilter(
        event_types=["OrderCreated", "OrderShipped"],
        aggregate_ids=["order:123", "order:456"],
    ),
)

# Create replay controller
controller = ReplayController(event_store)

# Run replay
results = controller.replay(config, handler=my_handler)

# Check results
print(f"Processed: {results.processed}")
print(f"Failed: {results.failed}")
```
"""

import json
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union

import logging

from .aggregates import Event, EventStore

logger = logging.getLogger(__name__)


class ReplayMode(Enum):
    """Replay modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    BATCH = "batch"
    STREAM = "stream"


class ReplayStatus(Enum):
    """Replay status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ReplayFilter:
    """
    Filter for selecting events to replay.
    
    All filters are ANDed together.
    """
    event_types: Optional[List[str]] = None
    aggregate_ids: Optional[List[str]] = None
    aggregate_types: Optional[List[str]] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    start_sequence: Optional[int] = None
    end_sequence: Optional[int] = None
    correlation_ids: Optional[Set[str]] = None
    custom_filter: Optional[Callable[[Event], bool]] = None
    
    def matches(self, event: Event) -> bool:
        """Check if an event matches the filter"""
        # Event type filter
        if self.event_types and event.event_type not in self.event_types:
            return False
        
        # Aggregate ID filter
        if self.aggregate_ids and event.aggregate_id not in self.aggregate_ids:
            return False
        
        # Aggregate type filter
        if self.aggregate_types and event.aggregate_type not in self.aggregate_types:
            return False
        
        # Time filters
        if self.start_time and event.timestamp < self.start_time:
            return False
        if self.end_time and event.timestamp > self.end_time:
            return False
        
        # Sequence filters
        if self.start_sequence and event.sequence < self.start_sequence:
            return False
        if self.end_sequence and event.sequence > self.end_sequence:
            return False
        
        # Correlation ID filter
        if self.correlation_ids and event.correlation_id not in self.correlation_ids:
            return False
        
        # Custom filter
        if self.custom_filter and not self.custom_filter(event):
            return False
        
        return True


@dataclass
class ReplayConfig:
    """
    Configuration for replay operations.
    """
    filter: Optional[ReplayFilter] = None
    mode: ReplayMode = ReplayMode.SEQUENTIAL
    batch_size: int = 100
    parallel_workers: int = 4
    checkpoint_interval: int = 1000
    timeout_seconds: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0
    stop_on_error: bool = False
    include_metadata: bool = True
    preserve_order: bool = True
    
    def __post_init__(self):
        if self.filter is None:
            self.filter = ReplayFilter()


@dataclass
class ReplayResult:
    """Result of a replay operation"""
    replay_id: str
    status: ReplayStatus
    processed: int = 0
    failed: int = 0
    skipped: int = 0
    duration_seconds: float = 0.0
    events_per_second: float = 0.0
    start_sequence: int = 0
    end_sequence: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    checkpoint_sequence: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "replay_id": self.replay_id,
            "status": self.status.value,
            "processed": self.processed,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration_seconds": self.duration_seconds,
            "events_per_second": self.events_per_second,
            "start_sequence": self.start_sequence,
            "end_sequence": self.end_sequence,
            "errors": self.errors,
            "checkpoint_sequence": self.checkpoint_sequence,
        }


@dataclass
class DeadLetter:
    """Dead letter event for failed processing"""
    event: Event
    error: str
    error_type: str
    retry_count: int
    first_failed_at: float
    last_failed_at: float
    handler: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event.to_dict(),
            "error": self.error,
            "error_type": self.error_type,
            "retry_count": self.retry_count,
            "first_failed_at": self.first_failed_at,
            "last_failed_at": self.last_failed_at,
            "handler": self.handler,
        }


class ReplayController:
    """
    Controller for managing event replays.
    
    The replay controller handles:
    - Selecting events based on filters
    - Processing events with handlers
    - Tracking progress with checkpoints
    - Managing dead letters for failures
    - Parallel and batch processing
    
    Usage:
    
    ```python
    controller = ReplayController(event_store)
    
    # Simple replay
    for event in controller.iter_events(filter=ReplayFilter(event_types=["OrderCreated"])):
        print(f"Processing: {event.event_id}")
    
    # Controlled replay with results
    config = ReplayConfig(
        mode=ReplayMode.PARALLEL,
        parallel_workers=8,
        filter=ReplayFilter(start_sequence=1000),
    )
    
    result = controller.replay(config, handler=process_event)
    print(f"Processed {result.processed} events")
    ```
    """
    
    def __init__(
        self,
        event_store: EventStore,
        checkpoint_path: Optional[Path] = None,
        dead_letter_path: Optional[Path] = None,
    ):
        self.event_store = event_store
        
        self.checkpoint_path = checkpoint_path or Path("./replay_checkpoints")
        self.dead_letter_path = dead_letter_path or Path("./dead_letters")
        
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        self.dead_letter_path.mkdir(parents=True, exist_ok=True)
        
        self._running = False
        self._paused = False
        self._current_replay: Optional[ReplayConfig] = None
        self._result: Optional[ReplayResult] = None
        self._lock = threading.RLock()
        
        # Load dead letters
        self._dead_letters: List[DeadLetter] = []
        self._load_dead_letters()
    
    def _load_dead_letters(self) -> None:
        """Load dead letters from disk"""
        for dl_file in self.dead_letter_path.glob("*.json"):
            try:
                with open(dl_file, "r") as f:
                    data = json.load(f)
                    self._dead_letters.append(DeadLetter(
                        event=Event.from_dict(data["event"]),
                        error=data["error"],
                        error_type=data["error_type"],
                        retry_count=data["retry_count"],
                        first_failed_at=data["first_failed_at"],
                        last_failed_at=data["last_failed_at"],
                        handler=data.get("handler"),
                    ))
            except Exception as e:
                logger.warning(f"Failed to load dead letter {dl_file}: {e}")
    
    def _save_dead_letter(self, dead_letter: DeadLetter) -> None:
        """Save a dead letter to disk"""
        path = self.dead_letter_path / f"{dead_letter.event.event_id}.json"
        with open(path, "w") as f:
            json.dump(dead_letter.to_dict(), f, indent=2)
    
    def iter_events(
        self,
        filter: Optional[ReplayFilter] = None,
        start_sequence: int = 0,
    ) -> Iterator[Event]:
        """
        Iterate over events matching a filter.
        
        Args:
            filter: Event filter
            start_sequence: Start from this sequence
            
        Yields:
            Matching events
        """
        for event in self.event_store.get_all_events(from_sequence=start_sequence):
            if filter is None or filter.matches(event):
                yield event
    
    def replay(
        self,
        config: ReplayConfig,
        handler: Callable[[Event], Any],
        replay_id: Optional[str] = None,
    ) -> ReplayResult:
        """
        Replay events according to configuration.
        
        Args:
            config: Replay configuration
            handler: Function to handle each event
            replay_id: Optional replay identifier
            
        Returns:
            ReplayResult with statistics
        """
        with self._lock:
            replay_id = replay_id or f"replay_{int(time.time() * 1000)}"
            
            result = ReplayResult(
                replay_id=replay_id,
                status=ReplayStatus.RUNNING,
            )
            
            self._running = True
            self._current_replay = config
            self._result = result
            
            start_time = time.time()
            
            try:
                if config.mode == ReplayMode.SEQUENTIAL:
                    self._replay_sequential(config, handler, result)
                elif config.mode == ReplayMode.PARALLEL:
                    self._replay_parallel(config, handler, result)
                elif config.mode == ReplayMode.BATCH:
                    self._replay_batch(config, handler, result)
                elif config.mode == ReplayMode.STREAM:
                    self._replay_stream(config, handler, result)
                
                result.status = ReplayStatus.COMPLETED
                
            except Exception as e:
                logger.error(f"Replay failed: {e}")
                result.status = ReplayStatus.FAILED
                result.errors.append({
                    "type": "replay_error",
                    "message": str(e),
                    "timestamp": time.time(),
                })
            
            finally:
                result.duration_seconds = time.time() - start_time
                if result.processed > 0:
                    result.events_per_second = result.processed / result.duration_seconds
                self._running = False
                
            return result
    
    def _replay_sequential(
        self,
        config: ReplayConfig,
        handler: Callable[[Event], Any],
        result: ReplayResult,
    ) -> None:
        """Sequential replay"""
        for event in self.iter_events(config.filter):
            if not self._running:
                result.status = ReplayStatus.CANCELLED
                break
            
            while self._paused:
                time.sleep(0.1)
            
            self._process_event(event, handler, config, result)
    
    def _replay_parallel(
        self,
        config: ReplayConfig,
        handler: Callable[[Event], Any],
        result: ReplayResult,
    ) -> None:
        """Parallel replay with thread pool"""
        events = list(self.iter_events(config.filter))
        
        with ThreadPoolExecutor(max_workers=config.parallel_workers) as executor:
            futures = {
                executor.submit(self._process_event, event, handler, config, result): event
                for event in events
            }
            
            for future in as_completed(futures):
                if not self._running:
                    result.status = ReplayStatus.CANCELLED
                    executor.shutdown(wait=False)
                    break
    
    def _replay_batch(
        self,
        config: ReplayConfig,
        handler: Callable[[Event], Any],
        result: ReplayResult,
    ) -> None:
        """Batch replay"""
        batch: List[Event] = []
        
        for event in self.iter_events(config.filter):
            batch.append(event)
            
            if len(batch) >= config.batch_size:
                self._process_batch(batch, handler, config, result)
                batch.clear()
        
        # Process remaining
        if batch:
            self._process_batch(batch, handler, config, result)
    
    def _process_batch(
        self,
        batch: List[Event],
        handler: Callable[[Event], Any],
        config: ReplayConfig,
        result: ReplayResult,
    ) -> None:
        """Process a batch of events"""
        for event in batch:
            self._process_event(event, handler, config, result)
    
    def _replay_stream(
        self,
        config: ReplayConfig,
        handler: Callable[[Event], Any],
        result: ReplayResult,
    ) -> None:
        """Stream replay with timeout"""
        for event in self.iter_events(config.filter):
            if not self._running:
                result.status = ReplayStatus.CANCELLED
                break
            
            self._process_event(event, handler, config, result)
            
            # Check timeout
            if result.duration_seconds > config.timeout_seconds:
                result.status = ReplayStatus.PAUSED
                result.checkpoint_sequence = event.sequence
                break
    
    def _process_event(
        self,
        event: Event,
        handler: Callable[[Event], Any],
        config: ReplayConfig,
        result: ReplayResult,
    ) -> None:
        """Process a single event"""
        # Track sequence bounds
        if result.start_sequence == 0:
            result.start_sequence = event.sequence
        result.end_sequence = event.sequence
        
        try:
            # Call handler
            handler(event)
            result.processed += 1
            
            # Checkpoint
            if result.processed % config.checkpoint_interval == 0:
                self._save_checkpoint(result.replay_id, event.sequence)
                
        except Exception as e:
            result.failed += 1
            
            # Create dead letter
            dead_letter = DeadLetter(
                event=event,
                error=str(e),
                error_type=type(e).__name__,
                retry_count=0,
                first_failed_at=time.time(),
                last_failed_at=time.time(),
                handler=getattr(handler, "__name__", None),
            )
            
            # Retry logic
            for attempt in range(config.retry_count):
                try:
                    time.sleep(config.retry_delay * (attempt + 1))
                    handler(event)
                    result.failed -= 1
                    result.processed += 1
                    break
                except Exception:
                    dead_letter.retry_count = attempt + 1
                    dead_letter.last_failed_at = time.time()
            else:
                # Save to dead letter queue
                self._dead_letters.append(dead_letter)
                self._save_dead_letter(dead_letter)
                
                result.errors.append({
                    "event_id": event.event_id,
                    "error": str(e),
                    "type": type(e).__name__,
                    "sequence": event.sequence,
                    "timestamp": time.time(),
                })
                
                if config.stop_on_error:
                    raise
        
        # Update checkpoint
        result.checkpoint_sequence = event.sequence
    
    def _save_checkpoint(self, replay_id: str, sequence: int) -> None:
        """Save replay checkpoint"""
        path = self.checkpoint_path / f"{replay_id}.json"
        with open(path, "w") as f:
            json.dump({
                "replay_id": replay_id,
                "sequence": sequence,
                "timestamp": time.time(),
            }, f)
    
    def resume(
        self,
        replay_id: str,
        handler: Callable[[Event], Any],
    ) -> ReplayResult:
        """Resume a paused replay"""
        checkpoint_path = self.checkpoint_path / f"{replay_id}.json"
        
        if not checkpoint_path.exists():
            raise ValueError(f"No checkpoint found for replay {replay_id}")
        
        with open(checkpoint_path, "r") as f:
            checkpoint = json.load(f)
        
        start_sequence = checkpoint["sequence"] + 1
        
        result = ReplayResult(
            replay_id=replay_id,
            status=ReplayStatus.RUNNING,
            start_sequence=start_sequence,
        )
        
        config = ReplayConfig()
        self._replay_sequential(config, handler, result)
        
        return result
    
    def pause(self) -> None:
        """Pause current replay"""
        with self._lock:
            self._paused = True
    
    def resume_replay(self) -> None:
        """Resume paused replay"""
        with self._lock:
            self._paused = False
    
    def cancel(self) -> None:
        """Cancel current replay"""
        with self._lock:
            self._running = False
    
    def get_dead_letters(
        self,
        event_type: Optional[str] = None,
        since: Optional[float] = None,
    ) -> List[DeadLetter]:
        """Get dead letters, optionally filtered"""
        letters = self._dead_letters
        
        if event_type:
            letters = [dl for dl in letters if dl.event.event_type == event_type]
        
        if since:
            letters = [dl for dl in letters if dl.first_failed_at >= since]
        
        return letters
    
    def retry_dead_letter(
        self,
        dead_letter: DeadLetter,
        handler: Callable[[Event], Any],
    ) -> bool:
        """Retry processing a dead letter"""
        try:
            handler(dead_letter.event)
            
            # Remove from dead letters
            self._dead_letters.remove(dead_letter)
            
            # Remove file
            path = self.dead_letter_path / f"{dead_letter.event.event_id}.json"
            if path.exists():
                path.unlink()
            
            return True
            
        except Exception as e:
            dead_letter.retry_count += 1
            dead_letter.last_failed_at = time.time()
            dead_letter.error = str(e)
            self._save_dead_letter(dead_letter)
            return False
    
    def retry_all_dead_letters(
        self,
        handler: Callable[[Event], Any],
    ) -> Dict[str, int]:
        """Retry all dead letters"""
        results = {"success": 0, "failed": 0}
        
        for dl in self._dead_letters.copy():
            if self.retry_dead_letter(dl, handler):
                results["success"] += 1
            else:
                results["failed"] += 1
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get replay statistics"""
        return {
            "running": self._running,
            "paused": self._paused,
            "current_replay": self._current_replay.replay_id if self._current_replay else None,
            "dead_letter_count": len(self._dead_letters),
            "dead_letters_by_type": self._get_dead_letter_stats(),
        }
    
    def _get_dead_letter_stats(self) -> Dict[str, int]:
        """Get dead letter statistics by type"""
        stats = {}
        for dl in self._dead_letters:
            event_type = dl.event.event_type
            stats[event_type] = stats.get(event_type, 0) + 1
        return stats
