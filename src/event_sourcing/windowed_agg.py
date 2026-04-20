"""
Windowed Aggregations for FluxDB

This module implements windowed aggregation capabilities for stream processing.
It supports various window types (tumbling, sliding, session) and aggregation
functions (sum, count, average, min, max, etc.).

Key Features:
- Tumbling windows (fixed size, non-overlapping)
- Sliding windows (overlapping, with configurable hop)
- Session windows (activity-based)
- Global windows (single window for all data)
- Watermarks for handling late arrivals
- Multiple aggregation functions

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                   Windowed Aggregation Pipeline                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Events ──► Window Assigner ──► Window ◄── Late Events            │
│                              │                                   │
│                              ▼                                   │
│                      ┌─────────────┐                            │
│                      │  Triggers   │                            │
│                      └─────────────┘                            │
│                              │                                   │
│                              ▼                                   │
│                      ┌─────────────┐                            │
│                      │  Aggregator  │                            │
│                      └─────────────┘                            │
│                              │                                   │
│                              ▼                                   │
│                      ┌─────────────┐                            │
│                      │   Emitter   │                            │
│                      └─────────────┘                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Window Types:

1. Tumbling Window (fixed, non-overlapping):
   
   |  0-5  |  5-10 | 10-15 | 15-20 |
   |  min  |  min  |  min  |  min  |

2. Sliding Window (overlapping, hop=2, size=5):
   
   |  0-5  |  2-7  |  4-9  |  6-11 |
   |  min  |  min  |  min  |  min  |

3. Session Window (activity-based):
   
   | Session 1 (gap=5) | Session 2 (gap=5) |
   | events...         | events...         |

Usage:

```python
from fluxdb.windowed import TumblingWindow, SlidingWindow, WindowedAggregator

# Tumbling window: every 5 minutes
window = TumblingWindow(size=timedelta(minutes=5))

# Sliding window: every 1 minute, 5 minute window
window = SlidingWindow(size=timedelta(minutes=5), step=timedelta(minutes=1))

# Create aggregator
aggregator = WindowedAggregator(
    window=window,
    aggregations=[
        ("count", lambda events: len(events)),
        ("sum", lambda events: sum(e.value for e in events)),
        ("avg", lambda events: sum(e.value for e in events) / len(events)),
    ]
)

# Process events
for event in event_stream:
    result = aggregator.add(event)
    if result:
        print(f"Window {result.window_id}: {result.aggregates}")
```
"""

import json
import time
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, TypeVar, Generic

import logging

from .aggregates import Event

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Types of windows"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


class TriggerType(Enum):
    """When to emit results"""
    ON_CLOSE = "on_close"
    ON_EVERY_EVENT = "on_every_event"
    ON_WATERMARK = "on_watermark"
    ON_COUNT = "on_count"
    ON_TIMEOUT = "on_timeout"


@dataclass
class Window:
    """Base class for windows"""
    window_id: str
    window_type: WindowType
    start_time: float
    end_time: float
    is_final: bool = False
    
    def contains(self, timestamp: float) -> bool:
        """Check if timestamp is within window"""
        return self.start_time <= timestamp < self.end_time
    
    def duration(self) -> float:
        """Get window duration in seconds"""
        return self.end_time - self.start_time
    
    @property
    def size(self) -> float:
        """Alias for duration"""
        return self.duration()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_id": self.window_id,
            "window_type": self.window_type.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "is_final": self.is_final,
        }


@dataclass
class WindowResult:
    """Result of window aggregation"""
    window: Window
    aggregates: Dict[str, Any]
    event_count: int
    late_events: int = 0
    watermark: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "window": self.window.to_dict(),
            "aggregates": self.aggregates,
            "event_count": self.event_count,
            "late_events": self.late_events,
            "watermark": self.watermark,
        }


@dataclass
class SessionWindow(Window):
    """Session-based window"""
    session_id: str
    gap: float
    last_activity: float = 0.0
    is_closed: bool = False
    
    def extend(self, timestamp: float) -> None:
        """Extend session window with new activity"""
        if timestamp > self.last_activity:
            self.end_time = timestamp + self.gap
            self.last_activity = timestamp
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "session_id": self.session_id,
            "gap": self.gap,
            "last_activity": self.last_activity,
            "is_closed": self.is_closed,
        })
        return base


class WindowAssigner(ABC):
    """
    Assigns events to windows.
    
    Window assigners determine which window(s) an event belongs to.
    """
    
    @abstractmethod
    def assign(self, timestamp: float) -> List[Window]:
        """Assign timestamp to appropriate window(s)"""
        pass
    
    @abstractmethod
    def get_earliest_window_end(self) -> float:
        """Get the earliest window end time"""
        pass


class TumblingWindowAssigner(WindowAssigner):
    """
    Tumbling (fixed, non-overlapping) window assigner.
    
    Events fall into exactly one window of fixed size.
    """
    
    def __init__(self, size_seconds: float):
        self.size_seconds = size_seconds
    
    def assign(self, timestamp: float) -> List[Window]:
        """Assign to tumbling window"""
        window_id = self._get_window_id(timestamp)
        start_time = timestamp - (timestamp % self.size_seconds)
        end_time = start_time + self.size_seconds
        
        window = Window(
            window_id=window_id,
            window_type=WindowType.TUMBLING,
            start_time=start_time,
            end_time=end_time,
        )
        return [window]
    
    def _get_window_id(self, timestamp: float) -> str:
        """Generate window ID from timestamp"""
        window_start = timestamp - (timestamp % self.size_seconds)
        return f"tumbling_{int(window_start)}"
    
    def get_earliest_window_end(self) -> float:
        """Get earliest window end (not determinable until first event)"""
        return float('inf')
    
    def get_window_for_time(self, timestamp: float) -> Window:
        """Get window for a specific time"""
        windows = self.assign(timestamp)
        return windows[0]


class SlidingWindowAssigner(WindowAssigner):
    """
    Sliding (overlapping) window assigner.
    
    Windows overlap and slide by a step size.
    """
    
    def __init__(self, size_seconds: float, step_seconds: float):
        self.size_seconds = size_seconds
        self.step_seconds = step_seconds
    
    def assign(self, timestamp: float) -> List[Window]:
        """Assign to sliding windows"""
        windows = []
        
        # Calculate all windows that contain this timestamp
        # Window slides every step_seconds
        current = timestamp - self.size_seconds + self.step_seconds
        
        while current <= timestamp:
            window_id = f"sliding_{int(current)}_{int(self.size_seconds)}"
            window = Window(
                window_id=window_id,
                window_type=WindowType.SLIDING,
                start_time=current,
                end_time=current + self.size_seconds,
            )
            windows.append(window)
            current += self.step_seconds
        
        return windows
    
    def get_earliest_window_end(self) -> float:
        return float('inf')


class SessionWindowAssigner(WindowAssigner):
    """
    Session window assigner.
    
    Windows are based on activity gaps.
    """
    
    def __init__(self, gap_seconds: float):
        self.gap_seconds = gap_seconds
        self._sessions: Dict[str, SessionWindow] = {}
        self._lock = threading.RLock()
    
    def assign(self, timestamp: float) -> List[Window]:
        """Assign to session window"""
        session_id = self._get_session_id(timestamp)
        
        with self._lock:
            if session_id not in self._sessions:
                self._sessions[session_id] = SessionWindow(
                    window_id=f"session_{session_id}",
                    window_type=WindowType.SESSION,
                    start_time=timestamp,
                    end_time=timestamp + self.gap_seconds,
                    session_id=session_id,
                    gap=self.gap_seconds,
                    last_activity=timestamp,
                )
            
            session = self._sessions[session_id]
            session.extend(timestamp)
            
            return [session]
    
    def _get_session_id(self, timestamp: float) -> str:
        """Generate session ID (simplified - would be more complex in practice)"""
        return f"session_{int(timestamp / self.gap_seconds)}"
    
    def get_earliest_window_end(self) -> float:
        """Get earliest session window end"""
        with self._lock:
            if not self._sessions:
                return float('inf')
            return min(s.end_time for s in self._sessions.values())
    
    def close_old_sessions(self, watermark: float) -> List[SessionWindow]:
        """Close sessions that have timed out"""
        with self._lock:
            closed = []
            for session in self._sessions.values():
                if session.end_time < watermark:
                    session.is_closed = True
                    closed.append(session)
            
            for session in closed:
                del self._sessions[session.session_id]
            
            return closed


class GlobalWindowAssigner(WindowAssigner):
    """
    Global window assigner.
    
    All events go to a single window.
    """
    
    GLOBAL_WINDOW_ID = "global"
    
    def assign(self, timestamp: float) -> List[Window]:
        """Assign to global window"""
        window = Window(
            window_id=self.GLOBAL_WINDOW_ID,
            window_type=WindowType.GLOBAL,
            start_time=0,
            end_time=float('inf'),
        )
        return [window]
    
    def get_earliest_window_end(self) -> float:
        return float('inf')


@dataclass
class AggregationDef:
    """Definition of an aggregation"""
    name: str
    function: Callable[[List[Any]], Any]
    initial_value: Any = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "initial_value": self.initial_value,
        }


class WindowedAggregator:
    """
    Main windowed aggregation processor.
    
    Combines window assigners with aggregation functions.
    """
    
    def __init__(
        self,
        window_assigner: WindowAssigner,
        aggregations: List[Union[str, AggregationDef, Tuple[str, Callable]]],
        trigger: TriggerType = TriggerType.ON_CLOSE,
        late_cutoff: float = 0.0,
    ):
        self.window_assigner = window_assigner
        self.trigger = trigger
        self.late_cutoff = late_cutoff
        
        # Parse aggregations
        self._aggregations: List[AggregationDef] = []
        for agg in aggregations:
            if isinstance(agg, str):
                self._aggregations.append(self._get_builtin_aggregation(agg))
            elif isinstance(agg, tuple):
                self._aggregations.append(AggregationDef(name=agg[0], function=agg[1]))
            elif isinstance(agg, AggregationDef):
                self._aggregations.append(agg)
        
        # Window state
        self._windows: Dict[str, Window] = {}
        self._window_events: Dict[str, List[Any]] = defaultdict(list)
        self._window_results: Dict[str, WindowResult] = {}
        self._late_count: Dict[str, int] = defaultdict(int)
        
        # Watermark
        self._watermark: float = 0
        
        # Lock
        self._lock = threading.RLock()
    
    def _get_builtin_aggregation(self, name: str) -> AggregationDef:
        """Get built-in aggregation by name"""
        builtins = {
            "count": AggregationDef(name="count", function=len, initial_value=0),
            "sum": AggregationDef(name="sum", function=lambda x: sum(x), initial_value=0),
            "min": AggregationDef(name="min", function=min, initial_value=None),
            "max": AggregationDef(name="max", function=max, initial_value=None),
            "avg": AggregationDef(
                name="avg",
                function=lambda x: sum(x) / len(x) if x else None,
                initial_value=None,
            ),
            "first": AggregationDef(name="first", function=lambda x: x[0] if x else None),
            "last": AggregationDef(name="last", function=lambda x: x[-1] if x else None),
            "collect": AggregationDef(name="collect", function=list),
        }
        
        if name not in builtins:
            raise ValueError(f"Unknown aggregation: {name}")
        
        return builtins[name]
    
    def add(self, event: Any, timestamp: Optional[float] = None) -> Optional[WindowResult]:
        """
        Add an event to the aggregator.
        
        Args:
            event: The event to add
            timestamp: Event timestamp (uses event.timestamp if not provided)
            
        Returns:
            WindowResult if trigger fired, None otherwise
        """
        timestamp = timestamp or (event.timestamp if hasattr(event, 'timestamp') else time.time())
        
        with self._lock:
            # Update watermark
            if timestamp > self._watermark:
                self._watermark = timestamp
            
            # Assign to windows
            windows = self.window_assigner.assign(timestamp)
            
            results = []
            for window in windows:
                is_late = window.end_time <= self._watermark - self.late_cutoff
                
                # Store event
                self._windows[window.window_id] = window
                self._window_events[window.window_id].append(event)
                
                if is_late:
                    self._late_count[window.window_id] += 1
                
                # Check trigger
                should_emit = self._should_emit(window)
                if should_emit:
                    result = self._emit(window)
                    if result:
                        results.append(result)
            
            # Return first result if any
            return results[0] if results else None
    
    def add_many(self, events: List[Any]) -> List[WindowResult]:
        """Add multiple events"""
        results = []
        for event in events:
            result = self.add(event)
            if result:
                results.append(result)
        return results
    
    def _should_emit(self, window: Window) -> bool:
        """Check if trigger should fire"""
        if self.trigger == TriggerType.ON_EVERY_EVENT:
            return True
        elif self.trigger == TriggerType.ON_CLOSE:
            return window.end_time <= self._watermark
        elif self.trigger == TriggerType.ON_WATERMARK:
            return window.end_time <= self._watermark
        return False
    
    def _emit(self, window: Window) -> Optional[WindowResult]:
        """Emit aggregation result for window"""
        events = self._window_events.get(window.window_id, [])
        
        if not events:
            return None
        
        # Compute aggregations
        aggregates = {}
        for agg in self._aggregations:
            try:
                values = [e.value if hasattr(e, 'value') else e for e in events]
                aggregates[agg.name] = agg.function(values)
            except Exception as e:
                logger.error(f"Aggregation {agg.name} failed: {e}")
                aggregates[agg.name] = None
        
        result = WindowResult(
            window=window,
            aggregates=aggregates,
            event_count=len(events),
            late_events=self._late_count.get(window.window_id, 0),
            watermark=self._watermark,
        )
        
        # Mark window as final and cleanup
        if window.end_time <= self._watermark:
            window.is_final = True
            del self._window_events[window.window_id]
        
        self._window_results[window.window_id] = result
        return result
    
    def close_early(self, window_id: str) -> Optional[WindowResult]:
        """Manually close a window early"""
        with self._lock:
            if window_id in self._windows:
                self._windows[window_id].is_final = True
                return self._emit(self._windows[window_id])
        return None
    
    def get_window_result(self, window_id: str) -> Optional[WindowResult]:
        """Get the result for a specific window"""
        return self._window_results.get(window_id)
    
    def get_all_results(self) -> Dict[str, WindowResult]:
        """Get all window results"""
        return self._window_results.copy()
    
    def get_watermark(self) -> float:
        """Get current watermark"""
        return self._watermark
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregator statistics"""
        return {
            "active_windows": len(self._windows),
            "total_results": len(self._window_results),
            "total_events": sum(len(e) for e in self._window_events.values()),
            "watermark": self._watermark,
            "late_count": dict(self._late_count),
        }


class TimeSeriesAggregator:
    """
    High-level API for time-series aggregations.
    
    Provides a simpler interface for common aggregation patterns.
    """
    
    def __init__(
        self,
        interval_seconds: float,
        aggregations: List[str] = None,
    ):
        self.interval_seconds = interval_seconds
        self.aggregations = aggregations or ["count", "sum", "avg", "min", "max"]
        
        # Create windowed aggregator
        window_assigner = TumblingWindowAssigner(interval_seconds)
        agg_defs = [
            (name, self._get_agg_function(name))
            for name in self.aggregations
        ]
        
        self.aggregator = WindowedAggregator(
            window_assigner=window_assigner,
            aggregations=agg_defs,
            trigger=TriggerType.ON_CLOSE,
        )
        
        self._results: Dict[str, WindowResult] = {}
        self._events: List[Any] = []
    
    def _get_agg_function(self, name: str) -> Callable:
        """Get aggregation function by name"""
        if name == "count":
            return lambda values: len(values)
        elif name == "sum":
            return lambda values: sum(values)
        elif name == "avg":
            return lambda values: sum(values) / len(values) if values else 0
        elif name == "min":
            return lambda values: min(values) if values else None
        elif name == "max":
            return lambda values: max(values) if values else None
        else:
            raise ValueError(f"Unknown aggregation: {name}")
    
    def add(self, value: float, timestamp: Optional[float] = None) -> Optional[WindowResult]:
        """Add a data point"""
        class EventWrapper:
            def __init__(self, value, timestamp):
                self.value = value
                self.timestamp = timestamp
        
        event = EventWrapper(value, timestamp or time.time())
        self._events.append(event)
        
        result = self.aggregator.add(event)
        if result:
            self._results[result.window.window_id] = result
        
        return result
    
    def get_results(self) -> List[Dict[str, Any]]:
        """Get all results"""
        return [r.to_dict() for r in self._results.values()]
    
    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Get the latest result"""
        if not self._results:
            return None
        
        latest = max(self._results.values(), key=lambda r: r.window.start_time)
        return latest.to_dict()
