"""
FluxDB - Append-Only Distributed Database

An immutable, event-driven database built on append-only storage,
CRDT-based conflict resolution, and Raft consensus.
"""

__version__ = "0.1.0"
__author__ = "FluxDB Team"

from .core.storage import AppendOnlyStore
from .core.event_log import EventLog
from .event_sourcing.aggregates import Aggregate, Event
from .crdt.registers import LWWRegister, ORSet
from .crdt.counters import PNCounter, GCounter

__all__ = [
    "AppendOnlyStore",
    "EventLog",
    "Aggregate",
    "Event",
    "LWWRegister",
    "ORSet",
    "PNCounter",
    "GCounter",
]
