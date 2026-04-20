"""
Kafka-Compatible Event Log for FluxDB

This module implements a Kafka-compatible event log that provides:
- Persistent, ordered event streaming
- Consumer groups with independent offsets
- Topic-based event organization
- Partition support for parallel processing
- Event retention and cleanup policies

The event log is the backbone of FluxDB's event sourcing architecture,
enabling event replay, reprocessing, and stream processing capabilities.

Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                        EventLog                                 │
├─────────────────────────────────────────────────────────────────┤
│  Topics                                                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                │
│  │  "orders"   │ │  "users"    │ │  "payments" │                │
│  ├─────────────┤ ├─────────────┤ ├─────────────┤                │
│  │ Partition 0 │ │ Partition 0 │ │ Partition 0 │                │
│  │ Partition 1 │ │ Partition 1 │ │ Partition 1 │                │
│  │ Partition 2 │ │ Partition 2 │ │ Partition 2 │                │
│  └─────────────┘ └─────────────┘ └─────────────┘                │
├─────────────────────────────────────────────────────────────────┤
│  Consumer Groups                                                │
│  ┌─────────────────────────────────────┐                       │
│  │ Group: "order-processor"            │                       │
│  │  Consumer 1 -> offset 1500          │                       │
│  │  Consumer 2 -> offset 1200          │                       │
│  └─────────────────────────────────────┘                       │
│  ┌─────────────────────────────────────┐                       │
│  │ Group: "analytics"                  │                       │
│  │  Consumer 1 -> offset 3000          │                       │
│  └─────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
"""

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union
from collections import defaultdict
from enum import Enum
import hashlib
import logging
import struct

from .storage import AppendOnlyStore, Record, RecordType

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Event compression types"""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"


@dataclass
class Event:
    """
    Represents an event in the event log.
    
    Events are the fundamental unit of data in FluxDB's event sourcing
    architecture. Each event contains:
    - topic: The event topic/stream name
    - key: Partition key for ordering
    - value: The event payload
    - headers: Metadata headers
    - timestamp: When the event occurred
    - sequence: Global sequence number for ordering
    """
    topic: str
    key: Optional[str]
    value: Any
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0
    headers: Dict[str, str] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    partition: int = 0
    compression: CompressionType = CompressionType.NONE
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
            "headers": self.headers,
            "partition": self.partition,
            "compression": self.compression.value,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create event from dictionary"""
        return cls(
            event_id=data["event_id"],
            topic=data["topic"],
            key=data["key"],
            value=data["value"],
            timestamp=data["timestamp"],
            sequence=data["sequence"],
            headers=data.get("headers", {}),
            partition=data.get("partition", 0),
            compression=CompressionType(data.get("compression", "none")),
        )
    
    def to_bytes(self) -> bytes:
        """Serialize event to bytes"""
        data = self.to_dict()
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        return struct.pack(">I", len(json_bytes)) + json_bytes
    
    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> "Event":
        """Deserialize event from bytes"""
        length = struct.unpack(">I", data[offset:offset + 4])[0]
        json_bytes = data[offset + 4:offset + 4 + length]
        parsed = json.loads(json_bytes.decode("utf-8"))
        return cls.from_dict(parsed)


@dataclass
class TopicMetadata:
    """Metadata for an event topic"""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    retention_bytes: int
    created_at: float
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerOffset:
    """Tracks the offset position for a consumer in a consumer group"""
    group_id: str
    topic: str
    partition: int
    current_offset: int
    last_updated: float
    lag: int = 0


class EventLog:
    """
    Kafka-compatible event log with topic-based organization.
    
    This class implements a persistent, ordered event log that supports:
    - Multiple topics with configurable retention
    - Partitioned events for parallel processing
    - Consumer groups with independent offset tracking
    - Event replay and seeking
    - At-least-once delivery semantics
    
    Usage:
    
    ```python
    log = EventLog("./data")
    
    # Create a topic
    log.create_topic("orders", partitions=3)
    
    # Produce events
    log.produce("orders", key="user:1", value={"order_id": "123", "total": 99.99})
    
    # Consume events
    consumer = log.consumer("order-processor", topics=["orders"])
    for event in consumer:
        print(f"Received: {event}")
        consumer.commit()
    
    # Seek to specific offset
    consumer.seek("orders", partition=0, offset=100)
    ```
    """
    
    MAX_EVENT_SIZE = 1024 * 1024  # 1 MB max event size
    DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days
    DEFAULT_RETENTION_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
    DEFAULT_PARTITIONS = 3
    
    def __init__(
        self,
        path: Union[str, Path],
        storage: Optional[AppendOnlyStore] = None,
        create_if_missing: bool = True,
    ):
        """
        Initialize the event log.
        
        Args:
            path: Directory to store event log data
            storage: Optional existing AppendOnlyStore to use
            create_if_missing: Create directory if it doesn't exist
        """
        self.base_path = Path(path)
        self.create_if_missing = create_if_missing
        
        if create_if_missing:
            self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize or use provided storage
        self.storage = storage or AppendOnlyStore(
            self.base_path / "storage",
            create_if_missing=create_if_missing,
        )
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Topic management
        self._topics: Dict[str, TopicMetadata] = {}
        self._topic_partitions: Dict[str, Dict[int, List[int]]]] = defaultdict(dict)
        
        # Consumer group offsets
        self._consumer_offsets: Dict[str, Dict[str, ConsumerOffset]] = defaultdict(dict)
        
        # Event sequence number
        self._sequence: int = 0
        
        # Partition assignment
        self._partitioner: Callable[[str, int], int] = self._default_partitioner
        
        # Load existing topics
        self._load_topics()
        
        logger.info(f"EventLog initialized at {self.base_path}")
    
    def _load_topics(self) -> None:
        """Load topic metadata from disk"""
        metadata_path = self.base_path / "topics.json"
        if metadata_path.exists():
            with open(metadata_path, "r") as f:
                topics_data = json.load(f)
                for name, data in topics_data.items():
                    self._topics[name] = TopicMetadata(**data)
    
    def _save_topics(self) -> None:
        """Save topic metadata to disk"""
        metadata_path = self.base_path / "topics.json"
        topics_data = {
            name: {
                "name": meta.name,
                "partitions": meta.partitions,
                "replication_factor": meta.replication_factor,
                "retention_ms": meta.retention_ms,
                "retention_bytes": meta.retention_bytes,
                "created_at": meta.created_at,
                "config": meta.config,
            }
            for name, meta in self._topics.items()
        }
        with open(metadata_path, "w") as f:
            json.dump(topics_data, f, indent=2)
    
    def _default_partitioner(self, key: str, num_partitions: int) -> int:
        """Default partitioner using hash of key"""
        if key is None:
            return 0
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_partitions
    
    def create_topic(
        self,
        name: str,
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = 1,
        retention_ms: int = DEFAULT_RETENTION_MS,
        retention_bytes: int = DEFAULT_RETENTION_BYTES,
        config: Optional[Dict[str, Any]] = None,
    ) -> TopicMetadata:
        """
        Create a new topic in the event log.
        
        Args:
            name: Topic name
            partitions: Number of partitions
            replication_factor: Replication factor (for distributed setup)
            retention_ms: Event retention time in milliseconds
            retention_bytes: Event retention size in bytes
            config: Additional topic configuration
            
        Returns:
            TopicMetadata for the created topic
        """
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic '{name}' already exists")
            
            meta = TopicMetadata(
                name=name,
                partitions=partitions,
                replication_factor=replication_factor,
                retention_ms=retention_ms,
                retention_bytes=retention_bytes,
                created_at=time.time(),
                config=config or {},
            )
            
            self._topics[name] = meta
            
            # Initialize partition tracking
            for p in range(partitions):
                self._topic_partitions[name][p] = []
            
            self._save_topics()
            
            logger.info(f"Created topic '{name}' with {partitions} partitions")
            return meta
    
    def delete_topic(self, name: str) -> None:
        """
        Delete a topic and all its events.
        
        Args:
            name: Topic name to delete
        """
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            
            del self._topics[name]
            del self._topic_partitions[name]
            
            self._save_topics()
            
            logger.info(f"Deleted topic '{name}'")
    
    def list_topics(self) -> List[str]:
        """List all topic names"""
        return list(self._topics.keys())
    
    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
    ) -> Event:
        """
        Produce an event to a topic.
        
        Args:
            topic: Topic name to produce to
            value: Event payload
            key: Optional partition key
            headers: Optional event headers
            partition: Optional explicit partition (uses partitioner if not provided)
            timestamp: Optional event timestamp
            
        Returns:
            The produced Event
        """
        with self._lock:
            if topic not in self._topics:
                raise ValueError(f"Topic '{topic}' does not exist")
            
            # Determine partition
            if partition is None:
                partition = self._partitioner(key, self._topics[topic].partitions)
            
            # Get sequence number
            self._sequence += 1
            
            # Create event
            event = Event(
                topic=topic,
                key=key,
                value=value,
                timestamp=timestamp or time.time(),
                sequence=self._sequence,
                headers=headers or {},
                partition=partition,
            )
            
            # Store event
            self.storage.put(
                key=f"{topic}:{partition}:{event.event_id}",
                value=event.to_dict(),
                record_type=RecordType.DATA,
                headers={
                    "topic": topic,
                    "partition": str(partition),
                    "event_id": event.event_id,
                },
            )
            
            # Update partition tracking
            self._topic_partitions[topic][partition].append(event.sequence)
            
            logger.debug(f"Produced event to {topic}:{partition} seq={event.sequence}")
            return event
    
    def produce_batch(
        self,
        topic: str,
        events: List[Dict[str, Any]],
        key: Optional[str] = None,
    ) -> List[Event]:
        """
        Produce multiple events in a batch.
        
        Args:
            topic: Topic name
            events: List of event values to produce
            key: Optional partition key
            
        Returns:
            List of produced Events
        """
        produced = []
        for event_value in events:
            if isinstance(event_value, dict) and "value" in event_value:
                event = self.produce(
                    topic=topic,
                    value=event_value["value"],
                    key=event_value.get("key", key),
                    headers=event_value.get("headers"),
                    partition=event_value.get("partition"),
                )
            else:
                event = self.produce(
                    topic=topic,
                    value=event_value,
                    key=key,
                )
            produced.append(event)
        return produced
    
    def consumer(
        self,
        group_id: str,
        topics: Optional[List[str]] = None,
        auto_offset_reset: str = "earliest",
    ) -> "EventConsumer":
        """
        Create a consumer for the event log.
        
        Args:
            group_id: Consumer group ID
            topics: List of topics to subscribe to
            auto_offset_reset: Where to start if no offset exists
            
        Returns:
            EventConsumer instance
        """
        return EventConsumer(
            log=self,
            group_id=group_id,
            topics=topics or [],
            auto_offset_reset=auto_offset_reset,
        )
    
    def get_latest_offset(self, topic: str, partition: int) -> int:
        """Get the latest offset for a topic/partition"""
        if topic not in self._topic_partitions:
            return -1
        partitions = self._topic_partitions[topic]
        if partition not in partitions:
            return -1
        offsets = partitions[partition]
        return max(offsets) if offsets else -1
    
    def get_earliest_offset(self, topic: str, partition: int) -> int:
        """Get the earliest offset for a topic/partition"""
        if topic not in self._topic_partitions:
            return 0
        partitions = self._topic_partitions[topic]
        if partition not in partitions:
            return 0
        offsets = partitions[partition]
        return min(offsets) if offsets else 0
    
    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_events: int = 100,
    ) -> List[Event]:
        """
        Fetch events starting from a specific offset.
        
        Args:
            topic: Topic name
            partition: Partition number
            offset: Starting offset
            max_events: Maximum events to fetch
            
        Returns:
            List of Events
        """
        events = []
        topic_meta = self._topics.get(topic)
        if not topic_meta or partition >= topic_meta.partitions:
            return events
        
        for seq in range(offset, offset + max_events):
            key = f"{topic}:{partition}:*"
            # Scan for matching events
            for stored_key in self.storage.keys():
                if stored_key.startswith(f"{topic}:{partition}:"):
                    record = self.storage.get(stored_key)
                    if record:
                        event_data = record.value
                        if isinstance(event_data, dict) and event_data.get("sequence") == seq:
                            events.append(Event.from_dict(event_data))
        
        return events
    
    def replay(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: Optional[int] = None,
        processor: Optional[Callable[[Event], None]] = None,
    ) -> Iterator[Event]:
        """
        Replay events for reprocessing.
        
        This is useful for:
        - Rebuilding projections
        - Reprocessing failed events
        - Debugging event processing
        
        Args:
            topic: Topic name
            partition: Partition number
            start_offset: Starting offset
            end_offset: Ending offset (None for latest)
            processor: Optional callback for each event
            
        Yields:
            Events in offset order
        """
        topic_meta = self._topics.get(topic)
        if not topic_meta:
            return
        
        end = end_offset or self.get_latest_offset(topic, partition)
        
        for seq in range(start_offset, end + 1):
            events = self.fetch(topic, partition, seq, max_events=1)
            for event in events:
                if processor:
                    processor(event)
                yield event
    
    def get_topic_info(self, topic: str) -> Optional[TopicMetadata]:
        """Get metadata for a topic"""
        return self._topics.get(topic)
    
    def close(self) -> None:
        """Close the event log"""
        self._save_topics()
        self.storage.close()


class EventConsumer:
    """
    Event consumer with offset management.
    
    Consumers maintain their position in each topic/partition and
    support various offset reset strategies.
    """
    
    def __init__(
        self,
        log: EventLog,
        group_id: str,
        topics: List[str],
        auto_offset_reset: str = "earliest",
    ):
        self.log = log
        self.group_id = group_id
        self.topics = topics
        self.auto_offset_reset = auto_offset_reset
        
        self._offsets: Dict[str, Dict[int, int]] = defaultdict(dict)
        self._running = False
        self._position: Dict[str, Dict[int, int]] = defaultdict(dict)
        
        # Load committed offsets
        self._load_offsets()
    
    def _load_offsets(self) -> None:
        """Load committed offsets from storage"""
        offset_path = self.log.base_path / f"offsets_{self.group_id}.json"
        if offset_path.exists():
            with open(offset_path, "r") as f:
                data = json.load(f)
                for topic, partitions in data.items():
                    for partition, offset in partitions.items():
                        self._offsets[topic][int(partition)] = offset
    
    def _save_offsets(self) -> None:
        """Save committed offsets to storage"""
        offset_path = self.log.base_path / f"offsets_{self.group_id}.json"
        data = {
            topic: {str(p): o for p, o in partitions.items()}
            for topic, partitions in self._offsets.items()
        }
        with open(offset_path, "w") as f:
            json.dump(data, f)
    
    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to additional topics"""
        self.topics.extend(topics)
    
    def seek(self, topic: str, partition: int, offset: int) -> None:
        """Seek to a specific offset"""
        self._position[topic][partition] = offset
    
    def tell(self, topic: str, partition: int) -> int:
        """Get current position for topic/partition"""
        return self._position[topic].get(partition, 0)
    
    def commit(self) -> None:
        """Commit current offsets"""
        for topic, partitions in self._position.items():
            for partition, offset in partitions.items():
                self._offsets[topic][partition] = offset
        self._save_offsets()
    
    def __iter__(self):
        return self
    
    def __next__(self) -> Event:
        if not self._running:
            self._running = True
            self._initialize_positions()
        
        # Poll for events
        for topic in self.topics:
            topic_meta = self.log.get_topic_info(topic)
            if not topic_meta:
                continue
            
            for partition in range(topic_meta.partitions):
                current = self._position[topic].get(partition)
                if current is None:
                    # Initialize position
                    if self.auto_offset_reset == "earliest":
                        current = self.log.get_earliest_offset(topic, partition)
                    else:
                        current = self.log.get_latest_offset(topic, partition)
                
                # Fetch next event
                events = self.log.fetch(topic, partition, current, max_events=1)
                if events:
                    event = events[0]
                    self._position[topic][partition] = event.sequence + 1
                    return event
        
        raise StopIteration
    
    def _initialize_positions(self) -> None:
        """Initialize consumer positions from committed offsets or reset policy"""
        for topic in self.topics:
            topic_meta = self.log.get_topic_info(topic)
            if not topic_meta:
                continue
            
            for partition in range(topic_meta.partitions):
                if topic in self._offsets and partition in self._offsets[topic]:
                    self._position[topic][partition] = self._offsets[topic][partition] + 1
                elif self.auto_offset_reset == "earliest":
                    self._position[topic][partition] = self.log.get_earliest_offset(topic, partition)
                else:
                    self._position[topic][partition] = self.log.get_latest_offset(topic, partition) + 1
    
    def close(self) -> None:
        """Close the consumer and commit offsets"""
        self.commit()
        self._running = False
