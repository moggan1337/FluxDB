# FluxDB - Append-Only Distributed Database
## 🎬 Demo
![FluxDB Demo](demo.gif)

*Append-only storage with CRDT conflict resolution*

## Screenshots
| Component | Preview |
|-----------|---------|
| Event Log | ![log](screenshots/event-log.png) |
| CRDT Merge | ![merge](screenshots/crdt-merge.png) |
| Cluster Status | ![cluster](screenshots/cluster.png) |

## Visual Description
Event log shows immutable records being appended with hash chains. CRDT merge displays concurrent operations being resolved. Cluster status presents distributed nodes with Raft leadership.

---



<p align="center">
  <img src="https://img.shields.io/badge/Version-0.1.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.8+-green.svg" alt="Python">
  <img src="https://img.shields.io/badge/License-MIT-orange.svg" alt="License">
</p>

FluxDB is an immutable, append-only distributed database built on event sourcing architecture with CRDT-based conflict resolution and Raft consensus for distributed coordination.

## 🚀 Features

### Core Storage
- **Append-Only Storage Engine**: Immutable data storage where records are never modified, only appended
- **Segmented Storage**: Data split into manageable segments for efficient reads and writes
- **Content Addressing**: Records referenced by content hash for deduplication
- **Sequence Numbers**: Monotonic ordering of all records for consistency

### Event Sourcing
- **Event Store**: Persistent event log with Kafka-compatible API
- **Aggregates**: Command handling with event generation
- **Projections**: Materialized views built from events
- **Event Replay**: Rebuild state by replaying events
- **Time-Travel Queries**: Query historical states at any point in time

### CRDT (Conflict-Free Replicated Data Types)
- **LWW-Register**: Last-Writer-Wins register for simple value storage
- **MV-Register**: Multi-Value register for preserving concurrent values
- **OR-Set**: Observed-Remove set for collaborative editing
- **Two-Phase Set**: Add-only set with removes
- **G-Counter**: Grow-only distributed counter
- **PN-Counter**: Positive-Negative counter for balance tracking
- **LWW-Map**: Map with LWW semantics per key
- **GCounterMap/PNCounterMap**: Counter collections per key

### Distributed Systems
- **Raft Consensus**: Leader election and log replication
- **Distributed Replication**: Sync and async replication modes
- **Consistency Levels**: Strong, quorum, local quorum, eventual
- **Anti-Entropy**: Merkle tree-based consistency repair
- **Automatic Failover**: Primary replica failover

### Advanced Features
- **Snapshots**: Point-in-time snapshots for fast recovery
- **Compaction**: Remove tombstoned data and optimize storage
- **Windowed Aggregations**: Tumbling, sliding, and session windows
- **Kafka-Compatible Event Log**: Topics, partitions, consumer groups

## 📖 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FluxDB Architecture                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Client Applications                          │    │
│  │    Python Client  │  HTTP API  │  CLI  │  Event Consumers          │    │
│  └──────────────────────────┬──────────────────────────────────────────┘    │
│                              │                                              │
│  ┌──────────────────────────▼──────────────────────────────────────────┐    │
│  │                        API Layer                                      │    │
│  │   PUT/GET  │  Event Emit  │  CRDT Ops  │  Admin  │  Time-Travel      │    │
│  └──────────────────────────┬──────────────────────────────────────────┘    │
│                              │                                              │
│  ┌──────────────────────────▼──────────────────────────────────────────┐    │
│  │                    Core Components                                   │    │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐      │    │
│  │  │  Append    │  │  Event     │  │  Time      │  │  Snapshot  │      │    │
│  │  │  Only      │  │  Log       │  │  Travel    │  │  Manager   │      │    │
│  │  │  Store     │  │  (Kafka)   │  │  Queries   │  │            │      │    │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘      │    │
│  └──────────────────────────┬──────────────────────────────────────────┘    │
│                              │                                              │
│  ┌──────────────────────────▼──────────────────────────────────────────┐    │
│  │                   Event Sourcing Layer                                │    │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐      │    │
│  │  │ Aggregate  │  │Projection │  │  Event     │  │  Windowed  │      │    │
│  │  │  Root      │  │ Manager   │  │  Replay     │  │Aggregation │      │    │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘      │    │
│  └──────────────────────────┬──────────────────────────────────────────┘    │
│                              │                                              │
│  ┌──────────────────────────▼──────────────────────────────────────────┐    │
│  │                       CRDT Layer                                       │    │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐      │    │
│  │  │ Registers │  │ Counters   │  │   Sets     │  │   Maps     │      │    │
│  │  │ LWW/MV    │  │ G/PN      │  │ OR-Set     │  │ LWW/GCounter│     │    │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘      │    │
│  └──────────────────────────┬──────────────────────────────────────────┘    │
│                              │                                              │
│  ┌──────────────────────────▼──────────────────────────────────────────┐    │
│  │                 Consensus & Replication                               │    │
│  │  ┌──────────────────────┐  ┌────────────────────────────────────┐   │    │
│  │  │   Raft Consensus      │  │   Distributed Replication          │   │    │
│  │  │   Leader Election     │  │   Sync/Async/Quorum                 │   │    │
│  │  │   Log Replication     │  │   Anti-Entropy                      │   │    │
│  │  └──────────────────────┘  └────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🔧 Installation

```bash
# Clone the repository
git clone https://github.com/moggan1337/FluxDB.git
cd FluxDB

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v

# Run examples
python examples/basic_usage.py
```

## 🎯 Use Cases

FluxDB is designed for various use cases:

### 1. Event-Driven Architecture
Perfect for event-driven microservices where events are the source of truth:
- Audit trails
- Financial transactions
- User activity tracking
- IoT data pipelines

### 2. Collaborative Applications
CRDTs enable real-time collaboration without conflicts:
- Collaborative documents
- Shared shopping carts
- Multiplayer games
- Distributed counters

### 3. Time-Series Data
Time-travel queries make it ideal for time-series:
- Sensor data
- Stock prices
- Application metrics
- User behavior analytics

### 4. Distributed Systems
Raft consensus provides strong consistency:
- Configuration stores
- Service discovery
- Distributed locks
- Coordination services

## 📚 Documentation

### Quick Start

```python
from fluxdb import FluxDBClient

# Create client
client = FluxDBClient("localhost", 8080)

# Store data
client.put("users:1", {"name": "Alice", "age": 30})

# Get data
value = client.get("users:1")
print(value)  # {"name": "Alice", "age": 30}

# Delete data
client.delete("users:1")

# Close
client.close()
```

### Append-Only Storage

```python
from src.core.storage import AppendOnlyStore

# Create store
store = AppendOnlyStore("./data")

# Put data
store.put("key1", {"name": "Alice"})

# Get latest
record = store.get("key1")
print(record.value)  # {"name": "Alice"}

# Update (creates new record, history preserved)
store.put("key1", {"name": "Alice Smith"})

# Get history
for record in store.get_history("key1"):
    print(f"{record.value} @ seq={record.sequence}")

# Time-travel query
past = store.get_at("key1", timestamp=1700000000)
print(past.value)

store.close()
```

### Event Sourcing

```python
from src.event_sourcing.aggregates import Event, AggregateRoot, EventStore

# Create event store
store = EventStore("./events")

# Create aggregate
class Account(AggregateRoot):
    def __init__(self, account_id):
        super().__init__(account_id)
        self.balance = 0
    
    def deposit(self, amount):
        self.balance += amount
        self._add_event(Event(
            event_type="Deposited",
            metadata={"amount": amount, "new_balance": self.balance}
        ))
    
    def apply(self, event):
        if event.event_type == "Deposited":
            self.balance = event.metadata["new_balance"]

# Create and use aggregate
account = Account("acc:1")
account.deposit(100)
account.deposit(50)

# Save events
store.append("acc:1", account.pull_events())

# Reconstruct from events
new_account = Account("acc:1")
new_account.replay(store.get_events("acc:1"))
print(new_account.balance)  # 150
```

### Time-Travel Queries

```python
from src.core.time_travel import TimeTravelStore

store = TimeTravelStore("./data")

# Create timeline
store.put("counter", 0)
time.sleep(0.1)
t1 = time.time()
store.put("counter", 10)
time.sleep(0.1)
store.put("counter", 25)

# Query at specific time
value_at_t1 = store.as_of("counter", timestamp=t1)
print(value_at_t1.value)  # 10

# Get full history
for entry in store.history("counter"):
    print(f"{entry.value} from {entry.valid_from}")

# Diff between times
diffs = store.diff("counter", t1, time.time())
```

### CRDT Usage

```python
from src.crdt.registers import LWWRegister, ORSet
from src.crdt.counters import GCounter, PNCounter

# LWW-Register
reg1 = LWWRegister(node_id="node1")
reg1.set("value1")

reg2 = LWWRegister(node_id="node2")
reg2.set("value2")

reg1.merge(reg2)
print(reg1.get())  # value2 (later timestamp)

# OR-Set for collaborative editing
set1 = ORSet(node_id="user1")
set1.add("item1")
set1.add("item2")

set2 = ORSet(node_id="user2")
set2.add("item2")
set2.add("item3")

set1.merge(set2)
print(set1.get())  # {"item1", "item2", "item3"}

# Distributed Counter
counter = GCounter()
counter.increment("node1", 100)
counter.increment("node2", 50)
print(counter.value())  # 150
```

### Projections

```python
from src.event_sourcing.projections import Projection, ProjectionManager

class UserProjection(Projection):
    name = "users"
    
    def init(self):
        self.users = {}
    
    def when(self, event):
        if event.event_type == "UserCreated":
            self.users[event.aggregate_id] = event.payload

manager = ProjectionManager(event_store)
manager.register(UserProjection())
manager.start()

# Query projection
users = manager.get_projection("users").users
```

### Raft Consensus

```python
from src.consensus.raft import RaftCluster

# Create cluster
cluster = RaftCluster(
    node_ids=["node1", "node2", "node3"],
    heartbeat_interval=0.5,
    election_timeout=2.0,
)

cluster.start()

# Wait for leader
leader = cluster.wait_for_leader()

# Submit command
future = leader.submit({"type": "set", "key": "foo", "value": "bar"})
result = future.result(timeout=5)

cluster.stop()
```

## 📊 Event Sourcing Deep Dive

Event sourcing is a pattern where state changes are stored as a sequence of events rather than current state. FluxDB implements this pattern with the following components:

### Event Store Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Event Store                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Event 1: UserCreated  @ seq=1                              │ │
│  │ Event 2: UserUpdated  @ seq=2                              │ │
│  │ Event 3: UserDeleted  @ seq=3  (tombstone)                 │ │
│  │ ...                                                         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Operations:                                                     │
│  - append(aggregate_id, events) → Add events                     │
│  - get_events(aggregate_id) → Retrieve all events              │
│  - get_all_events() → Stream all events                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Aggregate Root Pattern

Aggregates are the consistency boundary in event sourcing. They:
1. Receive commands (requests to change state)
2. Validate business rules
3. Generate events
4. Apply events to maintain state

```python
class Order(AggregateRoot):
    def __init__(self, order_id):
        super().__init__(order_id)
        self.status = "pending"
        self.items = []
    
    def place_order(self, items):
        # Validate
        if not items:
            raise ValueError("Order must have items")
        
        # Update state
        self.items = items
        self.status = "confirmed"
        
        # Generate event
        self._add_event(Event(
            event_type="OrderPlaced",
            metadata={"items": items}
        ))
    
    def apply(self, event):
        # Reconstruct state from events
        if event.event_type == "OrderPlaced":
            self.items = event.payload["items"]
            self.status = "confirmed"
```

### Projection Pattern

Projections build read models from events:

```python
class OrderProjection(Projection):
    name = "orders"
    
    def init(self):
        self.orders = {}
        self.by_status = defaultdict(list)
    
    def when(self, event):
        if event.event_type == "OrderPlaced":
            order_id = event.aggregate_id
            self.orders[order_id] = event.payload
            self.by_status["placed"].append(order_id)
```

## 🔄 CRDT Deep Dive

CRDTs (Conflict-free Replicated Data Types) enable distributed data structures that automatically merge without coordination.

### LWW-Register (Last-Writer-Wins)

The simplest CRDT - the value with the highest timestamp wins:

```
Node A: value="Hello" @ t=100
Node B: value="World" @ t=101

After merge: value="World" (t=101 > t=100)
```

```python
reg1 = LWWRegister(node_id="a")
reg1.set("Hello", timestamp=Timestamp(100, 0, "a"))

reg2 = LWWRegister(node_id="b")
reg2.set("World", timestamp=Timestamp(101, 0, "b"))

reg1.merge(reg2)
print(reg1.get())  # "World"
```

### OR-Set (Observed-Remove Set)

Add-only set where removes are observed:

```python
set1 = ORSet(node_id="user1")
set1.add("Apple")
set1.add("Banana")

set2 = ORSet(node_id="user2")
set2.add("Banana")  # Concurrent add
set2.add("Cherry")

# Merge - all elements preserved
set1.merge(set2)
print(set1.get())  # {"Apple", "Banana", "Cherry"}
```

### G-Counter (Grow-only Counter)

Counter that can only increase:

```
Node A: 5
Node B: 3

Merged: 5 + 3 = 8
```

### PN-Counter (Positive-Negative Counter)

Supports both increments and decrements:

```python
balance = PNCounter()
balance.increment("node1", 100)   # +100
balance.decrement("node1", 30)    # -30
print(balance.value())  # 70
```

### Vector Clocks

Vector clocks track causality:

```python
vc1 = VectorClock({"node1": 2, "node2": 1})
vc2 = VectorClock({"node1": 1, "node3": 3})

vc1.merge(vc2)
# Result: {"node1": 2, "node2": 1, "node3": 3}
```

## 🌐 Distributed Replication

FluxDB supports multiple replication modes:

### Synchronous Replication

Waits for all replicas before acknowledging:

```
Client → Primary → Replica1 → Replica2 → ACK
```

### Asynchronous Replication

Acknowledges immediately, replicates in background:

```
Client → Primary → ACK
           ↓
        Replica1
           ↓
        Replica2
```

### Quorum Replication

Waits for majority:

```python
from src.replication import ReplicationManager, ConsistencyLevel

manager = ReplicationManager(
    local_node_id="node1",
    replication_mode="quorum"
)

# Write with quorum consistency
manager.write("key", "value", consistency=ConsistencyLevel.QUORUM)
```

## 🛠️ Configuration

### AppendOnlyStore Configuration

```python
store = AppendOnlyStore(
    path="./data",
    segment_size=128 * 1024 * 1024,  # 128MB per segment
    create_if_missing=True,
    fsync_enabled=True,  # Enable for durability
)
```

### Raft Configuration

```python
config = RaftConfig(
    heartbeat_interval=0.5,      # Heartbeat every 500ms
    election_timeout_min=1.5,     # Min election timeout
    election_timeout_max=3.0,     # Max election timeout
    snapshot_interval=10000,      # Snapshot every 10k entries
)
```

### Event Log Configuration

```python
log = EventLog(
    path="./eventlog",
    create_if_missing=True,
)

log.create_topic(
    name="orders",
    partitions=3,
    retention_ms=7 * 24 * 60 * 60 * 1000,  # 7 days
)
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_storage.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run examples
python examples/basic_usage.py
python examples/event_sourcing_example.py
python examples/crdt_example.py
```

## 📋 Project Structure

```
FluxDB/
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── storage.py          # Append-only storage
│   │   ├── event_log.py        # Kafka-compatible event log
│   │   ├── snapshots.py         # Snapshots & compaction
│   │   └── time_travel.py       # Point-in-time queries
│   ├── event_sourcing/
│   │   ├── aggregates.py         # Aggregate roots
│   │   ├── projections.py        # Materialized views
│   │   ├── replay.py            # Event replay
│   │   └── windowed_agg.py      # Windowed aggregations
│   ├── crdt/
│   │   ├── registers.py         # LWW, MV, OR-Set registers
│   │   ├── counters.py          # G/PN counters
│   │   └── merge.py             # Merge operations
│   ├── consensus/
│   │   └── raft.py              # Raft implementation
│   ├── replication/
│   │   └── follower.py          # Replication manager
│   └── api/
│       └── client.py            # Python client
├── tests/
│   ├── test_storage.py
│   ├── test_event_sourcing.py
│   └── test_crdt.py
├── examples/
│   ├── basic_usage.py
│   ├── event_sourcing_example.py
│   └── crdt_example.py
├── README.md
├── requirements.txt
└── setup.py
```

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

- Inspired by Apache Kafka's event log
- CRDT implementations based on research by Shapiro et al.
- Raft implementation based on Ongaro & Ousterhout's paper

## 📚 Appendix

### A. Terminology

| Term | Description |
|------|-------------|
| Append-Only | Storage where data is never modified, only added |
| Event Sourcing | Pattern where state changes are stored as events |
| CRDT | Conflict-free Replicated Data Type |
| Aggregate | Consistency boundary in event sourcing |
| Projection | Derived read model from events |
| Raft | Consensus algorithm for distributed systems |
| Watermark | Threshold for processing time-based windows |

### B. Performance Considerations

1. **Segment Size**: Choose based on workload (128MB default)
2. **fsync**: Enable for durability, disable for speed
3. **Compaction**: Run periodically to reclaim space
4. **Snapshots**: Create for faster recovery

### C. Limitations

- Single-node version lacks distributed coordination
- No query language (use projections for read models)
- Eventual consistency in async replication mode

### D. Future Roadmap

- [ ] HTTP/REST API server
- [ ] GraphQL interface
- [ ] SQL-like query language
- [ ] Multi-datacenter replication
- [ ] Automatic partition rebalancing
- [ ] Streaming SQL (Flink-style)

## 🔗 Links

- [Documentation](https://github.com/moggan1337/FluxDB/wiki)
- [API Reference](https://github.com/moggan1337/FluxDB/wiki/API-Reference)
- [Examples](https://github.com/moggan1337/FluxDB/tree/main/examples)
- [Issue Tracker](https://github.com/moggan1337/FluxDB/issues)
- [Changelog](https://github.com/moggan1337/FluxDB/blob/main/CHANGELOG.md)
