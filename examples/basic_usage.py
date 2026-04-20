"""
Basic Usage Examples for FluxDB

This file demonstrates the core features of FluxDB.
"""

import time
import tempfile
import shutil

from src.core.storage import AppendOnlyStore, RecordType
from src.core.event_log import EventLog
from src.core.time_travel import TimeTravelStore
from src.event_sourcing.aggregates import Event, EventStore
from src.event_sourcing.projections import Projection, ProjectionManager
from src.crdt.registers import LWWRegister, ORSet
from src.crdt.counters import GCounter, PNCounter


def example_append_only_store():
    """Example: Using AppendOnlyStore"""
    print("\n" + "=" * 60)
    print("AppendOnlyStore Example")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create store
        store = AppendOnlyStore(f"{temp_dir}/store")
        
        # Store some data
        store.put("users:1", {"name": "Alice", "email": "alice@example.com"})
        store.put("users:2", {"name": "Bob", "email": "bob@example.com"})
        store.put("products:1", {"name": "Widget", "price": 29.99})
        
        print(f"\nStored {len(store)} keys")
        
        # Get data
        alice = store.get("users:1")
        print(f"\nUser 1: {alice.value}")
        
        # Update data (append-only, so creates new record)
        store.put("users:1", {"name": "Alice Smith", "email": "alice.smith@example.com"})
        
        # Get history
        history = store.get_history("users:1")
        print(f"\nUser 1 history ({len(history)} versions):")
        for record in history:
            print(f"  - {record.value} (seq: {record.sequence})")
        
        # Delete (soft delete with tombstone)
        store.delete("products:1")
        product = store.get("products:1")
        print(f"\nDeleted product record type: {product.record_type}")
        
        # Scan with prefix
        print("\nAll users:")
        for record in store.scan(prefix="users:"):
            print(f"  {record.key}: {record.value}")
        
        store.close()
        print("\n✓ AppendOnlyStore example complete")
        
    finally:
        shutil.rmtree(temp_dir)


def example_time_travel():
    """Example: Time-Travel Queries"""
    print("\n" + "=" * 60)
    print("Time-Travel Example")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        store = TimeTravelStore(f"{temp_dir}/timeless")
        
        # Create timeline of changes
        print("\nCreating timeline...")
        store.put("counter", 0)
        time.sleep(0.1)
        
        t1 = time.time()
        store.put("counter", 10)
        time.sleep(0.1)
        
        t2 = time.time()
        store.put("counter", 25)
        time.sleep(0.1)
        
        t3 = time.time()
        store.put("counter", 50)
        
        # Query at different points in time
        print(f"\nCounter at t1: {store.as_of('counter', timestamp=t1).value}")
        print(f"Counter at t2: {store.as_of('counter', timestamp=t2).value}")
        print(f"Counter at t3: {store.as_of('counter', timestamp=t3).value}")
        print(f"Counter now: {store.get('counter').value}")
        
        # Get full history
        print("\nFull history:")
        for entry in store.history("counter"):
            print(f"  {entry.value} (from {entry.valid_from:.2f} to {entry.valid_to})")
        
        # Diff between two times
        print(f"\nDiff between t1 and t3:")
        diffs = store.diff("counter", t1, t3)
        for d in diffs:
            print(f"  {d.change_type}: {d.old_value} -> {d.new_value}")
        
        store.close()
        print("\n✓ Time-Travel example complete")
        
    finally:
        shutil.rmtree(temp_dir)


def example_event_sourcing():
    """Example: Event Sourcing"""
    print("\n" + "=" * 60)
    print("Event Sourcing Example")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        store = EventStore(f"{temp_dir}/events")
        
        # Create events
        events = [
            Event(
                event_type="AccountOpened",
                aggregate_id="acc:1",
                metadata={"initial_balance": 100},
            ),
            Event(
                event_type="Deposit",
                aggregate_id="acc:1",
                metadata={"amount": 50, "new_balance": 150},
            ),
            Event(
                event_type="Withdrawal",
                aggregate_id="acc:1",
                metadata={"amount": 30, "new_balance": 120},
            ),
        ]
        
        # Append events
        store.append("acc:1", events)
        print(f"\nAppended {len(events)} events")
        
        # Retrieve events
        retrieved = store.get_events("acc:1")
        print(f"\nEvents for acc:1:")
        for e in retrieved:
            print(f"  - {e.event_type}: {e.metadata}")
        
        # Subscribe to events
        processed = []
        def handler(event):
            processed.append(event)
        
        store.subscribe("Deposit", handler)
        
        # Add more events
        new_events = [
            Event(
                event_type="Deposit",
                aggregate_id="acc:1",
                metadata={"amount": 100, "new_balance": 220},
            ),
        ]
        store.append("acc:1", new_events)
        
        print(f"\nProcessed events: {len(processed)}")
        
        store.close()
        print("\n✓ Event Sourcing example complete")
        
    finally:
        shutil.rmtree(temp_dir)


def example_event_log():
    """Example: Kafka-Compatible Event Log"""
    print("\n" + "=" * 60)
    print("Event Log Example")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        log = EventLog(f"{temp_dir}/eventlog")
        
        # Create topic
        log.create_topic("orders", partitions=3)
        print("\nCreated topic 'orders' with 3 partitions")
        
        # Produce events
        print("\nProducing events:")
        for i in range(5):
            event = log.produce(
                "orders",
                key=f"user:{i % 3}",
                value={"order_id": f"ord:{i}", "amount": 100 + i * 10},
            )
            print(f"  Produced: {event.event_id[:8]}... to {event.topic}:{event.partition}")
        
        # List topics
        print(f"\nTopics: {log.list_topics()}")
        
        # Create consumer
        consumer = log.consumer("order-processor", topics=["orders"])
        print("\nConsuming events:")
        
        for i, event in enumerate(consumer):
            print(f"  {event.event_type}: {event.value}")
            if i >= 2:
                break
        
        consumer.close()
        log.close()
        print("\n✓ Event Log example complete")
        
    finally:
        shutil.rmtree(temp_dir)


def example_crdt():
    """Example: CRDTs"""
    print("\n" + "=" * 60)
    print("CRDT Example")
    print("=" * 60)
    
    print("\n--- LWW-Register ---")
    reg1 = LWWRegister(node_id="node1")
    reg1.set("Hello")
    
    reg2 = LWWRegister(node_id="node2")
    reg2.set("World")
    
    print(f"Before merge: reg1={reg1.get()}, reg2={reg2.get()}")
    
    reg1.merge(reg2)
    print(f"After merge: reg1={reg1.get()}")
    
    print("\n--- OR-Set ---")
    set1 = ORSet(node_id="node1")
    set1.add("apple")
    set1.add("banana")
    
    set2 = ORSet(node_id="node2")
    set2.add("banana")
    set2.add("cherry")
    
    print(f"Before merge: set1={set1.get()}, set2={set2.get()}")
    
    set1.merge(set2)
    print(f"After merge: set1={set1.get()}")
    
    print("\n--- G-Counter ---")
    counter1 = GCounter()
    counter1.increment("node1", 5)
    counter1.increment("node2", 3)
    
    counter2 = GCounter()
    counter2.increment("node1", 2)
    counter2.increment("node3", 10)
    
    print(f"Before merge: c1={counter1.value()}, c2={counter2.value()}")
    
    counter1.merge(counter2)
    print(f"After merge: c1={counter1.value()}")
    
    print("\n--- PN-Counter ---")
    balance = PNCounter()
    balance.increment("node1", 100)
    balance.decrement("node1", 30)
    
    print(f"Balance: {balance.value()}")
    
    print("\n✓ CRDT example complete")


def example_projections():
    """Example: Projections"""
    print("\n" + "=" * 60)
    print("Projections Example")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create event store
        event_store = EventStore(f"{temp_dir}/events")
        
        # Define projection
        class UserProjection(Projection):
            name = "users"
            
            def init(self):
                self.users = {}
                self.count = 0
            
            def when(self, event):
                if event.event_type == "UserCreated":
                    self.users[event.aggregate_id] = {
                        "id": event.aggregate_id,
                        **event.payload,
                    }
                    self.count += 1
                elif event.event_type == "UserUpdated":
                    if event.aggregate_id in self.users:
                        self.users[event.aggregate_id].update(event.payload)
        
        # Create manager and register
        manager = ProjectionManager(event_store)
        user_proj = UserProjection()
        manager.register(user_proj)
        
        print("\nProjection registered: users")
        
        # Add some events
        events = [
            Event(event_type="UserCreated", aggregate_id="u1", payload={"name": "Alice", "email": "alice@example.com"}),
            Event(event_type="UserCreated", aggregate_id="u2", payload={"name": "Bob", "email": "bob@example.com"}),
            Event(event_type="UserUpdated", aggregate_id="u1", payload={"name": "Alice Smith"}),
        ]
        
        event_store.append("aggregate", events)
        
        # Process events
        for event in event_store.get_all_events():
            manager.process_event(event)
        
        print(f"\nProjection state:")
        print(f"  User count: {user_proj.count}")
        print(f"  Users: {user_proj.users}")
        
        manager.stop()
        event_store.close()
        print("\n✓ Projections example complete")
        
    finally:
        shutil.rmtree(temp_dir)


def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("FLUXDB EXAMPLES")
    print("=" * 60)
    
    example_append_only_store()
    example_time_travel()
    example_event_sourcing()
    example_event_log()
    example_crdt()
    example_projections()
    
    print("\n" + "=" * 60)
    print("ALL EXAMPLES COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
