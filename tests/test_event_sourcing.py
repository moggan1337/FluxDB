"""
Tests for Event Sourcing
"""

import tempfile
import shutil
import pytest
import time

from src.event_sourcing.aggregates import (
    Event, AggregateRoot, EventStore, AggregateFactory, register_aggregate
)
from src.event_sourcing.projections import (
    Projection, ProjectionManager, ReadModelProjection
)


class TestEvent:
    """Test cases for Event"""
    
    def test_event_creation(self):
        """Test creating an event"""
        event = Event(
            event_type="UserCreated",
            aggregate_id="user:1",
            aggregate_type="User",
            metadata={"name": "Alice"},
        )
        
        assert event.event_type == "UserCreated"
        assert event.aggregate_id == "user:1"
        assert event.metadata["name"] == "Alice"
    
    def test_event_serialization(self):
        """Test event to/from dict"""
        event = Event(
            event_type="TestEvent",
            aggregate_id="test:1",
            metadata={"key": "value"},
        )
        
        data = event.to_dict()
        restored = Event.from_dict(data)
        
        assert restored.event_type == event.event_type
        assert restored.aggregate_id == event.aggregate_id


class TestAggregateRoot:
    """Test cases for AggregateRoot"""
    
    def test_aggregate_creation(self):
        """Test creating an aggregate"""
        class TestAggregate(AggregateRoot):
            def __init__(self, id):
                super().__init__(id)
                self.value = None
            
            def apply(self, event):
                if event.event_type == "ValueSet":
                    self.value = event.metadata["value"]
            
            def set_value(self, value):
                self._add_event(Event(
                    event_type="ValueSet",
                    metadata={"value": value},
                ))
            
            def _get_state(self):
                return {"value": self.value}
        
        aggregate = TestAggregate("test:1")
        assert aggregate.aggregate_id == "test:1"
        assert aggregate.is_new
        
        aggregate.set_value("test")
        events = aggregate.pull_events()
        
        assert len(events) == 1
        assert events[0].event_type == "ValueSet"
        assert aggregate.value == "test"
    
    def test_event_replay(self):
        """Test replaying events"""
        class TestAggregate(AggregateRoot):
            def __init__(self, id):
                super().__init__(id)
                self.count = 0
            
            def apply(self, event):
                if event.event_type == "Incremented":
                    self.count += 1
            
            def increment(self):
                self._add_event(Event(event_type="Incremented"))
            
            def _get_state(self):
                return {"count": self.count}
        
        aggregate = TestAggregate("test:1")
        
        # Apply some events
        aggregate.increment()
        aggregate.increment()
        aggregate.increment()
        aggregate.pull_events()  # Clear pending
        
        # Create new aggregate and replay
        new_aggregate = TestAggregate("test:1")
        events = aggregate.pull_events()
        new_aggregate.replay(events)
        
        assert new_aggregate.count == 3


class TestProjection:
    """Test cases for Projection"""
    
    def test_projection_init(self):
        """Test projection initialization"""
        class TestProjection(Projection):
            name = "test"
            
            def init(self):
                self.data = {}
            
            def when(self, event):
                if event.event_type == "ItemAdded":
                    self.data[event.aggregate_id] = event.payload
        
        proj = TestProjection()
        proj.init()
        
        assert proj.name == "test"
        assert proj.data == {}
    
    def test_projection_event_handling(self):
        """Test projection event handling"""
        class UserProjection(Projection):
            name = "users"
            
            def init(self):
                self.users = {}
            
            def when(self, event):
                if event.event_type == "UserCreated":
                    self.users[event.aggregate_id] = event.payload
        
        proj = UserProjection()
        proj.init()
        
        # Create and process event
        event = Event(
            event_type="UserCreated",
            aggregate_id="user:1",
            metadata={},
        )
        event.payload = {"name": "Alice"}
        
        proj.when(event)
        
        assert "user:1" in proj.users


class TestProjectionManager:
    """Test cases for ProjectionManager"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        path = tempfile.mkdtemp()
        yield path
        shutil.rmtree(path)
    
    @pytest.fixture
    def event_store(self, temp_dir):
        """Create event store"""
        return EventStore(f"{temp_dir}/events")
    
    def test_register_projection(self, event_store):
        """Test registering a projection"""
        class TestProjection(Projection):
            name = "test"
            
            def init(self):
                self.count = 0
            
            def when(self, event):
                self.count += 1
        
        manager = ProjectionManager(event_store)
        proj = TestProjection()
        manager.register(proj)
        
        assert "test" in manager.list_projections()
        assert manager.get_projection("test") == proj
    
    def test_list_projections(self, event_store):
        """Test listing projections"""
        class Proj1(Projection):
            name = "proj1"
            def init(self): pass
            def when(self, e): pass
        
        class Proj2(Projection):
            name = "proj2"
            def init(self): pass
            def when(self, e): pass
        
        manager = ProjectionManager(event_store)
        manager.register(Proj1())
        manager.register(Proj2())
        
        names = manager.list_projections()
        assert "proj1" in names
        assert "proj2" in names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
