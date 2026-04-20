"""
Tests for CRDT
"""

import pytest
import time

from src.crdt.registers import (
    LWWRegister, MVRegister, ORSet, TwoPhaseSet, LWWMap,
    Timestamp, VectorClock
)
from src.crdt.counters import (
    GCounter, PNCounter, GCounterMap, PNCounterMap
)


class TestTimestamp:
    """Test cases for Timestamp"""
    
    def test_timestamp_creation(self):
        """Test creating timestamps"""
        ts = Timestamp.now("node1")
        
        assert ts.node_id == "node1"
        assert ts.physical > 0
        assert ts.logical >= 0
    
    def test_timestamp_comparison(self):
        """Test timestamp comparison"""
        ts1 = Timestamp(physical=100, logical=0, node_id="a")
        ts2 = Timestamp(physical=101, logical=0, node_id="b")
        ts3 = Timestamp(physical=100, logical=1, node_id="a")
        
        assert ts1 < ts2
        assert ts2 > ts1
        assert ts1 < ts3  # Same physical, ts1 logical < ts3 logical
    
    def test_timestamp_equality(self):
        """Test timestamp equality"""
        ts1 = Timestamp(physical=100, logical=5, node_id="a")
        ts2 = Timestamp(physical=100, logical=5, node_id="a")
        
        assert ts1 == ts2


class TestVectorClock:
    """Test cases for VectorClock"""
    
    def test_increment(self):
        """Test incrementing vector clock"""
        vc = VectorClock()
        vc.increment("node1")
        vc.increment("node1")
        vc.increment("node2")
        
        assert vc.get("node1") == 2
        assert vc.get("node2") == 1
        assert vc.get("node3") == 0
    
    def test_merge(self):
        """Test merging vector clocks"""
        vc1 = VectorClock({"node1": 2, "node2": 1})
        vc2 = VectorClock({"node1": 1, "node3": 3})
        
        vc1.merge(vc2)
        
        assert vc1.get("node1") == 2
        assert vc1.get("node2") == 1
        assert vc1.get("node3") == 3
    
    def test_happens_before(self):
        """Test happens-before relation"""
        vc1 = VectorClock({"node1": 1, "node2": 0})
        vc2 = VectorClock({"node1": 2, "node2": 1})
        
        assert vc1.happens_before(vc2)
        assert not vc2.happens_before(vc1)


class TestLWWRegister:
    """Test cases for LWWRegister"""
    
    def test_set_and_get(self):
        """Test basic set and get"""
        reg = LWWRegister(node_id="node1")
        reg.set("value1")
        
        assert reg.get() == "value1"
    
    def test_update(self):
        """Test updating value"""
        reg = LWWRegister(node_id="node1")
        reg.set("value1", Timestamp(100, 0, "node1"))
        reg.set("value2", Timestamp(101, 0, "node1"))
        
        assert reg.get() == "value2"
    
    def test_merge(self):
        """Test merging two LWW registers"""
        reg1 = LWWRegister(node_id="node1")
        reg1.set("value1", Timestamp(100, 0, "node1"))
        
        reg2 = LWWRegister(node_id="node2")
        reg2.set("value2", Timestamp(101, 0, "node2"))
        
        reg1.merge(reg2)
        assert reg1.get() == "value2"
    
    def test_merge_later_timestamp_wins(self):
        """Test that later timestamp wins in merge"""
        reg1 = LWWRegister(node_id="node1")
        reg1.set("value1", Timestamp(100, 0, "node1"))
        
        reg2 = LWWRegister(node_id="node2")
        reg2.set("value2", Timestamp(50, 0, "node2"))  # Earlier
        
        reg1.merge(reg2)
        assert reg1.get() == "value1"
    
    def test_delete(self):
        """Test deleting a register"""
        reg = LWWRegister(node_id="node1")
        reg.set("value1")
        reg.delete()
        
        assert reg.get() is None


class TestORSet:
    """Test cases for OR-Set"""
    
    def test_add(self):
        """Test adding elements"""
        s = ORSet(node_id="node1")
        s.add("item1")
        s.add("item2")
        
        assert "item1" in s.get()
        assert "item2" in s.get()
    
    def test_remove(self):
        """Test removing elements"""
        s = ORSet(node_id="node1")
        s.add("item1")
        s.add("item2")
        s.remove("item1")
        
        result = s.get()
        assert "item1" not in result
        assert "item2" in result
    
    def test_merge(self):
        """Test merging OR-Sets"""
        s1 = ORSet(node_id="node1")
        s1.add("item1")
        s1.add("item2")
        
        s2 = ORSet(node_id="node2")
        s2.add("item2")
        s2.add("item3")
        
        s1.merge(s2)
        
        result = s1.get()
        assert "item1" in result
        assert "item2" in result
        assert "item3" in result
    
    def test_concurrent_add_remove(self):
        """Test concurrent add and remove"""
        s1 = ORSet(node_id="node1")
        s1.add("item1")
        
        s2 = ORSet(node_id="node2")
        s2.add("item1")
        
        # Both add, then one removes
        s1.merge(s2)
        s1.remove("item1")
        
        assert "item1" not in s1.get()


class TestTwoPhaseSet:
    """Test cases for Two-Phase Set"""
    
    def test_add_and_remove(self):
        """Test add and remove"""
        s = TwoPhaseSet()
        s.add("item1")
        s.add("item2")
        s.remove("item1")
        
        assert "item1" not in s.get()
        assert "item2" in s.get()
    
    def test_cannot_readd_removed(self):
        """Test that removed items cannot be re-added"""
        s = TwoPhaseSet()
        s.add("item1")
        s.remove("item1")
        s.add("item1")  # Should have no effect
        
        assert "item1" not in s.get()


class TestGCounter:
    """Test cases for G-Counter"""
    
    def test_increment(self):
        """Test incrementing counter"""
        counter = GCounter()
        counter.increment("node1")
        counter.increment("node1")
        counter.increment("node2")
        
        assert counter.value() == 3
    
    def test_merge(self):
        """Test merging counters"""
        c1 = GCounter({"node1": 2})
        c2 = GCounter({"node1": 1, "node2": 3})
        
        c1.merge(c2)
        
        assert c1.value() == 5
        assert c1.get_node_value("node1") == 2
        assert c1.get_node_value("node2") == 3
    
    def test_max_merge(self):
        """Test that merge takes maximum"""
        c1 = GCounter({"node1": 5})
        c2 = GCounter({"node1": 3})  # Lower value
        
        c1.merge(c2)
        
        assert c1.get_node_value("node1") == 5


class TestPNCounter:
    """Test cases for PN-Counter"""
    
    def test_increment_decrement(self):
        """Test increment and decrement"""
        counter = PNCounter()
        counter.increment("node1", 10)
        counter.decrement("node1", 3)
        
        assert counter.value() == 7
    
    def test_multiple_nodes(self):
        """Test with multiple nodes"""
        counter = PNCounter()
        counter.increment("node1", 5)
        counter.increment("node2", 3)
        counter.decrement("node1", 2)
        
        assert counter.value() == 6
    
    def test_merge(self):
        """Test merging counters"""
        c1 = PNCounter({"node1": {"node1": 5}}, {"node1": {}})
        c2 = PNCounter({"node2": {"node2": 3}}, {"node2": {}})
        
        c1.merge(c2)
        
        assert c1.value() == 8


class TestLWWMap:
    """Test cases for LWW-Map"""
    
    def test_put_and_get(self):
        """Test put and get"""
        m = LWWMap(node_id="node1")
        m.put("name", "Alice")
        m.put("age", 30)
        
        assert m.get("name") == "Alice"
        assert m.get("age") == 30
    
    def test_keys(self):
        """Test getting keys"""
        m = LWWMap(node_id="node1")
        m.put("key1", "value1")
        m.put("key2", "value2")
        m.remove("key1")
        
        keys = m.keys()
        assert "key1" not in keys
        assert "key2" in keys


class TestSerialization:
    """Test CRDT serialization"""
    
    def test_lww_register_serialization(self):
        """Test LWWRegister to/from dict"""
        reg1 = LWWRegister(node_id="node1")
        reg1.set("value1")
        
        data = reg1.to_dict()
        reg2 = LWWRegister.from_dict(data)
        
        assert reg2.get() == "value1"
        assert reg2.node_id == "node1"
    
    def test_or_set_serialization(self):
        """Test ORSet to/from dict"""
        s1 = ORSet(node_id="node1")
        s1.add("item1")
        s1.add("item2")
        
        data = s1.to_dict()
        s2 = ORSet.from_dict(data)
        
        assert s2.get() == {"item1", "item2"}
    
    def test_gcounter_serialization(self):
        """Test GCounter to/from dict"""
        c1 = GCounter({"node1": 5, "node2": 3})
        
        data = c1.to_dict()
        c2 = GCounter.from_dict(data)
        
        assert c2.value() == 8


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
