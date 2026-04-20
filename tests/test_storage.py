"""
Tests for AppendOnlyStore
"""

import os
import shutil
import tempfile
import time
import pytest

from src.core.storage import AppendOnlyStore, Record, RecordType


class TestAppendOnlyStore:
    """Test cases for AppendOnlyStore"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests"""
        path = tempfile.mkdtemp()
        yield path
        shutil.rmtree(path)
    
    @pytest.fixture
    def store(self, temp_dir):
        """Create a store instance for testing"""
        return AppendOnlyStore(temp_dir)
    
    def test_put_and_get(self, store):
        """Test basic put and get operations"""
        store.put("key1", {"name": "Alice", "age": 30})
        
        record = store.get("key1")
        assert record is not None
        assert record.value == {"name": "Alice", "age": 30}
    
    def test_get_nonexistent(self, store):
        """Test getting a non-existent key"""
        record = store.get("nonexistent")
        assert record is None
    
    def test_put_multiple_values(self, store):
        """Test putting multiple values"""
        store.put("key1", "value1")
        store.put("key2", "value2")
        store.put("key3", "value3")
        
        assert store.get("key1").value == "value1"
        assert store.get("key2").value == "value2"
        assert store.get("key3").value == "value3"
    
    def test_update_value(self, store):
        """Test updating an existing value"""
        store.put("key1", "value1")
        store.put("key1", "value2")
        
        # Latest value
        assert store.get("key1").value == "value2"
    
    def test_get_history(self, store):
        """Test getting history of changes"""
        store.put("key1", "value1")
        time.sleep(0.01)
        store.put("key1", "value2")
        time.sleep(0.01)
        store.put("key1", "value3")
        
        history = store.get_history("key1")
        assert len(history) == 3
        assert history[0].value == "value1"
        assert history[1].value == "value2"
        assert history[2].value == "value3"
    
    def test_delete(self, store):
        """Test soft delete"""
        store.put("key1", "value1")
        store.delete("key1")
        
        record = store.get("key1")
        assert record is not None
        assert record.record_type == RecordType.TOMBSTONE
    
    def test_contains(self, store):
        """Test __contains__ method"""
        store.put("key1", "value1")
        
        assert "key1" in store
        assert "key2" not in store
    
    def test_len(self, store):
        """Test __len__ method"""
        store.put("key1", "value1")
        store.put("key2", "value2")
        store.put("key3", "value3")
        
        assert len(store) == 3
    
    def test_keys(self, store):
        """Test keys method"""
        store.put("key1", "value1")
        store.put("key2", "value2")
        
        keys = store.keys()
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys
    
    def test_scan(self, store):
        """Test scanning keys"""
        store.put("users:1", "Alice")
        store.put("users:2", "Bob")
        store.put("orders:1", "Order1")
        
        # Scan with prefix
        users = list(store.scan(prefix="users:"))
        assert len(users) == 2
        
        # Scan all
        all_keys = list(store.scan())
        assert len(all_keys) == 3
    
    def test_get_as_of(self, store):
        """Test point-in-time query"""
        store.put("key1", "value1")
        time.sleep(0.1)
        timestamp = time.time()
        store.put("key1", "value2")
        time.sleep(0.1)
        store.put("key1", "value3")
        
        # Get value at specific timestamp
        past_value = store.get_at("key1", timestamp)
        assert past_value.value == "value2"
    
    def test_stats(self, store):
        """Test statistics"""
        store.put("key1", "value1")
        store.put("key2", "value2")
        
        stats = store.get_stats()
        assert stats["total_records"] == 2
        assert stats["key_count"] == 2
    
    def test_record_verification(self, store):
        """Test record checksum verification"""
        store.put("key1", "value1")
        record = store.get("key1")
        
        assert record.verify()
    
    def test_context_manager(self, temp_dir):
        """Test context manager usage"""
        with AppendOnlyStore(temp_dir) as store:
            store.put("key1", "value1")
        
        # Store should be closed but data persisted
        with AppendOnlyStore(temp_dir) as store:
            assert store.get("key1").value == "value1"
    
    def test_concurrent_access(self, temp_dir):
        """Test concurrent access to store"""
        import threading
        
        store = AppendOnlyStore(temp_dir)
        results = []
        
        def write_thread(thread_id):
            for i in range(100):
                store.put(f"key{thread_id}:{i}", f"value{i}")
        
        threads = [threading.Thread(target=write_thread, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify all writes
        assert len(store) == 500


class TestRecord:
    """Test cases for Record"""
    
    def test_record_creation(self):
        """Test creating a record"""
        record = Record(
            key="test_key",
            value={"data": "test"},
            sequence=1,
            timestamp=time.time(),
            record_type=RecordType.DATA,
            checksum="",
        )
        
        # Compute checksum
        record_data = {
            "key": record.key,
            "value": record.value,
            "sequence": record.sequence,
            "timestamp": record.timestamp,
            "record_type": record.record_type.value,
            "headers": record.headers,
        }
        record.checksum = Record.compute_checksum(record_data)
        
        assert record.verify()
    
    def test_record_serialization(self):
        """Test record to/from bytes"""
        original = Record(
            key="test_key",
            value={"data": "test"},
            sequence=1,
            timestamp=time.time(),
            record_type=RecordType.DATA,
            checksum="test_checksum",
        )
        
        # Serialize
        data = original.to_bytes()
        
        # Deserialize
        restored = Record.from_bytes(data)
        
        assert restored.key == original.key
        assert restored.value == original.value
        assert restored.sequence == original.sequence


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
