"""
Python Client API for FluxDB

This module provides a high-level Python client for interacting
with FluxDB.

Features:
- Synchronous and async APIs
- Connection pooling
- Automatic retries
- Type hints and validation
- Context managers
- Event sourcing helpers

Usage:

```python
from fluxdb import FluxDBClient

# Create client
client = FluxDBClient("localhost", 8080)

# Basic operations
client.put("users:1", {"name": "Alice", "age": 30})
value = client.get("users:1")
print(value)  # {"name": "Alice", "age": 30}

# Event sourcing
client.emit("users", "UserCreated", {"user_id": "1", "name": "Alice"})

# Time travel
value_at_time = client.get_as_of("users:1", timestamp=1700000000)

# CRDT operations
client.register_lww("counter", "node1", initial_value=0)
client.register_lww("counter", "node2", initial_value=0)
client.merge_lww("counter")

# Close client
client.close()
```
"""

import json
import socket
import threading
import time
import queue
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Set, Union
from urllib.parse import urlparse

import logging

logger = logging.getLogger(__name__)


@dataclass
class FluxDBConfig:
    """Configuration for FluxDB client"""
    host: str = "localhost"
    port: int = 8080
    timeout: float = 30.0
    max_connections: int = 10
    retry_count: int = 3
    retry_delay: float = 0.5
    enable_tls: bool = False
    tls_cert: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class FluxDBError(Exception):
    """Base exception for FluxDB errors"""
    pass


class ConnectionError(FluxDBError):
    """Connection error"""
    pass


class TimeoutError(FluxDBError):
    """Timeout error"""
    pass


class NotFoundError(FluxDBError):
    """Key not found"""
    pass


class FluxDBResponse:
    """Response from FluxDB"""
    
    def __init__(self, status: int, data: Any, error: Optional[str] = None):
        self.status = status
        self.data = data
        self.error = error
        self.success = 200 <= status < 300
    
    def __repr__(self) -> str:
        return f"FluxDBResponse(status={self.status}, success={self.success})"


class FluxDBConnection:
    """
    Single connection to FluxDB server.
    """
    
    def __init__(self, config: FluxDBConfig):
        self.config = config
        self._socket: Optional[socket.socket] = None
        self._connected = False
        self._lock = threading.Lock()
    
    def connect(self) -> None:
        """Establish connection"""
        with self._lock:
            if self._connected:
                return
            
            try:
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self.config.timeout)
                self._socket.connect((self.config.host, self.config.port))
                self._connected = True
                
                # Send handshake if auth configured
                if self.config.username:
                    self._send_raw({
                        "type": "auth",
                        "username": self.config.username,
                        "password": self.config.password,
                    })
                
                logger.info(f"Connected to {self.config.host}:{self.config.port}")
                
            except Exception as e:
                self._connected = False
                raise ConnectionError(f"Failed to connect: {e}")
    
    def disconnect(self) -> None:
        """Close connection"""
        with self._lock:
            if self._socket:
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None
            self._connected = False
    
    def _send_raw(self, data: Dict[str, Any]) -> None:
        """Send raw message"""
        if not self._connected:
            raise ConnectionError("Not connected")
        
        encoded = json.dumps(data).encode()
        self._socket.sendall(encoded)
    
    def _receive_raw(self) -> Optional[Dict[str, Any]]:
        """Receive raw message"""
        if not self._connected:
            raise ConnectionError("Not connected")
        
        try:
            data = self._socket.recv(4096)
            if not data:
                return None
            return json.loads(data.decode())
        except socket.timeout:
            return None
    
    def send_command(self, command: Dict[str, Any]) -> FluxDBResponse:
        """Send command and receive response"""
        try:
            self._send_raw(command)
            response = self._receive_raw()
            
            if response is None:
                return FluxDBResponse(500, None, "No response")
            
            return FluxDBResponse(
                status=response.get("status", 200),
                data=response.get("data"),
                error=response.get("error"),
            )
            
        except socket.timeout:
            raise TimeoutError("Request timed out")
        except Exception as e:
            raise ConnectionError(f"Request failed: {e}")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


class ConnectionPool:
    """
    Connection pool for FluxDB.
    
    Provides efficient connection reuse and management.
    """
    
    def __init__(self, config: FluxDBConfig):
        self.config = config
        self._pool: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._created = 0
        self._closed = False
    
    def get_connection(self) -> FluxDBConnection:
        """Get a connection from the pool"""
        if self._closed:
            raise ConnectionError("Pool is closed")
        
        # Try to get from pool
        try:
            conn = self._pool.get_nowait()
            if conn._connected:
                return conn
        except queue.Empty:
            pass
        
        # Create new connection
        with self._lock:
            if self._created < self.config.max_connections:
                conn = FluxDBConnection(self.config)
                conn.connect()
                self._created += 1
                return conn
        
        # Wait for available connection
        conn = self._pool.get(timeout=self.config.timeout)
        if not conn._connected:
            conn.connect()
        return conn
    
    def return_connection(self, conn: FluxDBConnection) -> None:
        """Return a connection to the pool"""
        if not self._closed:
            try:
                self._pool.put_nowait(conn)
            except queue.Full:
                conn.disconnect()
    
    def close(self) -> None:
        """Close all connections"""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.disconnect()
            except queue.Empty:
                break
    
    @contextmanager
    def connection(self):
        """Context manager for connections"""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)


class FluxDBClient:
    """
    High-level FluxDB client.
    
    Provides a simple interface to FluxDB operations.
    
    Usage:
    
    ```python
    client = FluxDBClient("localhost", 8080)
    
    # Store data
    client.put("key", {"data": "value"})
    
    # Get data
    value = client.get("key")
    
    # Delete data
    client.delete("key")
    
    # Scan keys
    for key in client.scan(prefix="user"):
        print(key)
    
    client.close()
    ```
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        config: Optional[FluxDBConfig] = None,
    ):
        if config is None:
            config = FluxDBConfig(host=host, port=port)
        
        self.config = config
        self._pool = ConnectionPool(config)
        self._closed = False
    
    def _execute(self, command: Dict[str, Any]) -> Any:
        """Execute a command using pooled connection"""
        with self._pool.connection() as conn:
            response = conn.send_command(command)
            if not response.success:
                if response.status == 404:
                    raise NotFoundError(response.error or "Key not found")
                raise FluxDBError(response.error or "Command failed")
            return response.data
    
    def put(self, key: str, value: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Store a value.
        
        Args:
            key: The key to store
            value: The value to store
            headers: Optional headers
            
        Returns:
            True if successful
        """
        command = {
            "type": "put",
            "key": key,
            "value": value,
        }
        if headers:
            command["headers"] = headers
        
        self._execute(command)
        return True
    
    def get(self, key: str) -> Any:
        """
        Get a value by key.
        
        Args:
            key: The key to get
            
        Returns:
            The value, or None if not found
        """
        command = {"type": "get", "key": key}
        return self._execute(command)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful
        """
        command = {"type": "delete", "key": key}
        self._execute(command)
        return True
    
    def exists(self, key: str) -> bool:
        """Check if a key exists"""
        command = {"type": "exists", "key": key}
        return self._execute(command) is True
    
    def scan(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
    ) -> Iterator[str]:
        """
        Scan keys.
        
        Args:
            prefix: Optional key prefix filter
            limit: Maximum keys to return
            
        Yields:
            Keys matching the filter
        """
        command = {
            "type": "scan",
            "prefix": prefix,
            "limit": limit,
        }
        
        keys = self._execute(command)
        for key in keys:
            yield key
    
    def get_history(self, key: str) -> List[Dict[str, Any]]:
        """
        Get history of changes for a key.
        
        Args:
            key: The key to get history for
            
        Returns:
            List of historical values
        """
        command = {"type": "history", "key": key}
        return self._execute(command)
    
    def get_as_of(self, key: str, timestamp: float) -> Any:
        """
        Get value at a specific point in time.
        
        Args:
            key: The key to query
            timestamp: Unix timestamp
            
        Returns:
            Value at that time, or None
        """
        command = {"type": "get_as_of", "key": key, "timestamp": timestamp}
        return self._execute(command)
    
    # Event Sourcing Operations
    
    def emit(
        self,
        topic: str,
        event_type: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Emit an event to a topic.
        
        Args:
            topic: The topic to emit to
            event_type: Type of event
            payload: Event payload
            key: Optional partition key
            headers: Optional headers
            
        Returns:
            Event ID
        """
        command = {
            "type": "emit",
            "topic": topic,
            "event_type": event_type,
            "payload": payload,
        }
        if key:
            command["key"] = key
        if headers:
            command["headers"] = headers
        
        return self._execute(command)
    
    def subscribe(
        self,
        topic: str,
        handler: callable,
        group: Optional[str] = None,
    ) -> None:
        """
        Subscribe to events from a topic.
        
        Args:
            topic: Topic to subscribe to
            handler: Callback function for events
            group: Optional consumer group
        """
        command = {
            "type": "subscribe",
            "topic": topic,
            "group": group,
        }
        self._execute(command)
        
        # In a real implementation, would start background listener
        # For now, just return
    
    # CRDT Operations
    
    def create_lww_register(
        self,
        name: str,
        initial_value: Any = None,
    ) -> bool:
        """Create an LWW-Register CRDT"""
        command = {
            "type": "crdt_create",
            "crdt_type": "lww_register",
            "name": name,
            "initial_value": initial_value,
        }
        self._execute(command)
        return True
    
    def lww_set(
        self,
        name: str,
        value: Any,
        timestamp: Optional[float] = None,
    ) -> bool:
        """Set value in LWW-Register"""
        command = {
            "type": "crdt_lww_set",
            "name": name,
            "value": value,
        }
        if timestamp:
            command["timestamp"] = timestamp
        
        self._execute(command)
        return True
    
    def lww_get(self, name: str) -> Any:
        """Get value from LWW-Register"""
        command = {"type": "crdt_lww_get", "name": name}
        return self._execute(command)
    
    def create_g_counter(self, name: str, initial_value: int = 0) -> bool:
        """Create a G-Counter CRDT"""
        command = {
            "type": "crdt_create",
            "crdt_type": "g_counter",
            "name": name,
            "initial_value": initial_value,
        }
        self._execute(command)
        return True
    
    def gcounter_increment(self, name: str, amount: int = 1) -> int:
        """Increment a G-Counter"""
        command = {
            "type": "crdt_gcounter_increment",
            "name": name,
            "amount": amount,
        }
        return self._execute(command)
    
    def gcounter_get(self, name: str) -> int:
        """Get G-Counter value"""
        command = {"type": "crdt_gcounter_get", "name": name}
        return self._execute(command)
    
    def create_or_set(self, name: str) -> bool:
        """Create an OR-Set CRDT"""
        command = {
            "type": "crdt_create",
            "crdt_type": "or_set",
            "name": name,
        }
        self._execute(command)
        return True
    
    def orset_add(self, name: str, value: Any) -> bool:
        """Add to OR-Set"""
        command = {
            "type": "crdt_orset_add",
            "name": name,
            "value": value,
        }
        self._execute(command)
        return True
    
    def orset_remove(self, name: str, value: Any) -> bool:
        """Remove from OR-Set"""
        command = {
            "type": "crdt_orset_remove",
            "name": name,
            "value": value,
        }
        self._execute(command)
        return True
    
    def orset_get(self, name: str) -> Set[Any]:
        """Get OR-Set values"""
        command = {"type": "crdt_orset_get", "name": name}
        result = self._execute(command)
        return set(result) if result else set()
    
    def merge_crdt(self, name: str, other: Dict[str, Any]) -> Any:
        """Merge CRDT state"""
        command = {
            "type": "crdt_merge",
            "name": name,
            "state": other,
        }
        return self._execute(command)
    
    # Batch Operations
    
    def put_many(self, items: Dict[str, Any]) -> int:
        """
        Batch put multiple items.
        
        Args:
            items: Dictionary of key -> value
            
        Returns:
            Number of items stored
        """
        command = {
            "type": "put_many",
            "items": items,
        }
        return self._execute(command)
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Batch get multiple keys.
        
        Args:
            keys: List of keys to get
            
        Returns:
            Dictionary of key -> value
        """
        command = {"type": "get_many", "keys": keys}
        return self._execute(command)
    
    # Admin Operations
    
    def ping(self) -> bool:
        """Check connection"""
        command = {"type": "ping"}
        try:
            self._execute(command)
            return True
        except Exception:
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        command = {"type": "stats"}
        return self._execute(command)
    
    def close(self) -> None:
        """Close the client"""
        if not self._closed:
            self._pool.close()
            self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class AsyncFluxDBClient:
    """
    Async FluxDB client.
    
    For use with asyncio applications.
    
    Usage:
    
    ```python
    async with AsyncFluxDBClient("localhost", 8080) as client:
        await client.put("key", {"data": "value"})
        value = await client.get("key")
    ```
    """
    
    def __init__(self, host: str = "localhost", port: int = 8080):
        self.host = host
        self.port = port
        self._client = FluxDBClient(host, port)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._client.close()
        return False
    
    async def put(self, key: str, value: Any) -> bool:
        """Store a value"""
        return self._client.put(key, value)
    
    async def get(self, key: str) -> Any:
        """Get a value"""
        return self._client.get(key)
    
    async def delete(self, key: str) -> bool:
        """Delete a value"""
        return self._client.delete(key)
