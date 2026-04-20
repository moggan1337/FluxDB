"""
Event Sourcing Advanced Example

This demonstrates complex event sourcing patterns including:
- Aggregates with business logic
- Event handlers
- Sagas
- Snapshots
"""

import time
import uuid
import tempfile
import shutil
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from src.event_sourcing.aggregates import (
    Event, AggregateRoot, EventStore, Command, CommandResult
)
from src.event_sourcing.projections import Projection, ProjectionManager


@dataclass
class OrderEvent(Event):
    """Base class for order events"""
    pass


@dataclass
class OrderCreated(OrderEvent):
    """Order was created"""
    def __init__(self, aggregate_id: str, customer_id: str, items: List[Dict]):
        super().__init__()
        self.event_type = "OrderCreated"
        self.aggregate_id = aggregate_id
        self.payload = {
            "customer_id": customer_id,
            "items": items,
            "status": "pending",
        }


@dataclass
class OrderConfirmed(OrderEvent):
    """Order was confirmed"""
    def __init__(self, aggregate_id: str):
        super().__init__()
        self.event_type = "OrderConfirmed"
        self.aggregate_id = aggregate_id
        self.payload = {"status": "confirmed"}


@dataclass
class OrderShipped(OrderEvent):
    """Order was shipped"""
    def __init__(self, aggregate_id: str, tracking_number: str):
        super().__init__()
        self.event_type = "OrderShipped"
        self.aggregate_id = aggregate_id
        self.payload = {
            "status": "shipped",
            "tracking_number": tracking_number,
        }


@dataclass
class OrderDelivered(OrderEvent):
    """Order was delivered"""
    def __init__(self, aggregate_id: str):
        super().__init__()
        self.event_type = "OrderDelivered"
        self.aggregate_id = aggregate_id
        self.payload = {"status": "delivered"}


@dataclass
class OrderCancelled(OrderEvent):
    """Order was cancelled"""
    def __init__(self, aggregate_id: str, reason: str):
        super().__init__()
        self.event_type = "OrderCancelled"
        self.aggregate_id = aggregate_id
        self.payload = {
            "status": "cancelled",
            "reason": reason,
        }


class Order(AggregateRoot):
    """
    Order aggregate root.
    
    Encapsulates order business logic and state transitions.
    """
    
    def __init__(self, order_id: str):
        super().__init__(order_id)
        self.customer_id: Optional[str] = None
        self.items: List[Dict] = []
        self.status: str = "none"
        self.tracking_number: Optional[str] = None
        self.total: float = 0.0
    
    def create_order(self, customer_id: str, items: List[Dict]) -> CommandResult:
        """Create a new order"""
        if self.status != "none":
            return CommandResult.fail("Order already exists")
        
        if not items:
            return CommandResult.fail("Order must have items")
        
        # Calculate total
        self.total = sum(item.get("price", 0) * item.get("quantity", 1) for item in items)
        
        # Create order
        self.customer_id = customer_id
        self.items = items
        self.status = "pending"
        
        self._add_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            items=items,
        ))
        
        return CommandResult.ok(events=self.pull_events(), version=self.version)
    
    def confirm(self) -> CommandResult:
        """Confirm the order"""
        if self.status != "pending":
            return CommandResult.fail(f"Cannot confirm order in {self.status} status")
        
        self.status = "confirmed"
        self._add_event(OrderConfirmed(aggregate_id=self.aggregate_id))
        
        return CommandResult.ok(events=self.pull_events(), version=self.version)
    
    def ship(self, tracking_number: str) -> CommandResult:
        """Ship the order"""
        if self.status != "confirmed":
            return CommandResult.fail(f"Cannot ship order in {self.status} status")
        
        self.status = "shipped"
        self.tracking_number = tracking_number
        self._add_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
        ))
        
        return CommandResult.ok(events=self.pull_events(), version=self.version)
    
    def deliver(self) -> CommandResult:
        """Mark order as delivered"""
        if self.status != "shipped":
            return CommandResult.fail(f"Cannot deliver order in {self.status} status")
        
        self.status = "delivered"
        self._add_event(OrderDelivered(aggregate_id=self.aggregate_id))
        
        return CommandResult.ok(events=self.pull_events(), version=self.version)
    
    def cancel(self, reason: str) -> CommandResult:
        """Cancel the order"""
        if self.status in ("shipped", "delivered"):
            return CommandResult.fail(f"Cannot cancel order in {self.status} status")
        
        self.status = "cancelled"
        self._add_event(OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
        ))
        
        return CommandResult.ok(events=self.pull_events(), version=self.version)
    
    def apply(self, event: Event) -> None:
        """Apply event to reconstruct state"""
        if event.event_type == "OrderCreated":
            self.customer_id = event.payload["customer_id"]
            self.items = event.payload["items"]
            self.status = event.payload["status"]
            self.total = sum(
                item.get("price", 0) * item.get("quantity", 1)
                for item in self.items
            )
        elif event.event_type == "OrderConfirmed":
            self.status = event.payload["status"]
        elif event.event_type == "OrderShipped":
            self.status = event.payload["status"]
            self.tracking_number = event.payload["tracking_number"]
        elif event.event_type == "OrderDelivered":
            self.status = event.payload["status"]
        elif event.event_type == "OrderCancelled":
            self.status = event.payload["status"]
    
    def _get_state(self) -> Dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "items": self.items,
            "status": self.status,
            "tracking_number": self.tracking_number,
            "total": self.total,
        }


class OrderProjection(Projection):
    """Projection for orders"""
    
    name = "orders"
    
    def init(self):
        self.orders: Dict[str, Dict] = {}
        self.by_status: Dict[str, List[str]] = {}
        self.by_customer: Dict[str, List[str]] = {}
        self.total_revenue = 0.0
    
    def when(self, event: Event):
        if event.event_type == "OrderCreated":
            order_id = event.aggregate_id
            self.orders[order_id] = {
                "id": order_id,
                "customer_id": event.payload["customer_id"],
                "items": event.payload["items"],
                "status": event.payload["status"],
                "total": sum(
                    item.get("price", 0) * item.get("quantity", 1)
                    for item in event.payload["items"]
                ),
            }
            
            # Index by status
            status = event.payload["status"]
            if status not in self.by_status:
                self.by_status[status] = []
            self.by_status[status].append(order_id)
            
            # Index by customer
            customer = event.payload["customer_id"]
            if customer not in self.by_customer:
                self.by_customer[customer] = []
            self.by_customer[customer].append(order_id)
            
        elif event.event_type in ("OrderConfirmed", "OrderShipped", "OrderDelivered", "OrderCancelled"):
            order_id = event.aggregate_id
            if order_id in self.orders:
                old_status = self.orders[order_id]["status"]
                new_status = event.payload["status"]
                
                # Update status index
                if old_status in self.by_status:
                    if order_id in self.by_status[old_status]:
                        self.by_status[old_status].remove(order_id)
                
                if new_status not in self.by_status:
                    self.by_status[new_status] = []
                self.by_status[new_status].append(order_id)
                
                # Update order
                self.orders[order_id]["status"] = new_status
                
                # Track revenue
                if new_status == "delivered":
                    self.total_revenue += self.orders[order_id]["total"]


def main():
    """Run event sourcing example"""
    print("\n" + "=" * 60)
    print("EVENT SOURCING - ORDER MANAGEMENT")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create event store
        event_store = EventStore(f"{temp_dir}/events")
        
        # Create projection manager
        manager = ProjectionManager(event_store)
        order_proj = OrderProjection()
        manager.register(order_proj)
        
        # Create orders
        order = Order("order:1")
        
        print("\n1. Creating order...")
        result = order.create_order(
            customer_id="customer:1",
            items=[
                {"product": "Widget", "price": 29.99, "quantity": 2},
                {"product": "Gadget", "price": 49.99, "quantity": 1},
            ],
        )
        print(f"   Order created: {result.success}")
        
        # Append events
        event_store.append("order:1", result.events)
        
        # Process through projection
        for event in result.events:
            manager.process_event(event)
        
        print(f"   Order total: ${order.total:.2f}")
        
        print("\n2. Confirming order...")
        result = order.confirm()
        event_store.append("order:1", result.events)
        for event in result.events:
            manager.process_event(event)
        print(f"   Order confirmed: {result.success}")
        
        print("\n3. Shipping order...")
        result = order.ship("TRACK123456")
        event_store.append("order:1", result.events)
        for event in result.events:
            manager.process_event(event)
        print(f"   Order shipped: {result.success}")
        print(f"   Tracking: {order.tracking_number}")
        
        print("\n4. Delivering order...")
        result = order.deliver()
        event_store.append("order:1", result.events)
        for event in result.events:
            manager.process_event(event)
        print(f"   Order delivered: {result.success}")
        
        print("\n5. Projection state:")
        print(f"   Total orders: {len(order_proj.orders)}")
        print(f"   By status: {order_proj.by_status}")
        print(f"   Total revenue: ${order_proj.total_revenue:.2f}")
        
        # Create second order
        print("\n6. Creating second order (will be cancelled)...")
        order2 = Order("order:2")
        result = order2.create_order(
            customer_id="customer:2",
            items=[
                {"product": "Service", "price": 199.99, "quantity": 1},
            ],
        )
        event_store.append("order:2", result.events)
        for event in result.events:
            manager.process_event(event)
        
        result = order2.cancel("Customer request")
        event_store.append("order:2", result.events)
        for event in result.events:
            manager.process_event(event)
        
        print(f"   Order cancelled: {result.success}")
        
        print("\n7. Final projection state:")
        print(f"   Total orders: {len(order_proj.orders)}")
        print(f"   By status: {order_proj.by_status}")
        print(f"   Revenue: ${order_proj.total_revenue:.2f}")
        
        manager.stop()
        event_store.close()
        
        print("\n✓ Event sourcing example complete")
        
    finally:
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
