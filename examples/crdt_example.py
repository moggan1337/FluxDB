"""
CRDT Advanced Examples

Demonstrates advanced CRDT usage patterns including:
- Distributed counters
- Collaborative data structures
- Conflict resolution
- Merge strategies
"""

import time
from typing import Set, Any

from src.crdt.registers import (
    LWWRegister, MVRegister, ORSet, TwoPhaseSet, LWWMap,
    Timestamp
)
from src.crdt.counters import (
    GCounter, PNCounter, GCounterMap, PNCounterMap
)
from src.crdt.merge import (
    merge, merge_all, diagnose_merge, auto_merge, ConflictResolver
)


class LastWriterWinsResolver(ConflictResolver):
    """Custom resolver that picks latest timestamp"""
    
    def resolve(self, values: list, metadata: list) -> Any:
        if not values:
            return None
        
        latest_idx = 0
        latest_ts = None
        
        for i, meta in enumerate(metadata):
            if isinstance(meta, Timestamp):
                if latest_ts is None or meta > latest_ts:
                    latest_ts = meta
                    latest_idx = i
        
        return values[latest_idx]


def example_distributed_counter():
    """Example: Distributed Counter System"""
    print("\n" + "=" * 60)
    print("DISTRIBUTED COUNTER SYSTEM")
    print("=" * 60)
    
    # Simulate 3 nodes
    nodes = {
        "node1": GCounter(),
        "node2": GCounter(),
        "node3": GCounter(),
    }
    
    print("\nSimulating distributed counting...")
    
    # Each node processes requests independently
    nodes["node1"].increment("node1", 100)
    nodes["node1"].increment("node1", 50)
    
    nodes["node2"].increment("node2", 75)
    nodes["node2"].increment("node2", 25)
    
    nodes["node3"].increment("node3", 200)
    
    print(f"\nNode 1 count: {nodes['node1'].value()}")
    print(f"Node 2 count: {nodes['node2'].value()}")
    print(f"Node 3 count: {nodes['node3'].value()}")
    
    # Merge all counters
    total = merge_all(list(nodes.values()))
    print(f"\nMerged total: {total.value()}")
    
    print("\n✓ Distributed counter example complete")


def example_collaborative_shopping_cart():
    """Example: Collaborative Shopping Cart with OR-Set"""
    print("\n" + "=" * 60)
    print("COLLABORATIVE SHOPPING CART")
    print("=" * 60)
    
    # Simulate two users editing a cart
    cart_node1 = ORSet(node_id="user1")
    cart_node2 = ORSet(node_id="user2")
    
    print("\nUser 1 adds items to cart...")
    cart_node1.add("Apple")
    cart_node1.add("Banana")
    cart_node1.add("Milk")
    print(f"   User 1 cart: {cart_node1.get()}")
    
    print("\nUser 2 adds items to cart (while offline)...")
    cart_node2.add("Banana")  # Already in User 1's cart
    cart_node2.add("Orange")
    cart_node2.add("Bread")
    print(f"   User 2 cart: {cart_node2.get()}")
    
    print("\nUser 1 comes back online, syncing...")
    cart_node1.merge(cart_node2)
    print(f"   Merged cart: {cart_node1.get()}")
    
    print("\nUser 1 removes Banana...")
    cart_node1.remove("Banana")
    print(f"   After remove: {cart_node1.get()}")
    
    print("\nUser 2 syncs again...")
    cart_node2.merge(cart_node1)
    print(f"   User 2 sees: {cart_node2.get()}")
    
    print("\n✓ Collaborative cart example complete")


def example_multi_value_register():
    """Example: Multi-Value Register for conflict visualization"""
    print("\n" + "=" * 60)
    print("MULTI-VALUE REGISTER (CONFLICT VISUALIZATION)")
    print("=" * 60)
    
    # Two nodes make concurrent edits
    reg1 = MVRegister()
    reg2 = MVRegister()
    
    print("\nNode 1 sets value to 'Option A'")
    reg1.add("Option A", Timestamp(100, 0, "node1"))
    
    print("Node 2 sets value to 'Option B' (concurrent)")
    reg2.add("Option B", Timestamp(101, 0, "node2"))
    
    print("\nBefore merge - Node 1 sees:", reg1.get())
    print("Before merge - Node 2 sees:", reg2.get())
    
    # Merge - both options preserved
    reg1.merge(reg2)
    
    print("\nAfter merge, both options preserved:", reg1.get())
    print("\nThis allows applications to show users both choices!")
    
    print("\n✓ Multi-value register example complete")


def example_lww_map_user_profile():
    """Example: LWW-Map for User Profiles"""
    print("\n" + "=" * 60)
    print("LWW-MAP USER PROFILES")
    print("=" * 60)
    
    # User profile stored as LWW-Map
    profile1 = LWWMap(node_id="web-app")
    profile2 = LWWMap(node_id="mobile-app")
    
    print("\nWeb app updates user profile...")
    profile1.put("display_name", "Alice Smith")
    profile1.put("bio", "Software developer")
    profile1.put("avatar_url", "https://example.com/alice.jpg")
    print(f"   Profile: {profile1.items()}")
    
    print("\nMobile app updates same profile (different fields)...")
    profile2.put("display_name", "Alice S.")  # Shorter name
    profile2.put("status", "online")
    print(f"   Profile: {profile2.items()}")
    
    print("\nMerging profiles...")
    profile1.merge(profile2)
    print(f"   Merged: {profile1.items()}")
    
    print("\n✓ LWW-Map example complete")


def example_positive_negative_counter():
    """Example: PN-Counter for Account Balance"""
    print("\n" + "=" * 60)
    print("PN-COUNTER ACCOUNT BALANCE")
    print("=" * 60)
    
    # Simulate distributed accounting
    accounts = {
        "atm_1": PNCounter(),
        "mobile_app": PNCounter(),
        "web_portal": PNCounter(),
    }
    
    print("\nATM 1 processes deposit: +$500")
    accounts["atm_1"].increment("atm_1", 500)
    
    print("Mobile app processes deposit: +$200")
    accounts["mobile_app"].increment("mobile_app", 200)
    
    print("Web portal processes withdrawal: -$100")
    accounts["web_portal"].decrement("web_portal", 100)
    
    print("Mobile app processes another deposit: +$50")
    accounts["mobile_app"].increment("mobile_app", 50)
    
    print("\nIndividual account totals:")
    for name, counter in accounts.items():
        print(f"   {name}: ${counter.value()}")
    
    # Merge all
    total = merge_all(list(accounts.values()))
    print(f"\nMerged balance: ${total.value()}")
    
    print("\n✓ PN-Counter example complete")


def example_inventory_tracking():
    """Example: Inventory Tracking with Multiple Warehouses"""
    print("\n" + "=" * 60)
    print("INVENTORY TRACKING")
    print("=" * 60)
    
    # Track inventory across warehouses
    inventory = GCounterMap()
    
    print("\nWarehouse A receives stock:")
    inventory.increment("SKU:123", "warehouse_a", 100)
    inventory.increment("SKU:456", "warehouse_a", 50)
    print(f"   SKU:123 = {inventory.value('SKU:123')}")
    print(f"   SKU:456 = {inventory.value('SKU:456')}")
    
    print("\nWarehouse B receives stock:")
    inventory.increment("SKU:123", "warehouse_b", 75)
    inventory.increment("SKU:789", "warehouse_b", 200)
    print(f"   SKU:123 = {inventory.value('SKU:123')}")
    print(f"   SKU:789 = {inventory.value('SKU:789')}")
    
    print("\nWarehouse C receives stock:")
    inventory.increment("SKU:123", "warehouse_c", 25)
    
    print("\nWarehouse A ships some items:")
    # In real scenario, would use PN-CounterMap for decrements
    # Here just showing the view
    print(f"   SKU:123 = {inventory.value('SKU:123')} (total across all warehouses)")
    
    print("\n✓ Inventory tracking example complete")


def example_merge_diagnostics():
    """Example: Merge Diagnostics and Debugging"""
    print("\n" + "=" * 60)
    print("MERGE DIAGNOSTICS")
    print("=" * 60)
    
    # Create conflicting registers
    reg1 = LWWRegister(node_id="node1")
    reg1.set("value_a", Timestamp(100, 0, "node1"))
    
    reg2 = LWWRegister(node_id="node2")
    reg2.set("value_b", Timestamp(101, 0, "node2"))
    
    print("\nDiagnosing potential conflicts...")
    diagnosis = diagnose_merge(reg1, reg2)
    
    print(f"   Type 1: {diagnosis['type1']}")
    print(f"   Type 2: {diagnosis['type2']}")
    print(f"   Compatible: {diagnosis['compatible']}")
    
    if diagnosis['potential_conflicts']:
        print("\n   Potential conflicts:")
        for conflict in diagnosis['potential_conflicts']:
            print(f"   - Type: {conflict['type']}")
            print(f"     Value 1: {conflict.get('value1')}")
            print(f"     Value 2: {conflict.get('value2')}")
    
    print("\nPerforming auto-merge...")
    result = auto_merge(reg1, reg2)
    
    print(f"   Success: {result.success}")
    print(f"   Merged value: {result.merged.get()}")
    print(f"   Errors: {result.errors}")
    
    print("\n✓ Merge diagnostics example complete")


def main():
    """Run all CRDT examples"""
    print("\n" + "=" * 60)
    print("FLUXDB CRDT EXAMPLES")
    print("=" * 60)
    
    example_distributed_counter()
    example_collaborative_shopping_cart()
    example_multi_value_register()
    example_lww_map_user_profile()
    example_positive_negative_counter()
    example_inventory_tracking()
    example_merge_diagnostics()
    
    print("\n" + "=" * 60)
    print("ALL CRDT EXAMPLES COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
