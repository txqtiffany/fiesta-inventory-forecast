"""
shopify_sync.py
Syncs Shopify data (products, inventory, orders, locations) to sync_data.json
"""

import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()


def gid_to_id(gid: str) -> str:
    return gid.split("/")[-1] if gid else ""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ShopifySync:
    def __init__(self):
        self.shop_name = os.getenv("SHOPIFY_SHOP_NAME")
        self.access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
        self.api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")

        if not self.shop_name:
            raise ValueError("Missing SHOPIFY_SHOP_NAME in .env (use the myshopify subdomain, e.g. ssxqid-8t)")
        if not self.access_token:
            raise ValueError("Missing SHOPIFY_ACCESS_TOKEN in .env")

        self.base_url = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
        }

        # built during product sync: inventory_item_id -> variant_id
        self.inv_item_to_variant_id = {}

    def execute_query(self, query, variables=None):
        """Execute GraphQL query with basic retry + helpful error prints."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.post(self.base_url, json=payload, headers=self.headers, timeout=60)

                # Print useful context on non-2xx
                if resp.status_code >= 400:
                    print(f"HTTP {resp.status_code} for {self.base_url}")
                    print(f"Response (first 300 chars): {resp.text[:300]}")

                resp.raise_for_status()
                data = resp.json()

                # GraphQL errors
                if "errors" in data and data["errors"]:
                    print(f"GraphQL errors: {data['errors']}")
                    return None

                # Rate limit / throttle info (not money—just API capacity)
                ext = data.get("extensions", {})
                cost = ext.get("cost", {})
                throttle = cost.get("throttleStatus", {}) if cost else {}
                if cost and throttle:
                    actual = cost.get("actualQueryCost")
                    available = throttle.get("currentlyAvailable")
                    restore = throttle.get("restoreRate")
                    print(f"  Query cost: {actual}/{available} (restoreRate={restore})")

                    # If you're close to empty, pause a bit
                    if restore and available is not None and actual is not None:
                        if available < max(50, actual):
                            needed = max(0, max(50, actual) - available)
                            sleep_sec = max(1, int(needed / restore) + 1)
                            time.sleep(sleep_sec)

                return data.get("data")

            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

        return None

    # ---------------------------
    # GraphQL Fetchers
    # ---------------------------

    def fetch_products(self, cursor=None):
        query = """
        query ($cursor: String) {
          products(first: 50, after: $cursor) {
            edges {
              node {
                id
                title
                vendor
                status
                createdAt
                updatedAt
                variants(first: 100) {
                  edges {
                    node {
                      id
                      title
                      sku
                      price
                      inventoryItem { id }
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        variables = {"cursor": cursor} if cursor else {}
        return self.execute_query(query, variables)

    def fetch_locations(self):
        query = """
        query {
          locations(first: 50) {
            edges {
              node {
                id
                name
                isActive
              }
            }
          }
        }
        """
        return self.execute_query(query)

    def fetch_inventory_levels(self, location_gid, cursor=None):
        """
        IMPORTANT: InventoryLevel uses `item`, not `inventoryItem`.
        We'll map item.id (inventory_item_id) -> variant_id using product variants we already synced.
        """
        query = """
        query ($locationId: ID!, $cursor: String) {
          location(id: $locationId) {
            inventoryLevels(first: 250, after: $cursor) {
              edges {
                node {
                  id
                  item {
                    id
                    sku
                  }
                  quantities(names: ["available", "incoming", "committed"]) {
                    name
                    quantity
                  }
                }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """
        variables = {"locationId": location_gid, "cursor": cursor}
        return self.execute_query(query, variables)

    def fetch_orders(self, since_dt: datetime, cursor=None):
        query = """
        query ($query: String!, $cursor: String) {
          orders(first: 250, query: $query, after: $cursor) {
            edges {
              node {
                id
                name
                createdAt
                cancelledAt
                test
                lineItems(first: 250) {
                  edges {
                    node {
                      id
                      title
                      sku
                      quantity
                      variant {
                        id
                        product { vendor }
                      }
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        # Use UTC timestamp format for Shopify query
        since_utc = since_dt.astimezone(timezone.utc)
        since_ts = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        query_string = f"created_at:>={since_ts}"
        variables = {"query": query_string, "cursor": cursor}
        return self.execute_query(query, variables)

    # ---------------------------
    # Sync Methods
    # ---------------------------

    def sync_all_products(self):
        print("\n📦 Syncing products...")
        all_products = []
        all_variants = []
        cursor = None
        page = 1

        while True:
            print(f"  Fetching page {page}...")
            data = self.fetch_products(cursor)
            if not data or "products" not in data:
                break

            products = data["products"]

            for edge in products["edges"]:
                product = edge["node"]
                product_id = gid_to_id(product["id"])

                all_products.append(
                    {
                        "product_id": product_id,
                        "title": product.get("title"),
                        "vendor": product.get("vendor"),
                        "status": product.get("status"),
                        "created_at": product.get("createdAt"),
                        "updated_at": product.get("updatedAt"),
                    }
                )

                for v_edge in product["variants"]["edges"]:
                    v = v_edge["node"]
                    inv_item = v.get("inventoryItem") or {}
                    inv_item_id = gid_to_id(inv_item.get("id", ""))

                    variant_row = {
                        "variant_id": gid_to_id(v.get("id", "")),
                        "product_id": product_id,
                        "sku": v.get("sku") or "",
                        "title": v.get("title"),
                        "price": float(v.get("price") or 0.0),
                        "inventory_item_id": inv_item_id,
                    }
                    all_variants.append(variant_row)

            page_info = products["pageInfo"]
            if not page_info["hasNextPage"]:
                break

            cursor = page_info["endCursor"]
            page += 1
            time.sleep(0.25)

        # Build inventory_item_id -> variant_id mapping for inventory sync
        self.inv_item_to_variant_id = {
            v["inventory_item_id"]: v["variant_id"]
            for v in all_variants
            if v.get("inventory_item_id") and v.get("variant_id")
        }

        print(f"  ✓ Synced {len(all_products)} products, {len(all_variants)} variants")
        return all_products, all_variants

    def sync_all_locations(self):
        print("\n📍 Syncing locations...")
        data = self.fetch_locations()
        if not data or "locations" not in data:
            return []

        locations = []
        for edge in data["locations"]["edges"]:
            loc = edge["node"]
            locations.append(
                {
                    "location_id": gid_to_id(loc["id"]),
                    # Keep the GID for API calls, but we will *not* export it to BigQuery.
                    "location_gid": loc["id"],
                    "name": loc.get("name"),
                    "active": bool(loc.get("isActive")),
                }
            )

        print(f"  ✓ Synced {len(locations)} locations")
        return locations

    def sync_inventory_for_location(self, location_gid, location_id, location_name):
        print(f"  Syncing inventory for {location_name}...")
        all_inventory = []
        cursor = None

        snapshot_dt = utc_now()
        snapshot_date = snapshot_dt.date().isoformat()
        snapshot_ts = snapshot_dt.isoformat().replace("+00:00", "Z")

        while True:
            data = self.fetch_inventory_levels(location_gid, cursor)
            if not data or not data.get("location"):
                break

            inv_levels = data["location"]["inventoryLevels"]
            for edge in inv_levels["edges"]:
                node = edge["node"]
                item = node.get("item")
                if not item:
                    continue

                inv_item_id = gid_to_id(item.get("id", ""))
                sku = item.get("sku") or ""

                # Map inventory_item_id -> variant_id (from product sync)
                variant_id = self.inv_item_to_variant_id.get(inv_item_id)
                if not variant_id:
                    # If you want to keep records even when unmapped, remove this continue
                    continue

                quantities = {q["name"]: q["quantity"] for q in (node.get("quantities") or [])}

                all_inventory.append(
                    {
                        "snapshot_id": f"{variant_id}_{location_id}_{snapshot_date}",
                        "variant_id": variant_id,
                        "sku": sku,
                        "location_id": location_id,
                        "available_qty": int(quantities.get("available", 0) or 0),
                        "incoming_qty": int(quantities.get("incoming", 0) or 0),
                        "committed_qty": int(quantities.get("committed", 0) or 0),
                        "snapshot_date": snapshot_date,
                        "snapshot_timestamp": snapshot_ts,
                    }
                )

            page_info = inv_levels["pageInfo"]
            if not page_info["hasNextPage"]:
                break

            cursor = page_info["endCursor"]
            time.sleep(0.25)

        print(f"    ✓ {len(all_inventory)} inventory records")
        return all_inventory

    def sync_all_inventory(self, locations):
        print("\n📊 Syncing inventory...")
        all_inventory = []

        # Single-location store: just use active locations
        active_locs = [l for l in locations if l.get("active")]
        for loc in active_locs:
            inv = self.sync_inventory_for_location(
                location_gid=loc["location_gid"],
                location_id=loc["location_id"],
                location_name=loc.get("name", "Unknown"),
            )
            all_inventory.extend(inv)

        print(f"  ✓ Total inventory records: {len(all_inventory)}")
        return all_inventory

    def sync_orders(self, days_back=365):
        print(f"\n🛒 Syncing orders (last {days_back} days)...")
        all_sales = []
        cursor = None
        page = 1

        since_dt = utc_now() - timedelta(days=days_back)

        while True:
            print(f"  Fetching page {page}...")
            data = self.fetch_orders(since_dt, cursor)
            if not data or "orders" not in data:
                break

            orders = data["orders"]
            for edge in orders["edges"]:
                order = edge["node"]

                # Skip test/cancelled orders
                if order.get("test") or order.get("cancelledAt"):
                    continue

                order_id = gid_to_id(order["id"])
                order_name = order.get("name") or ""

                created_at = order.get("createdAt")
                order_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                sale_date = order_dt.date().isoformat()
                sale_ts = order_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

                for li_edge in order["lineItems"]["edges"]:
                    item = li_edge["node"]
                    variant = item.get("variant")
                    if not variant:
                        continue

                    variant_id = gid_to_id(variant.get("id", ""))
                    sku = item.get("sku") or ""

                    vendor = ""
                    prod = variant.get("product") if variant else None
                    if prod and prod.get("vendor"):
                        vendor = prod["vendor"]

                    sale_id = f"{order_id}_{gid_to_id(item.get('id', ''))}"

                    all_sales.append(
                        {
                            "sale_id": sale_id,
                            "order_id": order_id,
                            "order_name": order_name,
                            "variant_id": variant_id,
                            "sku": sku,
                            "product_title": item.get("title"),
                            "quantity_sold": int(item.get("quantity") or 0),
                            "sale_date": sale_date,
                            "sale_timestamp": sale_ts,
                            "vendor": vendor,
                        }
                    )

            page_info = orders["pageInfo"]
            if not page_info["hasNextPage"]:
                break

            cursor = page_info["endCursor"]
            page += 1
            time.sleep(0.25)

        print(f"  ✓ Synced {len(all_sales)} sales records")
        return all_sales


if __name__ == "__main__":
    print("=" * 60)
    print("SHOPIFY DATA SYNC")
    print("=" * 60)

    syncer = ShopifySync()

    products, variants = syncer.sync_all_products()
    locations = syncer.sync_all_locations()
    inventory = syncer.sync_all_inventory(locations)

    # Daily runs should be incremental for cost/perf.
    # First-time backfill: set SHOPIFY_ORDERS_DAYS_BACK=365 (or more) in your .env.
    orders_days_back = int(os.getenv("SHOPIFY_ORDERS_DAYS_BACK", "14"))
    sales = syncer.sync_orders(days_back=orders_days_back)

    print("\n" + "=" * 60)
    print("SYNC SUMMARY")
    print("=" * 60)
    print(f"Products:  {len(products)}")
    print(f"Variants:  {len(variants)}")
    print(f"Locations: {len(locations)}")
    print(f"Inventory: {len(inventory)}")
    print(f"Sales:     {len(sales)}")
    print("=" * 60)

    # Export locations WITHOUT location_gid (BigQuery locations table doesn't need it).
    locations_export = [
        {
            "location_id": l.get("location_id", ""),
            "name": l.get("name"),
            "active": bool(l.get("active")),
        }
        for l in locations
    ]

    with open("sync_data.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "products": products,
                "variants": variants,
                "locations": locations_export,
                "inventory": inventory,
                "sales": sales,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print("\n✅ Data saved to sync_data.json")
    print("Next step: Run load_to_bigquery.py to upload to BigQuery")
