#!/usr/bin/env python3
"""
Synthetic Workload Generator for Schema Travels

Generates realistic PostgreSQL query logs based on configurable workload patterns.
Simulates an e-commerce application with users, products, orders, etc.

Usage:
    python generate_workload.py --queries 10000 --output ./logs/
    python generate_workload.py --queries 50000 --pattern oltp --output ./logs/
"""

import argparse
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass
from typing import Callable
import math


@dataclass
class QueryTemplate:
    """A query template with weight and timing characteristics."""
    sql: str
    weight: float  # Relative frequency (higher = more common)
    avg_duration_ms: float
    std_duration_ms: float
    query_type: str  # SELECT, INSERT, UPDATE, DELETE


class WorkloadGenerator:
    """Generates realistic database query logs."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.user_ids = list(range(1, 10001))  # 10K users
        self.product_ids = list(range(1, 5001))  # 5K products
        self.order_ids = list(range(1, 50001))  # 50K orders
        self.category_ids = list(range(1, 51))  # 50 categories
        
        # Track generated data for consistency
        self.generated_orders = []

    def generate_ecommerce_workload(self, num_queries: int) -> list[tuple[str, float]]:
        """Generate e-commerce workload queries."""
        templates = self._get_ecommerce_templates()
        return self._generate_from_templates(templates, num_queries)

    def generate_oltp_workload(self, num_queries: int) -> list[tuple[str, float]]:
        """Generate OLTP-style workload (high write ratio)."""
        templates = self._get_oltp_templates()
        return self._generate_from_templates(templates, num_queries)

    def generate_analytics_workload(self, num_queries: int) -> list[tuple[str, float]]:
        """Generate analytics workload (complex reads, aggregations)."""
        templates = self._get_analytics_templates()
        return self._generate_from_templates(templates, num_queries)

    def generate_mixed_workload(self, num_queries: int) -> list[tuple[str, float]]:
        """Generate mixed workload combining all patterns."""
        ecommerce = self._get_ecommerce_templates()
        oltp = self._get_oltp_templates()
        analytics = self._get_analytics_templates()
        
        # Combine with weights
        all_templates = ecommerce + oltp[:5] + analytics[:3]
        return self._generate_from_templates(all_templates, num_queries)

    def _generate_from_templates(
        self, 
        templates: list[QueryTemplate], 
        num_queries: int
    ) -> list[tuple[str, float]]:
        """Generate queries from weighted templates."""
        # Normalize weights
        total_weight = sum(t.weight for t in templates)
        normalized = [(t, t.weight / total_weight) for t in templates]
        
        # Build cumulative distribution
        cumulative = []
        cum_sum = 0
        for template, weight in normalized:
            cum_sum += weight
            cumulative.append((cum_sum, template))
        
        queries = []
        for _ in range(num_queries):
            # Select template based on weight
            r = random.random()
            template = cumulative[-1][1]  # Default to last
            for cum_weight, t in cumulative:
                if r <= cum_weight:
                    template = t
                    break
            
            # Generate concrete query
            sql = self._fill_template(template.sql)
            duration = max(0.1, random.gauss(template.avg_duration_ms, template.std_duration_ms))
            queries.append((sql, duration))
        
        return queries

    def _fill_template(self, sql: str) -> str:
        """Fill in template placeholders with realistic values."""
        # Replace placeholders
        sql = sql.replace("{user_id}", str(random.choice(self.user_ids)))
        sql = sql.replace("{product_id}", str(random.choice(self.product_ids)))
        sql = sql.replace("{order_id}", str(random.choice(self.order_ids)))
        sql = sql.replace("{category_id}", str(random.choice(self.category_ids)))
        sql = sql.replace("{email}", f"user{random.randint(1,10000)}@example.com")
        sql = sql.replace("{name}", f"User {random.randint(1,10000)}")
        sql = sql.replace("{price}", f"{random.uniform(10, 500):.2f}")
        sql = sql.replace("{quantity}", str(random.randint(1, 10)))
        sql = sql.replace("{status}", random.choice(["pending", "processing", "shipped", "delivered"]))
        sql = sql.replace("{limit}", str(random.choice([10, 20, 50, 100])))
        sql = sql.replace("{offset}", str(random.randint(0, 100) * 10))
        sql = sql.replace("{days}", str(random.randint(1, 30)))
        sql = sql.replace("{rating}", str(random.randint(1, 5)))
        sql = sql.replace("{search_term}", random.choice(["phone", "laptop", "shirt", "book", "camera"]))
        
        return sql

    def _get_ecommerce_templates(self) -> list[QueryTemplate]:
        """E-commerce query templates."""
        return [
            # High frequency - Product browsing
            QueryTemplate(
                sql="SELECT p.id, p.name, p.price, p.description, c.name as category FROM products p JOIN categories c ON p.category_id = c.id WHERE p.id = {product_id}",
                weight=100, avg_duration_ms=3.5, std_duration_ms=1.2, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT * FROM product_images WHERE product_id = {product_id} ORDER BY sort_order",
                weight=95, avg_duration_ms=2.0, std_duration_ms=0.8, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT p.id, p.name, p.price FROM products p WHERE p.category_id = {category_id} ORDER BY p.created_at DESC LIMIT {limit}",
                weight=80, avg_duration_ms=8.5, std_duration_ms=3.0, query_type="SELECT"
            ),
            
            # High frequency - User sessions
            QueryTemplate(
                sql="SELECT id, email, name FROM users WHERE id = {user_id}",
                weight=90, avg_duration_ms=1.5, std_duration_ms=0.5, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT * FROM addresses WHERE user_id = {user_id}",
                weight=60, avg_duration_ms=2.0, std_duration_ms=0.7, query_type="SELECT"
            ),
            
            # Medium frequency - Order viewing
            QueryTemplate(
                sql="SELECT o.id, o.status, o.total, o.created_at, u.name, u.email FROM orders o JOIN users u ON o.user_id = u.id WHERE o.user_id = {user_id} ORDER BY o.created_at DESC LIMIT 10",
                weight=50, avg_duration_ms=12.0, std_duration_ms=4.0, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT oi.id, oi.quantity, oi.unit_price, p.name, p.sku FROM order_items oi JOIN products p ON oi.product_id = p.id WHERE oi.order_id = {order_id}",
                weight=45, avg_duration_ms=8.0, std_duration_ms=2.5, query_type="SELECT"
            ),
            
            # Medium frequency - Reviews
            QueryTemplate(
                sql="SELECT r.id, r.rating, r.title, r.body, r.created_at, u.name FROM reviews r JOIN users u ON r.user_id = u.id WHERE r.product_id = {product_id} ORDER BY r.created_at DESC LIMIT 10",
                weight=40, avg_duration_ms=10.0, std_duration_ms=3.5, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT AVG(rating) as avg_rating, COUNT(*) as review_count FROM reviews WHERE product_id = {product_id}",
                weight=35, avg_duration_ms=5.0, std_duration_ms=1.5, query_type="SELECT"
            ),
            
            # Lower frequency - Cart/Checkout
            QueryTemplate(
                sql="INSERT INTO orders (user_id, status, total, shipping_address_id) VALUES ({user_id}, 'pending', {price}, {user_id})",
                weight=15, avg_duration_ms=5.0, std_duration_ms=1.5, query_type="INSERT"
            ),
            QueryTemplate(
                sql="INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES ({order_id}, {product_id}, {quantity}, {price}, {price})",
                weight=18, avg_duration_ms=4.0, std_duration_ms=1.2, query_type="INSERT"
            ),
            QueryTemplate(
                sql="UPDATE orders SET status = '{status}', updated_at = NOW() WHERE id = {order_id}",
                weight=12, avg_duration_ms=3.5, std_duration_ms=1.0, query_type="UPDATE"
            ),
            QueryTemplate(
                sql="UPDATE products SET inventory_count = inventory_count - {quantity} WHERE id = {product_id}",
                weight=10, avg_duration_ms=4.0, std_duration_ms=1.2, query_type="UPDATE"
            ),
            
            # Lower frequency - Reviews
            QueryTemplate(
                sql="INSERT INTO reviews (user_id, product_id, rating, title, body) VALUES ({user_id}, {product_id}, {rating}, 'Great product', 'Really enjoyed this purchase')",
                weight=8, avg_duration_ms=4.5, std_duration_ms=1.3, query_type="INSERT"
            ),
            
            # Search queries
            QueryTemplate(
                sql="SELECT p.id, p.name, p.price FROM products p WHERE p.name ILIKE '%{search_term}%' ORDER BY p.created_at DESC LIMIT {limit}",
                weight=30, avg_duration_ms=25.0, std_duration_ms=10.0, query_type="SELECT"
            ),
            
            # Category browsing
            QueryTemplate(
                sql="SELECT c.id, c.name, COUNT(p.id) as product_count FROM categories c LEFT JOIN products p ON c.id = p.category_id GROUP BY c.id, c.name ORDER BY c.name",
                weight=20, avg_duration_ms=15.0, std_duration_ms=5.0, query_type="SELECT"
            ),
        ]

    def _get_oltp_templates(self) -> list[QueryTemplate]:
        """OLTP workload templates (high write ratio)."""
        return [
            # Logging / Events (very high write)
            QueryTemplate(
                sql="INSERT INTO event_logs (user_id, event_type, event_data, created_at) VALUES ({user_id}, 'page_view', '{{\"page\": \"product\"}}', NOW())",
                weight=200, avg_duration_ms=2.0, std_duration_ms=0.5, query_type="INSERT"
            ),
            QueryTemplate(
                sql="INSERT INTO session_events (session_id, event_type, timestamp) VALUES ('sess_{user_id}', 'click', NOW())",
                weight=150, avg_duration_ms=1.5, std_duration_ms=0.4, query_type="INSERT"
            ),
            
            # Inventory updates
            QueryTemplate(
                sql="UPDATE inventory SET quantity = quantity - 1, updated_at = NOW() WHERE product_id = {product_id} AND warehouse_id = 1",
                weight=50, avg_duration_ms=3.0, std_duration_ms=0.8, query_type="UPDATE"
            ),
            
            # Price updates
            QueryTemplate(
                sql="UPDATE products SET price = {price}, updated_at = NOW() WHERE id = {product_id}",
                weight=20, avg_duration_ms=3.5, std_duration_ms=1.0, query_type="UPDATE"
            ),
            
            # User activity
            QueryTemplate(
                sql="UPDATE users SET last_active_at = NOW() WHERE id = {user_id}",
                weight=80, avg_duration_ms=2.0, std_duration_ms=0.6, query_type="UPDATE"
            ),
            
            # Quick lookups
            QueryTemplate(
                sql="SELECT id, email FROM users WHERE id = {user_id}",
                weight=100, avg_duration_ms=1.0, std_duration_ms=0.3, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT id, inventory_count FROM products WHERE id = {product_id}",
                weight=60, avg_duration_ms=1.2, std_duration_ms=0.4, query_type="SELECT"
            ),
        ]

    def _get_analytics_templates(self) -> list[QueryTemplate]:
        """Analytics workload templates (complex reads)."""
        return [
            # Sales reports
            QueryTemplate(
                sql="SELECT DATE(o.created_at) as date, COUNT(*) as orders, SUM(o.total) as revenue FROM orders o WHERE o.created_at > NOW() - INTERVAL '{days} days' GROUP BY DATE(o.created_at) ORDER BY date",
                weight=10, avg_duration_ms=150.0, std_duration_ms=50.0, query_type="SELECT"
            ),
            QueryTemplate(
                sql="SELECT p.category_id, c.name, COUNT(oi.id) as items_sold, SUM(oi.total_price) as revenue FROM order_items oi JOIN products p ON oi.product_id = p.id JOIN categories c ON p.category_id = c.id JOIN orders o ON oi.order_id = o.id WHERE o.created_at > NOW() - INTERVAL '{days} days' GROUP BY p.category_id, c.name ORDER BY revenue DESC",
                weight=8, avg_duration_ms=200.0, std_duration_ms=80.0, query_type="SELECT"
            ),
            
            # Top products
            QueryTemplate(
                sql="SELECT p.id, p.name, COUNT(oi.id) as times_ordered, SUM(oi.quantity) as units_sold FROM products p JOIN order_items oi ON p.id = oi.product_id GROUP BY p.id, p.name ORDER BY units_sold DESC LIMIT 100",
                weight=5, avg_duration_ms=180.0, std_duration_ms=60.0, query_type="SELECT"
            ),
            
            # User analytics
            QueryTemplate(
                sql="SELECT u.id, u.email, COUNT(o.id) as order_count, SUM(o.total) as lifetime_value FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.email HAVING COUNT(o.id) > 0 ORDER BY lifetime_value DESC LIMIT 100",
                weight=4, avg_duration_ms=250.0, std_duration_ms=100.0, query_type="SELECT"
            ),
            
            # Inventory analysis
            QueryTemplate(
                sql="SELECT p.id, p.name, p.inventory_count, COALESCE(SUM(oi.quantity), 0) as sold_last_30d FROM products p LEFT JOIN order_items oi ON p.id = oi.product_id LEFT JOIN orders o ON oi.order_id = o.id AND o.created_at > NOW() - INTERVAL '30 days' GROUP BY p.id, p.name, p.inventory_count ORDER BY sold_last_30d DESC",
                weight=3, avg_duration_ms=300.0, std_duration_ms=120.0, query_type="SELECT"
            ),
            
            # Review analytics
            QueryTemplate(
                sql="SELECT p.id, p.name, AVG(r.rating) as avg_rating, COUNT(r.id) as review_count FROM products p LEFT JOIN reviews r ON p.id = r.product_id GROUP BY p.id, p.name HAVING COUNT(r.id) >= 5 ORDER BY avg_rating DESC LIMIT 50",
                weight=3, avg_duration_ms=120.0, std_duration_ms=40.0, query_type="SELECT"
            ),
        ]


def format_postgres_log(
    queries: list[tuple[str, float]], 
    start_time: datetime | None = None
) -> str:
    """Format queries as PostgreSQL log entries."""
    if start_time is None:
        start_time = datetime.now() - timedelta(hours=1)
    
    lines = []
    current_time = start_time
    
    for sql, duration_ms in queries:
        # Add some time variance
        current_time += timedelta(milliseconds=random.randint(10, 500))
        
        pid = random.randint(10000, 99999)
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Log statement
        lines.append(
            f"{timestamp} UTC [{pid}] app@ecommerce LOG:  statement: {sql}"
        )
        # Log duration
        lines.append(
            f"{timestamp} UTC [{pid}] app@ecommerce LOG:  duration: {duration_ms:.3f} ms"
        )
        
        current_time += timedelta(milliseconds=duration_ms)
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic PostgreSQL query logs"
    )
    parser.add_argument(
        "--queries", "-n",
        type=int,
        default=10000,
        help="Number of queries to generate (default: 10000)"
    )
    parser.add_argument(
        "--pattern", "-p",
        choices=["ecommerce", "oltp", "analytics", "mixed"],
        default="ecommerce",
        help="Workload pattern (default: ecommerce)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("./generated_logs"),
        help="Output directory (default: ./generated_logs)"
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    
    # Generate workload
    print(f"Generating {args.queries:,} queries with '{args.pattern}' pattern...")
    generator = WorkloadGenerator(seed=args.seed)
    
    if args.pattern == "ecommerce":
        queries = generator.generate_ecommerce_workload(args.queries)
    elif args.pattern == "oltp":
        queries = generator.generate_oltp_workload(args.queries)
    elif args.pattern == "analytics":
        queries = generator.generate_analytics_workload(args.queries)
    else:
        queries = generator.generate_mixed_workload(args.queries)
    
    # Format and write logs
    log_content = format_postgres_log(queries)
    log_file = args.output / "postgresql.log"
    log_file.write_text(log_content)
    
    # Print stats
    query_types = {}
    for sql, _ in queries:
        qt = sql.strip().split()[0].upper()
        query_types[qt] = query_types.get(qt, 0) + 1
    
    print(f"\n✓ Generated {len(queries):,} queries")
    print(f"  Output: {log_file}")
    print(f"\n  Query distribution:")
    for qt, count in sorted(query_types.items(), key=lambda x: -x[1]):
        pct = count / len(queries) * 100
        print(f"    {qt}: {count:,} ({pct:.1f}%)")
    
    # Calculate total simulated time
    total_duration = sum(d for _, d in queries)
    print(f"\n  Simulated duration: {total_duration/1000:.1f} seconds")
    print(f"  Avg query time: {total_duration/len(queries):.2f} ms")


if __name__ == "__main__":
    main()
