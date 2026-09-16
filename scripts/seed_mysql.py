"""
DataNexus Utility: Seed MySQL Orders Database
Inserts batch transactions into `orders`, `order_items`, and `payments` tables
to simulate high-volume e-commerce operational data.
"""

import uuid
import random
from datetime import datetime, timedelta
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "rootpassword",
    "database": "orders_db"
}

USERS = [f"usr-{i:03d}" for i in range(1, 21)]
CITIES = ["Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Pune", "Chennai", "Kolkata", "Ahmedabad"]
PRODUCTS = [
    ("prod-A", 149.99),
    ("prod-B", 44.75),
    ("prod-C", 299.00),
    ("prod-D", 19.99),
    ("prod-E", 59.99),
    ("prod-F", 389.50),
    ("prod-G", 89.00)
]
PAYMENT_METHODS = ["CREDIT_CARD", "UPI", "DEBIT_CARD", "NET_BANKING"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED"]

def seed_database(num_orders=50):
    print(f"🔌 Connecting to MySQL orders_db at {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    print(f"🌱 Generating and inserting {num_orders} orders...")
    now = datetime.now()

    for i in range(num_orders):
        order_id = f"ord-{uuid.uuid4().hex[:8]}"
        user_id = random.choice(USERS)
        city = random.choice(CITIES)
        status = random.choice(STATUSES)
        order_time = now - timedelta(hours=random.randint(0, 72), minutes=random.randint(0, 59))

        # Select items and compute total BEFORE any inserts
        selected_prods = random.sample(PRODUCTS, k=random.randint(1, 3))
        total_amount = 0.0
        items_to_insert = []

        for prod_id, price in selected_prods:
            qty = random.randint(1, 3)
            total_amount += price * qty
            items_to_insert.append((f"itm-{uuid.uuid4().hex[:8]}", order_id, prod_id, qty, price, order_time))

        # 1. Insert into orders FIRST (parent table)
        cursor.execute(
            """
            INSERT INTO orders (order_id, user_id, total_amount, status, city, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (order_id, user_id, round(total_amount, 2), status, city, order_time)
        )

        # 2. Insert order_items AFTER orders exists (child table with FK)
        for item in items_to_insert:
            cursor.execute(
                """
                INSERT INTO order_items (item_id, order_id, product_id, quantity, price, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                item
            )

        cursor.execute(
            """
            INSERT INTO payments (payment_id, order_id, amount, method, status, paid_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                f"pay-{uuid.uuid4().hex[:8]}",
                order_id,
                round(total_amount, 2),
                random.choice(PAYMENT_METHODS),
                "SUCCESS" if status == "COMPLETED" else "FAILED",
                order_time
            )
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Successfully seeded {num_orders} orders into MySQL orders_db!")

if __name__ == "__main__":
    seed_database(num_orders=50)
