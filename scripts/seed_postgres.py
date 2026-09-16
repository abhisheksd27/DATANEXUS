"""
DataNexus Utility: Seed PostgreSQL Users Database
Generates user accounts and inventory stock catalog in PostgreSQL `users_db`
to provide realistic customer demographics and product availability.
"""

import random
from datetime import datetime, timedelta
import psycopg2

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgrespassword",
    "dbname": "users_db"
}

FIRST_NAMES = ["Aarav", "Priya", "Rohan", "Ananya", "Vikram", "Neha", "Rahul", "Sneha", "Karan", "Pooja", "Arjun", "Kavya", "Siddharth", "Meera"]
LAST_NAMES = ["Sharma", "Patel", "Verma", "Reddy", "Joshi", "Nair", "Iyer", "Mehta", "Gupta", "Deshmukh", "Chopra", "Kulkarni"]
CITIES = ["Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Pune", "Chennai", "Kolkata", "Ahmedabad"]

PRODUCTS = [
    ("prod-A", "Wireless Headphones", "Electronics", 200, 149.99),
    ("prod-B", "Ergonomic Mouse", "Electronics", 350, 44.75),
    ("prod-C", "Mechanical Keyboard", "Electronics", 120, 299.00),
    ("prod-D", "USB-C Fast Charging Cable", "Accessories", 600, 19.99),
    ("prod-E", "Aluminum Laptop Stand", "Accessories", 250, 59.99),
    ("prod-F", "27-inch 4K IPS Monitor", "Displays", 50, 389.50),
    ("prod-G", "Noise-Cancelling Earbuds", "Electronics", 220, 89.00),
    ("prod-H", "Ultra-Slim Web Camera 1080p", "Electronics", 90, 75.00),
    ("prod-I", "Desk Mat Gaming Surface", "Accessories", 400, 25.00),
    ("prod-J", "Smart LED Desk Lamp", "Accessories", 150, 49.99)
]

def seed_database(num_users=30):
    print(f"🔌 Connecting to PostgreSQL users_db at {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}...")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    # Seed Users
    print(f"🌱 Seeding {num_users} users...")
    now = datetime.now()

    for i in range(1, num_users + 1):
        user_id = f"usr-{i:03d}"
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        full_name = f"{fn} {ln}"
        email = f"{fn.lower()}.{ln.lower()}{random.randint(10, 999)}@example.com"
        city = random.choice(CITIES)
        signup_date = now - timedelta(days=random.randint(1, 365))

        cursor.execute(
            """
            INSERT INTO users (user_id, full_name, email, city, signup_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET full_name = EXCLUDED.full_name, email = EXCLUDED.email, city = EXCLUDED.city;
            """,
            (user_id, full_name, email, city, signup_date)
        )

    # Seed Inventory
    print("🌱 Seeding product inventory catalog...")
    for prod_id, name, cat, stock, price in PRODUCTS:
        cursor.execute(
            """
            INSERT INTO inventory (product_id, product_name, category, stock_quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET stock_quantity = EXCLUDED.stock_quantity, unit_price = EXCLUDED.unit_price;
            """,
            (prod_id, name, cat, stock, price)
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Successfully seeded {num_users} users and {len(PRODUCTS)} inventory products!")

if __name__ == "__main__":
    seed_database(num_users=30)
