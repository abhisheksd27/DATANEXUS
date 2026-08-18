-- 1. Users Table: Customer Profiles
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR(36) PRIMARY KEY,
    full_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(100) UNIQUE NOT NULL,
    city        VARCHAR(50) NOT NULL,
    signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Inventory Table: Product catalog & stock counts
CREATE TABLE IF NOT EXISTS inventory (
    product_id     VARCHAR(36) PRIMARY KEY,
    product_name   VARCHAR(100) NOT NULL,
    category       VARCHAR(50) NOT NULL,
    stock_quantity INT NOT NULL,
    unit_price     DECIMAL(10, 2) NOT NULL
);

-- Seed initial records
INSERT INTO users (user_id, full_name, email, city) VALUES
('usr-001', 'Aarav Sharma', 'aarav@example.com', 'Mumbai'),
('usr-002', 'Priya Patel', 'priya@example.com', 'Bengaluru'),
('usr-003', 'Rohan Verma', 'rohan@example.com', 'Delhi');

INSERT INTO inventory (product_id, product_name, category, stock_quantity, unit_price) VALUES
('prod-A', 'Wireless Headphones', 'Electronics', 150, 149.99),
('prod-B', 'Ergonomic Mouse', 'Electronics', 300, 44.75),
('prod-C', 'Mechanical Keyboard', 'Electronics', 80, 299.00);