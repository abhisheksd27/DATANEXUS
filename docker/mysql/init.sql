-- Create database for Orders & Payments microservice
CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

-- 1. Orders Table: Tracks customer orders
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    shipping_city VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Order Items Table: Tracks individual products inside each order
CREATE TABLE IF NOT EXISTS order_items (
    item_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);

-- 3. Payments Table: Tracks payment transactions
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);

-- Seed initial records into MySQL
INSERT INTO orders (order_id, user_id, total_amount, status, shipping_city) VALUES
('ord-101', 'usr-001', 149.99, 'COMPLETED', 'Mumbai'),
('ord-102', 'usr-002', 89.50, 'PENDING', 'Bengaluru'),
('ord-103', 'usr-003', 299.00, 'COMPLETED', 'Delhi'),
('ord-104', 'usr-001', 45.00, 'COMPLETED', 'Mumbai');

INSERT INTO order_items (item_id, order_id, product_id, quantity, price) VALUES
('itm-01', 'ord-101', 'prod-A', 1, 149.99),
('itm-02', 'ord-102', 'prod-B', 2, 44.75),
('itm-03', 'ord-103', 'prod-C', 1, 299.00),
('itm-04', 'ord-104', 'prod-D', 1, 45.00);

INSERT INTO payments (payment_id, order_id, payment_method, payment_status, amount) VALUES
('pay-501', 'ord-101', 'CREDIT_CARD', 'SUCCESS', 149.99),
('pay-502', 'ord-102', 'UPI', 'PENDING', 89.50),
('pay-503', 'ord-103', 'DEBIT_CARD', 'SUCCESS', 299.00),
('pay-504', 'ord-104', 'UPI', 'SUCCESS', 45.00);
