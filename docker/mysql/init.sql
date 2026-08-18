CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

create table if not exists orders(
    order_id  varchar(255) primary key,
    user_id   varchar(255) not null,
    total_amount decimal(10,2) not null,
    status    varchar(50) not null,
    city varchar(100) not null,
    created_at timestamp default current_timestamp
);

create table if not exists order_items(
    item_id    varchar(255) primary key,
    order_id   varchar(255) not null,
    product_id varchar(255) not null,
    quantity   int not null,
    price      decimal(10,2) not null,
    created_at timestamp default current_timestamp,
    foreign key (order_id) references orders(order_id) on delete cascade
);

create table if not exists payments (
    payment_id varchar(255) primary key,
    order_id   varchar(255) not null,
    amount     decimal(10,2) not null,
    method     varchar(50) not null,
    status     varchar(50) not null,
    paid_at  timestamp default current_timestamp,
    foreign key (order_id) references orders(order_id) on delete cascade
);


-- Seed initial transactions
INSERT INTO orders (order_id, user_id, total_amount, status, city) VALUES
('ord-101', 'usr-001', 149.99, 'COMPLETED', 'Mumbai'),
('ord-102', 'usr-002', 89.50, 'PENDING', 'Bengaluru'),
('ord-103', 'usr-003', 299.00, 'COMPLETED', 'Delhi');
INSERT INTO order_items (item_id, order_id, product_id, quantity, price) VALUES
('itm-01', 'ord-101', 'prod-A', 1, 149.99),
('itm-02', 'ord-102', 'prod-B', 2, 44.75),
('itm-03', 'ord-103', 'prod-C', 1, 299.00);
INSERT INTO payments (payment_id, order_id, method, status, amount) VALUES
('pay-501', 'ord-101', 'CREDIT_CARD', 'SUCCESS', 149.99),
('pay-502', 'ord-102', 'UPI', 'PENDING', 89.50),
('pay-503', 'ord-103', 'DEBIT_CARD', 'SUCCESS', 299.00);