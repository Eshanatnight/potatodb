CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    city VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    order_date DATE NOT NULL
);

INSERT INTO customers (id, name, email, city) VALUES (1, 'Alice Johnson', 'alice@example.com', 'Seattle');
INSERT INTO customers (id, name, email, city) VALUES (2, 'Bob Smith', 'bob@example.com', 'Portland');
INSERT INTO customers (id, name, email, city) VALUES (3, 'Carol Lee', 'carol@example.com', 'Denver');
INSERT INTO customers (id, name, email, city) VALUES (4, 'David Park', 'david@example.com', 'Austin');
INSERT INTO customers (id, name, email, city) VALUES (5, 'Eva Martinez', 'eva@example.com', 'Chicago');

INSERT INTO products (id, name, price, category) VALUES (1, 'Laptop', 999.99, 'Electronics');
INSERT INTO products (id, name, price, category) VALUES (2, 'Headphones', 49.95, 'Electronics');
INSERT INTO products (id, name, price, category) VALUES (3, 'Notebook', 5.50, 'Office');
INSERT INTO products (id, name, price, category) VALUES (4, 'Desk Lamp', 34.00, 'Home');
INSERT INTO products (id, name, price, category) VALUES (5, 'Backpack', 75.00, 'Accessories');

INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (1, 1, 1, 1, '2026-01-15');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (2, 1, 2, 2, '2026-01-20');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (3, 2, 3, 10, '2026-02-03');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (4, 3, 4, 1, '2026-02-10');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (5, 4, 5, 1, '2026-02-18');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (6, 5, 1, 1, '2026-02-25');
INSERT INTO orders (id, customer_id, product_id, quantity, order_date) VALUES (7, 2, 2, 3, '2026-03-01');

FLUSH;