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

-- Insert 500,000 customers
INSERT INTO customers (id, name, email, city)
SELECT
    gs.value                                           AS id,
    'Customer ' || gs.value                            AS name,
    'customer_' || gs.value || '@example.com'          AS email,
    CASE (gs.value % 10)
        WHEN 0 THEN 'Seattle'
        WHEN 1 THEN 'Portland'
        WHEN 2 THEN 'Denver'
        WHEN 3 THEN 'Austin'
        WHEN 4 THEN 'Chicago'
        WHEN 5 THEN 'San Francisco'
        WHEN 6 THEN 'New York'
        WHEN 7 THEN 'Boston'
        WHEN 8 THEN 'Miami'
        ELSE 'Dallas'
    END                                                AS city
FROM generate_series(1, 500000000) AS gs;

-- Insert 500,000 products
INSERT INTO products (id, name, price, category)
SELECT
    gs.value                                           AS id,
    'Product ' || gs.value                             AS name,
    CAST(1 + (random() * 999) AS INT)                  AS price,
    CASE (gs.value % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Office'
        WHEN 2 THEN 'Home'
        WHEN 3 THEN 'Accessories'
        ELSE 'Other'
    END                                                AS category
FROM generate_series(1, 500000000) AS gs;

-- Insert 500,000 orders
INSERT INTO orders (id, customer_id, product_id, quantity, order_date)
SELECT
    gs.value                                                   AS id,
    CAST(random() * 500000 + 1 AS INT)                         AS customer_id,
    CAST(random() * 500000 + 1 AS INT)                         AS product_id,
    CAST(random() * 10 + 1 AS INT)                             AS quantity,
    CURRENT_DATE - (gs.value % 365)                            AS order_date
FROM generate_series(1, 500000000) AS gs;

FLUSH;

-- Join queries (examples to run against loaded data)

-- Orders with customer and product details
SELECT o.id, c.name AS customer_name, c.city, p.name AS product_name, p.category, o.quantity, o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.id <= 10;

-- Total order value by order (quantity * price)
SELECT o.id, c.name, p.name AS product, o.quantity, p.price, o.quantity * p.price AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.id <= 10;

-- Order count and total revenue per customer
SELECT c.id, c.name, c.city, COUNT(o.id) AS order_count, SUM(o.quantity * p.price) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
LEFT JOIN products p ON o.product_id = p.id
GROUP BY c.id, c.name, c.city
HAVING COUNT(o.id) > 0
LIMIT 20;

SELECT COUNT(*) FROM ( SELECT o.id, c.name AS customer_name, c.city, p.name AS product_name, p.category, o.quantity, o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.id <= 10);