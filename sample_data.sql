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
FROM generate_series(1, 5000000) AS gs;

-- Insert 500,000 products
INSERT INTO products (id, name, price, category)
SELECT
    gs.value                                           AS id,
    'Product ' || gs.value                             AS name,
    1 + ((gs.value * 7919 + 104729) % 999)              AS price,
    CASE (gs.value % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Office'
        WHEN 2 THEN 'Home'
        WHEN 3 THEN 'Accessories'
        ELSE 'Other'
    END                                                AS category
FROM generate_series(1, 5000000) AS gs;

-- Insert 500,000 orders
INSERT INTO orders (id, customer_id, product_id, quantity, order_date)
SELECT
    gs.value                                                   AS id,
    ((gs.value * 104729 + 1) % 5000000) + 1                     AS customer_id,
    ((gs.value * 7919 + 17) % 5000000) + 1                     AS product_id,
    (gs.value % 10) + 1                                        AS quantity,
    CURRENT_DATE - (gs.value % 365)                            AS order_date
FROM generate_series(1, 5000000) AS gs;


-- ============================================================
-- Transactions
-- ============================================================

BEGIN;
UPDATE products SET price = 999.99 WHERE id = 2;
UPDATE products SET price = 888.88 WHERE id = 3;
COMMIT;

SELECT id, name, price FROM products WHERE id IN (2, 3);

BEGIN;
UPDATE products SET price = 0.01 WHERE id = 2;
ROLLBACK;

SELECT id, name, price FROM products WHERE id = 2;

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

-- ============================================================
-- DISTINCT + ORDER BY + OFFSET
-- ============================================================

SELECT DISTINCT city FROM customers ORDER BY city LIMIT 10;

SELECT DISTINCT category FROM products ORDER BY category DESC;

SELECT o.id, c.name, p.name AS product
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
ORDER BY o.id
LIMIT 10 OFFSET 5;

-- ============================================================
-- Filtering: BETWEEN, LIKE, IN, IS NULL, NOT IN
-- ============================================================

SELECT * FROM products WHERE price BETWEEN 100 AND 200 LIMIT 20;

SELECT * FROM customers WHERE name LIKE 'Customer 42%' LIMIT 10;

SELECT * FROM customers WHERE city IN ('Seattle', 'Austin', 'Miami') LIMIT 20;

SELECT * FROM customers WHERE city NOT IN ('Seattle', 'Portland') LIMIT 10;

SELECT c.id, c.name, c.city
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.id IS NULL
LIMIT 10;

SELECT c.id, c.name, c.city
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.id IS NOT NULL
LIMIT 10;

-- ============================================================
-- Aggregations: AVG, MIN, MAX, COUNT DISTINCT
-- ============================================================

SELECT
    category,
    COUNT(*) AS num_products,
    MIN(price) AS cheapest,
    MAX(price) AS most_expensive,
    AVG(price) AS avg_price
FROM products
GROUP BY category
ORDER BY avg_price DESC;

SELECT COUNT(DISTINCT city) AS unique_cities FROM customers;

SELECT COUNT(DISTINCT category) AS unique_categories FROM products;

SELECT
    c.city,
    COUNT(o.id) AS total_orders,
    SUM(o.quantity) AS total_items,
    AVG(o.quantity) AS avg_qty_per_order
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.city
ORDER BY total_orders DESC
LIMIT 10;

-- ============================================================
-- Subqueries: scalar, WHERE IN, EXISTS, correlated
-- ============================================================

SELECT id, name, price,
    (SELECT AVG(price) FROM products) AS global_avg
FROM products
WHERE id <= 10;

SELECT * FROM customers
WHERE id IN (SELECT customer_id FROM orders WHERE quantity >= 10)
LIMIT 20;

SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.quantity > 8
)
LIMIT 20;

SELECT * FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
)
LIMIT 10;

SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products)
LIMIT 20;

-- ============================================================
-- CTEs (Common Table Expressions)
-- ============================================================

WITH high_value_orders AS (
    SELECT o.id, o.customer_id, o.product_id, o.quantity, p.price,
           o.quantity * p.price AS order_total
    FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE o.quantity * p.price > 5000
)
SELECT c.name, c.city, h.order_total
FROM high_value_orders h
JOIN customers c ON h.customer_id = c.id
ORDER BY h.order_total DESC
LIMIT 20;

WITH city_stats AS (
    SELECT c.city, COUNT(o.id) AS order_count, SUM(o.quantity * p.price) AS revenue
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    JOIN products p ON o.product_id = p.id
    GROUP BY c.city
),
ranked AS (
    SELECT city, order_count, revenue,
           RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
    FROM city_stats
)
SELECT * FROM ranked ORDER BY revenue_rank LIMIT 10;

-- ============================================================
-- Window Functions
-- ============================================================

SELECT id, name, price, category,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
FROM products
WHERE id <= 100
ORDER BY category, rn;

SELECT id, name, price, category,
    RANK() OVER (ORDER BY price DESC) AS price_rank,
    DENSE_RANK() OVER (ORDER BY price DESC) AS price_dense_rank
FROM products
WHERE id <= 50;

SELECT id, name, price,
    LAG(price, 1) OVER (ORDER BY id) AS prev_price,
    LEAD(price, 1) OVER (ORDER BY id) AS next_price
FROM products
WHERE id <= 20;

SELECT id, name, price,
    SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running_total
FROM products
WHERE id <= 20;

SELECT category, id, price,
    AVG(price) OVER (PARTITION BY category) AS category_avg,
    price - AVG(price) OVER (PARTITION BY category) AS diff_from_avg
FROM products
WHERE id <= 50
ORDER BY category, id;

-- ============================================================
-- Set Operations: UNION, UNION ALL, INTERSECT, EXCEPT
-- ============================================================

SELECT city FROM customers WHERE id <= 100
UNION
SELECT category FROM products WHERE id <= 100;

SELECT city AS label FROM customers WHERE id <= 50
UNION ALL
SELECT city AS label FROM customers WHERE id BETWEEN 25 AND 75;

SELECT city FROM customers WHERE id <= 500
INTERSECT
SELECT city FROM customers WHERE id BETWEEN 250 AND 750;

SELECT city FROM customers WHERE id <= 500
EXCEPT
SELECT city FROM customers WHERE id BETWEEN 1 AND 100;

-- ============================================================
-- Additional JOIN types: RIGHT JOIN, CROSS JOIN
-- ============================================================

SELECT p.id, p.name, p.category, o.id AS order_id
FROM orders o
RIGHT JOIN products p ON o.product_id = p.id
WHERE p.id <= 10;

SELECT c.name, p.name AS product
FROM (SELECT * FROM customers WHERE id <= 3) c
CROSS JOIN (SELECT * FROM products WHERE id <= 3) p;

-- ============================================================
-- Complex CASE expressions
-- ============================================================

SELECT id, name, price,
    CASE
        WHEN price < 100 THEN 'Budget'
        WHEN price < 500 THEN 'Mid-Range'
        WHEN price < 800 THEN 'Premium'
        ELSE 'Luxury'
    END AS price_tier
FROM products
WHERE id <= 20;

SELECT c.city,
    COUNT(o.id) AS orders,
    CASE
        WHEN COUNT(o.id) > 100 THEN 'High'
        WHEN COUNT(o.id) > 50 THEN 'Medium'
        ELSE 'Low'
    END AS activity_level
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.city
ORDER BY orders DESC;

-- ============================================================
-- HAVING with various aggregate conditions
-- ============================================================

SELECT category, COUNT(*) AS cnt
FROM products
GROUP BY category
HAVING COUNT(*) > 500000;

SELECT c.city, AVG(p.price) AS avg_order_price
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN products p ON o.product_id = p.id
GROUP BY c.city
HAVING AVG(p.price) > 400
ORDER BY avg_order_price DESC;

-- ============================================================
-- Arithmetic and computed columns
-- ============================================================

SELECT o.id,
    o.quantity,
    p.price,
    o.quantity * p.price AS subtotal,
    ROUND(o.quantity * p.price * 0.08, 2) AS tax,
    ROUND(o.quantity * p.price * 1.08, 2) AS total_with_tax
FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.id <= 10;

SELECT
    category,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(MIN(price) * 1.0 / MAX(price), 4) AS price_ratio
FROM products
GROUP BY category;

-- ============================================================
-- Nested aggregation via subquery
-- ============================================================

SELECT AVG(order_count) AS avg_orders_per_customer
FROM (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
) sub;

SELECT MAX(city_revenue) AS top_city_revenue, MIN(city_revenue) AS bottom_city_revenue
FROM (
    SELECT c.city, SUM(o.quantity * p.price) AS city_revenue
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    JOIN products p ON o.product_id = p.id
    GROUP BY c.city
) sub;

-- ============================================================
-- COALESCE / NULLIF
-- ============================================================

SELECT c.id, c.name,
    COALESCE(c.city, 'Unknown') AS city
FROM customers c
WHERE c.id <= 10;

SELECT id, name, NULLIF(city, 'Seattle') AS city_non_seattle
FROM customers
WHERE id <= 20;

-- ============================================================
-- EXPLAIN
-- ============================================================

EXPLAIN SELECT o.id, c.name, p.name, o.quantity * p.price AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.id <= 100;

-- ============================================================
-- DDL: ALTER TABLE, CREATE INDEX, CREATE/DROP VIEW
-- ============================================================

ALTER TABLE customers ADD COLUMN signup_date DATE;

CREATE INDEX idx_orders_customer ON orders (customer_id);

CREATE INDEX idx_orders_product ON orders (product_id);

CREATE VIEW top_customers AS
SELECT c.id, c.name, c.city, COUNT(o.id) AS order_count, SUM(o.quantity * p.price) AS total_spent
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN products p ON o.product_id = p.id
GROUP BY c.id, c.name, c.city
ORDER BY total_spent DESC
LIMIT 100;

SELECT * FROM top_customers LIMIT 10;

DROP VIEW top_customers;

-- ============================================================
-- DML: UPDATE, DELETE
-- ============================================================

UPDATE customers SET city = 'Los Angeles' WHERE id = 1;

SELECT id, name, city FROM customers WHERE id = 1;

UPDATE customers SET signup_date = CURRENT_DATE WHERE id <= 10;

DELETE FROM orders WHERE id = 1;

SELECT COUNT(*) FROM orders;

-- ============================================================
-- Upsert: INSERT ... ON CONFLICT
-- ============================================================

INSERT INTO customers (id, name, email, city)
VALUES (1, 'Customer 1 Updated', 'customer_1_updated@example.com', 'Los Angeles')
ON CONFLICT (id) DO UPDATE SET name = 'Customer 1 Updated', email = 'customer_1_updated@example.com';

SELECT id, name, email, city FROM customers WHERE id = 1;

INSERT INTO products (id, name, price, category)
VALUES (99999999, 'Phantom Product', 0.01, 'Test')
ON CONFLICT (id) DO NOTHING;

SELECT * FROM products WHERE id = 99999999;

-- ============================================================
-- TRUNCATE + re-verify
-- ============================================================

CREATE TABLE temp_test (id INT PRIMARY KEY, val VARCHAR(50));
INSERT INTO temp_test (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT COUNT(*) FROM temp_test;
TRUNCATE TABLE temp_test;
SELECT COUNT(*) FROM temp_test;
DROP TABLE temp_test;

-- ============================================================
-- Self-join: customers in the same city as customer 1
-- ============================================================

SELECT a.id AS customer_a, b.id AS customer_b, a.city
FROM customers a
JOIN customers b ON a.city = b.city AND a.id < b.id
WHERE a.id = 1
LIMIT 20;

-- ============================================================
-- Top-N per group: top 3 most expensive products per category
-- ============================================================

SELECT category, id, name, price
FROM (
    SELECT category, id, name, price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
    FROM products
) ranked
WHERE rn <= 3
ORDER BY category, rn;

-- ============================================================
-- Sliding window: 3-row moving average of price
-- ============================================================

SELECT id, name, price,
    AVG(price) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg_3,
    SUM(price) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trailing_sum_3,
    MIN(price) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS window_min_7
FROM products
WHERE id <= 30
ORDER BY id;

-- ============================================================
-- FULL OUTER JOIN
-- ============================================================

SELECT
    COALESCE(c.id, -1) AS customer_id,
    COALESCE(p.id, -1) AS product_id,
    c.name AS customer_name,
    p.name AS product_name
FROM (SELECT * FROM customers WHERE id <= 5) c
FULL OUTER JOIN (SELECT * FROM products WHERE id BETWEEN 3 AND 8) p
    ON c.id = p.id
ORDER BY COALESCE(c.id, p.id);

-- ============================================================
-- Pivot-style: order counts per city x category
-- ============================================================

SELECT c.city,
    SUM(CASE WHEN p.category = 'Electronics' THEN 1 ELSE 0 END) AS electronics,
    SUM(CASE WHEN p.category = 'Office'      THEN 1 ELSE 0 END) AS office,
    SUM(CASE WHEN p.category = 'Home'        THEN 1 ELSE 0 END) AS home,
    SUM(CASE WHEN p.category = 'Accessories' THEN 1 ELSE 0 END) AS accessories,
    SUM(CASE WHEN p.category = 'Other'       THEN 1 ELSE 0 END) AS other,
    COUNT(*) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products  p ON o.product_id  = p.id
GROUP BY c.city
ORDER BY total DESC;

-- ============================================================
-- Multi-CTE analytics pipeline
-- ============================================================

WITH order_values AS (
    SELECT o.id AS order_id, o.customer_id, o.product_id,
           o.quantity, p.price, p.category,
           o.quantity * p.price AS order_total,
           o.order_date
    FROM orders o
    JOIN products p ON o.product_id = p.id
),
customer_summary AS (
    SELECT customer_id,
           COUNT(*)            AS num_orders,
           SUM(order_total)    AS lifetime_value,
           AVG(order_total)    AS avg_order_value,
           MIN(order_date)     AS first_order,
           MAX(order_date)     AS last_order
    FROM order_values
    GROUP BY customer_id
),
customer_segments AS (
    SELECT cs.*,
        CASE
            WHEN lifetime_value > 50000 THEN 'VIP'
            WHEN lifetime_value > 10000 THEN 'Regular'
            ELSE 'Occasional'
        END AS segment,
        NTILE(10) OVER (ORDER BY lifetime_value DESC) AS decile
    FROM customer_summary cs
)
SELECT seg.segment,
       COUNT(*)                    AS customer_count,
       ROUND(AVG(seg.lifetime_value), 2) AS avg_ltv,
       ROUND(AVG(seg.num_orders), 2)     AS avg_orders,
       MIN(seg.decile)             AS best_decile,
       MAX(seg.decile)             AS worst_decile
FROM customer_segments seg
GROUP BY seg.segment
ORDER BY avg_ltv DESC;

-- ============================================================
-- Deeply nested subqueries (3 levels)
-- ============================================================

SELECT id, name, city
FROM customers
WHERE id IN (
    SELECT customer_id FROM orders
    WHERE product_id IN (
        SELECT id FROM products
        WHERE price > (
            SELECT AVG(price) * 1.5 FROM products
        )
    )
)
LIMIT 20;

-- ============================================================
-- Correlated subquery with aggregate in SELECT list
-- ============================================================

-- SELECT c.id, c.name, c.city,
--     (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count,
--     (SELECT COALESCE(SUM(o2.quantity), 0) FROM orders o2 WHERE o2.customer_id = c.id) AS total_qty
-- FROM customers c
-- WHERE c.id <= 20
-- ORDER BY c.id;

-- ============================================================
-- Subquery joined to subquery (derived table join)
-- ============================================================

SELECT cs.city, cs.customer_count, os.city_orders, os.city_revenue
FROM (
    SELECT city, COUNT(*) AS customer_count
    FROM customers
    GROUP BY city
) cs
JOIN (
    SELECT c.city, COUNT(o.id) AS city_orders, SUM(o.quantity * p.price) AS city_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products  p ON o.product_id  = p.id
    GROUP BY c.city
) os ON cs.city = os.city
ORDER BY os.city_revenue DESC;

-- ============================================================
-- Complex predicate: nested AND/OR
-- ============================================================

SELECT o.id, c.name, c.city, p.name AS product, p.category, p.price, o.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products  p ON o.product_id  = p.id
WHERE (
    (c.city IN ('Seattle', 'Austin') AND p.category = 'Electronics' AND p.price > 500)
    OR
    (c.city = 'New York' AND o.quantity >= 8)
    OR
    (p.category = 'Home' AND p.price BETWEEN 200 AND 400 AND c.city <> 'Dallas')
)
LIMIT 30;

-- ============================================================
-- Window function + CASE + aggregate combo
-- ============================================================

SELECT category, price_tier, cnt,
    SUM(cnt) OVER (PARTITION BY category ORDER BY price_tier) AS cumulative,
    ROUND(cnt * 100.0 / SUM(cnt) OVER (PARTITION BY category), 2) AS pct_of_category
FROM (
    SELECT category,
        CASE
            WHEN price < 250 THEN '1-Budget'
            WHEN price < 500 THEN '2-Mid'
            WHEN price < 750 THEN '3-High'
            ELSE '4-Premium'
        END AS price_tier,
        COUNT(*) AS cnt
    FROM products
    GROUP BY category,
        CASE
            WHEN price < 250 THEN '1-Budget'
            WHEN price < 500 THEN '2-Mid'
            WHEN price < 750 THEN '3-High'
            ELSE '4-Premium'
        END
) sub
ORDER BY category, price_tier;

-- ============================================================
-- UNION inside CTE
-- ============================================================

WITH all_names AS (
    SELECT 'customer' AS entity_type, id, name FROM customers WHERE id <= 10
    UNION ALL
    SELECT 'product'  AS entity_type, id, name FROM products  WHERE id <= 10
)
SELECT entity_type, COUNT(*) AS cnt, MIN(id) AS min_id, MAX(id) AS max_id
FROM all_names
GROUP BY entity_type;

-- ============================================================
-- Window filter pattern: percentile-based filtering
-- ============================================================

SELECT id, name, price, category, price_rank
FROM (
    SELECT id, name, price, category,
        PERCENT_RANK() OVER (ORDER BY price DESC) AS price_rank
    FROM products
) sub
WHERE price_rank <= 0.01
ORDER BY price DESC
LIMIT 20;

-- ============================================================
-- Multiple aggregates with HAVING on different columns
-- ============================================================

SELECT c.city, p.category,
    COUNT(*)          AS order_count,
    SUM(o.quantity)   AS total_qty,
    AVG(p.price)      AS avg_price,
    MAX(o.quantity * p.price) AS largest_order
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products  p ON o.product_id  = p.id
GROUP BY c.city, p.category
HAVING COUNT(*) > 10000 AND AVG(p.price) > 300
ORDER BY order_count DESC
LIMIT 20;

-- ============================================================
-- Anti-join: products never ordered
-- ============================================================

SELECT p.id, p.name, p.price, p.category
FROM products p
LEFT JOIN orders o ON p.id = o.product_id
WHERE o.id IS NULL
LIMIT 20;

-- ============================================================
-- CTAS: CREATE TABLE AS SELECT
-- ============================================================

CREATE TABLE customer_order_summary AS
SELECT c.id, c.name, c.city,
       COUNT(o.id) AS order_count,
       COALESCE(SUM(o.quantity * p.price), 0) AS total_revenue
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
LEFT JOIN products p ON o.product_id = p.id
GROUP BY c.id, c.name, c.city;

SELECT * FROM customer_order_summary ORDER BY total_revenue DESC LIMIT 10;

DROP TABLE customer_order_summary;

-- ============================================================
-- MERGE: upsert via MERGE INTO
-- ============================================================

CREATE TABLE product_stats (
    category VARCHAR(50) PRIMARY KEY,
    product_count INT NOT NULL,
    avg_price DECIMAL(10, 2) NOT NULL
);

MERGE INTO product_stats t
USING (
    SELECT category, COUNT(*) AS product_count, AVG(price) AS avg_price
    FROM products
    GROUP BY category
) s ON t.category = s.category
WHEN MATCHED THEN UPDATE SET product_count = s.product_count, avg_price = s.avg_price
WHEN NOT MATCHED THEN INSERT (category, product_count, avg_price) VALUES (s.category, s.product_count, s.avg_price);

SELECT * FROM product_stats ORDER BY category;

DROP TABLE product_stats;

-- ============================================================
-- Prepared statements
-- ============================================================

PREPARE get_orders_by_city(text) AS
SELECT o.id, c.name, c.city, p.name AS product, o.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products  p ON o.product_id  = p.id
WHERE c.city = $1
LIMIT 10;

EXECUTE get_orders_by_city('Seattle');
EXECUTE get_orders_by_city('Chicago');

-- ============================================================
-- Date arithmetic and filtering
-- ============================================================

SELECT id, customer_id, product_id, quantity, order_date,
    CURRENT_DATE - order_date AS days_ago
FROM orders
WHERE order_date >= CURRENT_DATE - 30
LIMIT 20;

SELECT order_date, COUNT(*) AS daily_orders, SUM(quantity) AS daily_items
FROM orders
WHERE order_date >= CURRENT_DATE - 7
GROUP BY order_date
ORDER BY order_date;

-- ============================================================
-- Multi-window: ranking across multiple dimensions
-- ============================================================

SELECT id, name, price, category,
    ROW_NUMBER() OVER (ORDER BY price DESC)                          AS global_rank,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC)    AS category_rank,
    ROUND(price - AVG(price) OVER (), 2)                             AS diff_from_global_avg,
    ROUND(price - AVG(price) OVER (PARTITION BY category), 2)        AS diff_from_cat_avg,
    ROUND(price * 1.0 / MAX(price) OVER (PARTITION BY category), 4)  AS pct_of_cat_max
FROM products
WHERE id <= 50
ORDER BY category, category_rank;

-- ============================================================
-- Complex CTE + window: customer retention cohorts
-- ============================================================

-- WITH customer_first_order AS (
--     SELECT customer_id, MIN(order_date) AS cohort_date
--     FROM orders
--     GROUP BY customer_id
-- ),
-- customer_activity AS (
--     SELECT o.customer_id, cfo.cohort_date, o.order_date,
--            o.order_date - cfo.cohort_date AS days_since_first
--     FROM orders o
--     JOIN customer_first_order cfo ON o.customer_id = cfo.customer_id
-- )
-- SELECT cohort_date,
--     COUNT(DISTINCT customer_id)                                                      AS cohort_size,
--     COUNT(DISTINCT CASE WHEN days_since_first BETWEEN 1 AND 30  THEN customer_id END) AS returned_30d,
--     COUNT(DISTINCT CASE WHEN days_since_first BETWEEN 1 AND 90  THEN customer_id END) AS returned_90d,
--     COUNT(DISTINCT CASE WHEN days_since_first BETWEEN 1 AND 180 THEN customer_id END) AS returned_180d
-- FROM customer_activity
-- GROUP BY cohort_date
-- ORDER BY cohort_date
-- LIMIT 15;

-- ============================================================
-- Deeply nested: top spenders who buy above-average-priced products
-- in their city's most popular category
-- ============================================================

WITH city_top_category AS (
    SELECT city, category, order_count,
           ROW_NUMBER() OVER (PARTITION BY city ORDER BY order_count DESC) AS rn
    FROM (
        SELECT c.city, p.category, COUNT(*) AS order_count
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products  p ON o.product_id  = p.id
        GROUP BY c.city, p.category
    ) sub
),
target_combos AS (
    SELECT city, category FROM city_top_category WHERE rn = 1
)
SELECT c.id, c.name, c.city, tc.category AS top_category,
       COUNT(o.id) AS orders_in_top_cat,
       ROUND(AVG(p.price), 2) AS avg_price_paid
FROM customers c
JOIN target_combos tc ON c.city = tc.city
JOIN orders o ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id AND p.category = tc.category
WHERE p.price > (SELECT AVG(price) FROM products WHERE category = tc.category)
GROUP BY c.id, c.name, c.city, tc.category
HAVING COUNT(o.id) >= 3
ORDER BY orders_in_top_cat DESC
LIMIT 20;

-- ============================================================
-- String concatenation in complex context
-- ============================================================

SELECT
    c.city || ' - ' || p.category AS market_segment,
    COUNT(*)                      AS orders,
    ROUND(AVG(o.quantity * p.price), 2) AS avg_order_value,
    MIN(o.order_date) || ' to ' || MAX(o.order_date) AS date_range
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products  p ON o.product_id  = p.id
GROUP BY c.city, p.category
HAVING COUNT(*) > 5000
ORDER BY avg_order_value DESC
LIMIT 20;

-- ============================================================
-- Mixed set operations with ORDER BY
-- ============================================================

(SELECT city AS name, 'city' AS type FROM customers GROUP BY city)
UNION ALL
(SELECT category AS name, 'category' AS type FROM products GROUP BY category)
ORDER BY type, name;

-- ============================================================
-- Window: running distinct count approximation via dense_rank
-- ============================================================

SELECT order_date,
    COUNT(*)                                           AS daily_orders,
    SUM(COUNT(*)) OVER (ORDER BY order_date)           AS cumulative_orders,
    SUM(quantity)                                       AS daily_qty,
    SUM(SUM(quantity)) OVER (ORDER BY order_date)       AS cumulative_qty
FROM orders
WHERE order_date >= CURRENT_DATE - 14
GROUP BY order_date
ORDER BY order_date;

-- ============================================================
-- EXISTS with correlated aggregate threshold
-- ============================================================

SELECT c.id, c.name, c.city
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE o.customer_id = c.id
    GROUP BY o.customer_id
    HAVING SUM(o.quantity * p.price) > 20000
)
LIMIT 20;

-- ============================================================
-- Cleanup: undo test mutations so data stays consistent
-- ============================================================

ALTER TABLE customers DROP COLUMN signup_date;

DROP INDEX idx_orders_customer;

DROP INDEX idx_orders_product;

DELETE FROM products WHERE id = 99999999;
