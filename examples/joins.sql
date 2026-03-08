-- Example SQL Queries: JOIN Types
-- Assumes a simple e-commerce schema:
--   users(id, name, email)
--   orders(id, user_id, created_at, total)
--   order_items(id, order_id, product_id, quantity, price)
--   products(id, name, category_id, price)
--   categories(id, name)

-- ─────────────────────────────────────────
-- INNER JOIN
-- Returns only rows with matching keys in both tables
-- ─────────────────────────────────────────
SELECT
    u.name        AS customer,
    o.id          AS order_id,
    o.total,
    o.created_at
FROM users u
INNER JOIN orders o ON o.user_id = u.id;


-- ─────────────────────────────────────────
-- LEFT JOIN
-- Returns all users, even those with no orders
-- ─────────────────────────────────────────
SELECT
    u.name        AS customer,
    COUNT(o.id)   AS order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name;


-- ─────────────────────────────────────────
-- RIGHT JOIN
-- Returns all orders, even if the user no longer exists
-- ─────────────────────────────────────────
SELECT
    u.name        AS customer,
    o.id          AS order_id,
    o.total
FROM users u
RIGHT JOIN orders o ON o.user_id = u.id;


-- ─────────────────────────────────────────
-- FULL OUTER JOIN
-- Returns all users and all orders, matched where possible
-- ─────────────────────────────────────────
SELECT
    u.name        AS customer,
    o.id          AS order_id,
    o.total
FROM users u
FULL OUTER JOIN orders o ON o.user_id = u.id;


-- ─────────────────────────────────────────
-- CROSS JOIN
-- Every user paired with every product (cartesian product)
-- ─────────────────────────────────────────
SELECT
    u.name        AS customer,
    p.name        AS product
FROM users u
CROSS JOIN products p;


-- ─────────────────────────────────────────
-- SELF JOIN
-- Find users who share the same email domain
-- ─────────────────────────────────────────
SELECT
    a.name AS user_a,
    b.name AS user_b,
    SUBSTRING(a.email, POSITION('@' IN a.email)) AS shared_domain
FROM users a
INNER JOIN users b
    ON  SUBSTRING(a.email, POSITION('@' IN a.email))
      = SUBSTRING(b.email, POSITION('@' IN b.email))
    AND a.id < b.id;


-- ─────────────────────────────────────────
-- MULTI-TABLE JOIN
-- Full order details: customer, product, category, line total
-- ─────────────────────────────────────────
SELECT
    u.name                         AS customer,
    o.id                           AS order_id,
    o.created_at,
    p.name                         AS product,
    c.name                         AS category,
    oi.quantity,
    oi.price,
    (oi.quantity * oi.price)       AS line_total
FROM orders o
INNER JOIN users        u  ON u.id  = o.user_id
INNER JOIN order_items  oi ON oi.order_id = o.id
INNER JOIN products     p  ON p.id  = oi.product_id
INNER JOIN categories   c  ON c.id  = p.category_id
ORDER BY o.created_at DESC, o.id, oi.id;


-- ─────────────────────────────────────────
-- LEFT JOIN with NULL filter (anti-join pattern)
-- Find users who have never placed an order
-- ─────────────────────────────────────────
SELECT
    u.id,
    u.name,
    u.email
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
WHERE o.id IS NULL;
