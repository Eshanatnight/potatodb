CREATE TABLE categories (
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE products (
    id          INT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    category_id INT          NOT NULL REFERENCES categories(id),
    price       NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE users (
    id         INT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id         INT PRIMARY KEY,
    user_id    INT            NOT NULL REFERENCES users(id),
    total      NUMERIC(10, 2) NOT NULL CHECK (total >= 0),
    created_at TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
    id         INT PRIMARY KEY,
    order_id   INT            NOT NULL REFERENCES orders(id),
    product_id INT            NOT NULL REFERENCES products(id),
    quantity   INT            NOT NULL CHECK (quantity > 0),
    price      NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);


-- Categories
INSERT INTO categories (id, name) VALUES
    (1, 'Electronics'),
    (2, 'Clothing'),
    (3, 'Books'),
    (4, 'Home & Garden'),
    (5, 'Sports');

-- Products
INSERT INTO products (id, name, category_id, price) VALUES
    (1,  'Wireless Headphones',     1, 79.99),
    (2,  'Mechanical Keyboard',     1, 129.99),
    (3,  'USB-C Hub',               1, 49.99),
    (4,  'Running Shoes',           2, 89.99),
    (5,  'Hooded Sweatshirt',       2, 44.99),
    (6,  'Denim Jacket',            2, 74.99),
    (7,  'Clean Code',              3, 34.99),
    (8,  'The Pragmatic Programmer',3, 39.99),
    (9,  'Database Internals',      3, 49.99),
    (10, 'Succulent Plant Set',     4, 29.99),
    (11, 'Ceramic Plant Pots',      4, 24.99),
    (12, 'Yoga Mat',                5, 34.99),
    (13, 'Resistance Bands',        5, 19.99);

-- Users
INSERT INTO users (id, name, email, created_at) VALUES
    (1, 'Alice Johnson',  'alice@example.com',   '2024-01-15 09:00:00'),
    (2, 'Bob Smith',      'bob@example.com',     '2024-02-03 14:30:00'),
    (3, 'Carol White',    'carol@example.com',   '2024-02-20 11:15:00'),
    (4, 'David Brown',    'david@example.com',   '2024-03-08 16:45:00'),
    (5, 'Eve Davis',      'eve@example.com',     '2024-03-22 10:00:00'),
    (6, 'Frank Miller',   'frank@company.com',   '2024-04-01 08:30:00'),
    (7, 'Grace Wilson',   'grace@company.com',   '2024-04-10 13:00:00'),
    -- User with no orders (useful for LEFT JOIN / anti-join demos)
    (8, 'Henry Taylor',   'henry@example.com',   '2024-05-01 09:00:00');

-- Orders
INSERT INTO orders (id, user_id, total, created_at) VALUES
    (1, 1, 209.97, '2024-03-01 10:00:00'),  -- Alice,  order 1
    (2, 1,  49.99, '2024-04-15 12:30:00'),  -- Alice,  order 2
    (3, 2,  34.99, '2024-03-10 09:15:00'),  -- Bob,    order 3
    (4, 3, 164.98, '2024-03-18 14:00:00'),  -- Carol,  order 4
    (5, 4,  89.99, '2024-04-02 11:30:00'),  -- David,  order 5
    (6, 5,  54.98, '2024-04-20 15:00:00'),  -- Eve,    order 6
    (7, 6, 129.99, '2024-05-05 10:45:00'),  -- Frank,  order 7
    (8, 7,  79.99, '2024-05-12 16:00:00');  -- Grace,  order 8

-- Order Items
INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES
    -- Order 1: Alice buys headphones + keyboard + hub
    (1, 1, 1, 1,  79.99),
    (2, 1, 2, 1, 129.99),
    (3, 1, 3, 1,  49.99),
    -- Order 2: Alice buys another USB-C Hub
    (4, 2, 3, 1,  49.99),
    -- Order 3: Bob buys Clean Code
    (5, 3, 7, 1,  34.99),
    -- Order 4: Carol buys running shoes + denim jacket
    (6, 4, 4, 1,  89.99),
    (7, 4, 6, 1,  74.99),
    -- Order 5: David buys running shoes
    (8, 5, 4, 1,  89.99),
    -- Order 6: Eve buys yoga mat + resistance bands
    (9,  6, 12, 1, 34.99),
    (10, 6, 13, 1, 19.99),
    -- Order 7: Frank buys mechanical keyboard
    (11, 7, 2, 1, 129.99),
    -- Order 8: Grace buys wireless headphones
    (12, 8, 1, 1,  79.99);
