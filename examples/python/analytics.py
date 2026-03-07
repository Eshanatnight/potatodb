"""
PotatoDB Python example: analytics queries.

Build the Python bindings first:
    cd crates/python
    maturin develop --release

Then run:
    python examples/python/analytics.py
"""

from potatodb_python import PotatoDB


def main():
    db = PotatoDB.open("/tmp/potatodb_python_example")

    # ── Setup ──────────────────────────────────────────────
    db.execute("CREATE TABLE IF NOT EXISTS products (id INT, name VARCHAR, category VARCHAR, price DOUBLE);")
    db.execute("DELETE FROM products;")

    db.execute("""
        INSERT INTO products VALUES
            (1, 'Laptop',       'Electronics', 999.99),
            (2, 'Headphones',   'Electronics', 149.99),
            (3, 'Coffee Maker', 'Kitchen',     79.99),
            (4, 'Blender',      'Kitchen',     45.50),
            (5, 'Monitor',      'Electronics', 349.00),
            (6, 'Toaster',      'Kitchen',     29.99),
            (7, 'Keyboard',     'Electronics',  69.99),
            (8, 'Knife Set',    'Kitchen',     120.00);
    """)

    # ── Basic query ────────────────────────────────────────
    print("=== All Products ===")
    rows = db.execute("SELECT * FROM products ORDER BY id;")
    for row in rows:
        print(f"  {row['id']:>2}. {row['name']:<15} {row['category']:<12} ${row['price']:>8.2f}")

    # ── Aggregation by category ────────────────────────────
    print("\n=== Category Summary ===")
    rows = db.execute("""
        SELECT category,
               COUNT(*)  AS items,
               ROUND(AVG(price), 2) AS avg_price,
               MIN(price) AS cheapest,
               MAX(price) AS priciest
        FROM products
        GROUP BY category
        ORDER BY avg_price DESC;
    """)
    for row in rows:
        print(f"  {row['category']:<12} "
              f"items={row['items']}  "
              f"avg=${row['avg_price']:.2f}  "
              f"range=${row['cheapest']:.2f}-${row['priciest']:.2f}")

    # ── Filtered query ─────────────────────────────────────
    print("\n=== Electronics over $100 ===")
    rows = db.execute("""
        SELECT name, price
        FROM products
        WHERE category = 'Electronics' AND price > 100
        ORDER BY price DESC;
    """)
    for row in rows:
        print(f"  {row['name']:<15} ${row['price']:.2f}")

    # ── Top N with LIMIT ───────────────────────────────────
    print("\n=== Top 3 Most Expensive ===")
    rows = db.execute("SELECT name, price FROM products ORDER BY price DESC LIMIT 3;")
    for i, row in enumerate(rows, 1):
        print(f"  {i}. {row['name']} - ${row['price']:.2f}")

    # ── Cleanup ────────────────────────────────────────────
    result = db.execute("DROP TABLE products;")
    print(f"\n{result}")

    db.close()
    print("Done!")


if __name__ == "__main__":
    main()
