"""Seeds the demo PostgreSQL database (running in Docker) used by the Presto connector.

The PostgreSQL container must be running before this script is executed:
    docker start presto-postgres
"""
import time
import psycopg2

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "demo"
PG_USER = "presto"
PG_PASS = "presto"


def connect(retries: int = 20) -> psycopg2.extensions.connection:
    """Wait up to retries*2 seconds for PostgreSQL to accept connections."""
    for attempt in range(retries):
        try:
            return psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS,
            )
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(2)


def main():
    print("Connecting to PostgreSQL...")
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    # customers
    cur.execute("DROP TABLE IF EXISTS customer_orders")
    cur.execute("DROP TABLE IF EXISTS customers")

    cur.execute("""
        CREATE TABLE customers (
            id      SERIAL PRIMARY KEY,
            name    TEXT NOT NULL,
            email   TEXT NOT NULL,
            country TEXT NOT NULL,
            tier    TEXT NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO customers (name, email, country, tier) VALUES (%s, %s, %s, %s)",
        [
            ("Acme Corp",    "acme@example.com",     "USA",     "Enterprise"),
            ("Globex Inc",   "globex@example.com",   "USA",     "Enterprise"),
            ("Initech",      "initech@example.com",  "USA",     "SMB"),
            ("Umbrella Ltd", "umbrella@example.com", "UK",      "Enterprise"),
            ("Soylent Co",   "soylent@example.com",  "Canada",  "SMB"),
            ("Cyberdyne",    "cyber@example.com",    "USA",     "Enterprise"),
            ("Tyrell Corp",  "tyrell@example.com",   "USA",     "SMB"),
            ("Oscorp",       "oscorp@example.com",   "USA",     "SMB"),
        ],
    )

    # customer_orders
    # product_id references products.id in the SQLite database (the cross-catalog link).
    cur.execute("""
        CREATE TABLE customer_orders (
            id           SERIAL PRIMARY KEY,
            customer_id  INTEGER       NOT NULL REFERENCES customers(id),
            product_id   INTEGER       NOT NULL,
            quantity     INTEGER       NOT NULL,
            order_date   DATE          NOT NULL,
            total_amount NUMERIC(10,2) NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO customer_orders"
        " (customer_id, product_id, quantity, order_date, total_amount)"
        " VALUES (%s, %s, %s, %s, %s)",
        [
            (1, 1,  5,  "2024-01-05",  6499.95),
            (1, 6,  10, "2024-01-20",  4499.90),
            (2, 1,  3,  "2024-02-10",  3899.97),
            (2, 3,  6,  "2024-02-15",  3294.00),
            (3, 7,  8,  "2024-03-01",   719.92),
            (3, 2,  20, "2024-03-10",   599.80),
            (4, 1,  10, "2024-04-01", 12999.90),
            (4, 4,  4,  "2024-04-12",  1596.00),
            (5, 5,  15, "2024-05-05",   899.85),
            (5, 9,  8,  "2024-05-20",   639.92),
            (6, 1,  7,  "2024-06-01",  9099.93),
            (6, 6,  5,  "2024-06-15",  2249.95),
            (7, 7,  12, "2024-07-01",  1079.88),
            (7, 10, 50, "2024-07-10",   499.50),
            (8, 2,  30, "2024-08-01",   899.70),
            (8, 8,  10, "2024-08-15",   350.00),
        ],
    )

    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL demo database seeded successfully.")
    print(f"  customers       - 8 rows")
    print(f"  customer_orders - 16 rows  (product_id links to SQLite products)")


if __name__ == "__main__":
    main()
