"""Creates the SQLite database (mock_data.db) used by the Presto connector."""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_data.db")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE departments (
            id        INTEGER PRIMARY KEY,
            name      TEXT    NOT NULL,
            budget    REAL    NOT NULL,
            location  TEXT    NOT NULL
        )
    """)
    cur.executemany("INSERT INTO departments VALUES (?, ?, ?, ?)", [
        (1, "Engineering",  1500000.00, "San Francisco"),
        (2, "Marketing",     800000.00, "New York"),
        (3, "Sales",        1200000.00, "Chicago"),
        (4, "HR",            500000.00, "San Francisco"),
        (5, "Finance",       900000.00, "New York"),
    ])

    cur.execute("""
        CREATE TABLE employees (
            id             INTEGER PRIMARY KEY,
            first_name     TEXT    NOT NULL,
            last_name      TEXT    NOT NULL,
            email          TEXT    NOT NULL,
            department_id  INTEGER NOT NULL,
            salary         REAL    NOT NULL,
            hire_date      TEXT    NOT NULL,
            is_active      INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (department_id) REFERENCES departments(id)
        )
    """)
    cur.executemany("INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
        (1,  "Alice",   "Johnson",  "alice@example.com",    1, 130000, "2020-03-15", 1),
        (2,  "Bob",     "Smith",    "bob@example.com",      1, 125000, "2019-07-22", 1),
        (3,  "Carol",   "Williams", "carol@example.com",    2,  95000, "2021-01-10", 1),
        (4,  "David",   "Brown",    "david@example.com",    3, 105000, "2018-11-05", 1),
        (5,  "Eve",     "Davis",    "eve@example.com",      3,  98000, "2020-06-18", 1),
        (6,  "Frank",   "Miller",   "frank@example.com",    4,  85000, "2022-02-28", 1),
        (7,  "Grace",   "Wilson",   "grace@example.com",    1, 140000, "2017-09-12", 1),
        (8,  "Henry",   "Moore",    "henry@example.com",    5, 115000, "2019-04-03", 1),
        (9,  "Iris",    "Taylor",   "iris@example.com",     2,  88000, "2023-08-14", 1),
        (10, "Jack",    "Anderson", "jack@example.com",     5, 120000, "2018-12-01", 0),
        (11, "Karen",   "Thomas",   "karen@example.com",    1, 135000, "2021-05-20", 1),
        (12, "Leo",     "Jackson",  "leo@example.com",      3, 102000, "2022-10-07", 1),
    ])

    cur.execute("""
        CREATE TABLE products (
            id          INTEGER PRIMARY KEY,
            name        TEXT    NOT NULL,
            category    TEXT    NOT NULL,
            price       REAL    NOT NULL,
            stock       INTEGER NOT NULL
        )
    """)
    cur.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?)", [
        (1,  "Laptop Pro 15",      "Electronics",  1299.99, 150),
        (2,  "Wireless Mouse",     "Electronics",    29.99, 500),
        (3,  "Standing Desk",      "Furniture",     549.00,  75),
        (4,  "Ergonomic Chair",    "Furniture",     399.00, 120),
        (5,  "USB-C Hub",          "Electronics",    59.99, 300),
        (6,  "Monitor 27-inch",    "Electronics",   449.99, 200),
        (7,  "Keyboard Mechanical","Electronics",    89.99, 400),
        (8,  "Desk Lamp",          "Furniture",      35.00, 250),
        (9,  "Webcam HD",          "Electronics",    79.99, 180),
        (10, "Notebook Pack",      "Office",          9.99, 1000),
    ])

    cur.execute("""
        CREATE TABLE orders (
            id           INTEGER PRIMARY KEY,
            employee_id  INTEGER NOT NULL,
            product_id   INTEGER NOT NULL,
            quantity     INTEGER NOT NULL,
            order_date   TEXT    NOT NULL,
            total_amount REAL    NOT NULL,
            FOREIGN KEY (employee_id) REFERENCES employees(id),
            FOREIGN KEY (product_id)  REFERENCES products(id)
        )
    """)
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)", [
        (1,  1, 1, 1, "2024-01-10", 1299.99),
        (2,  2, 3, 1, "2024-01-15",  549.00),
        (3,  3, 2, 3, "2024-02-01",   89.97),
        (4,  4, 6, 2, "2024-02-14",  899.98),
        (5,  5, 7, 1, "2024-03-01",   89.99),
        (6,  1, 5, 2, "2024-03-10",  119.98),
        (7,  7, 4, 1, "2024-03-22",  399.00),
        (8,  8, 9, 1, "2024-04-05",   79.99),
        (9,  6, 10, 5, "2024-04-18",  49.95),
        (10, 9, 8, 2, "2024-05-02",   70.00),
        (11, 11, 1, 1, "2024-05-15", 1299.99),
        (12, 12, 2, 4, "2024-06-01",  119.96),
        (13, 2, 6, 1, "2024-06-20",  449.99),
        (14, 3, 4, 1, "2024-07-04",  399.00),
        (15, 7, 5, 3, "2024-07-19",  179.97),
    ])

    cur.execute("""
        CREATE VIEW employee_summary AS
        SELECT
            e.id,
            e.first_name || ' ' || e.last_name AS full_name,
            d.name AS department,
            e.salary,
            e.hire_date
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.is_active = 1
    """)

    conn.commit()
    conn.close()

    print(f"Created {DB_PATH}")


if __name__ == "__main__":
    main()
