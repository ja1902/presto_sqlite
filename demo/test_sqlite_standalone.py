"""Standalone test of the SQLite database without the Presto server."""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_data.db")


def separator(title):
    print(f"\n--- {title} ---\n")


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        print("Run create_sqlite_db.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    separator("Tables and Views")
    cur.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name")
    for row_type, name in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM "{name}"')
        count = cur.fetchone()[0]
        print(f"  [{row_type:5s}] {name:25s}  ({count} rows)")

    separator("departments")
    cur.execute("SELECT * FROM departments")
    print(f"  {'ID':>3s}  {'Name':<15s}  {'Budget':>12s}  {'Location'}")
    print(f"  {'---':>3s}  {'-'*15}  {'-'*12}  {'-'*15}")
    for row in cur.fetchall():
        print(f"  {row[0]:3d}  {row[1]:<15s}  {row[2]:>12,.2f}  {row[3]}")

    separator("Employees by salary")
    cur.execute("""
        SELECT e.first_name || ' ' || e.last_name, d.name, e.salary
        FROM employees e JOIN departments d ON e.department_id = d.id
        ORDER BY e.salary DESC
    """)
    print(f"  {'Name':<20s}  {'Department':<15s}  {'Salary':>10s}")
    print(f"  {'-'*20}  {'-'*15}  {'-'*10}")
    for name, dept, salary in cur.fetchall():
        print(f"  {name:<20s}  {dept:<15s}  {salary:>10,.2f}")

    separator("Avg salary by department")
    cur.execute("""
        SELECT d.name, COUNT(*) as cnt, ROUND(AVG(e.salary),2) as avg_sal,
               MIN(e.salary) as min_sal, MAX(e.salary) as max_sal
        FROM employees e JOIN departments d ON e.department_id = d.id
        WHERE e.is_active = 1
        GROUP BY d.name
        ORDER BY avg_sal DESC
    """)
    print(f"  {'Department':<15s}  {'Count':>5s}  {'Avg':>10s}  {'Min':>10s}  {'Max':>10s}")
    print(f"  {'-'*15}  {'-'*5}  {'-'*10}  {'-'*10}  {'-'*10}")
    for dept, cnt, avg, mn, mx in cur.fetchall():
        print(f"  {dept:<15s}  {cnt:>5d}  {avg:>10,.2f}  {mn:>10,.2f}  {mx:>10,.2f}")

    separator("Revenue by category")
    cur.execute("""
        SELECT p.category, SUM(o.total_amount) as revenue, SUM(o.quantity) as units
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.category ORDER BY revenue DESC
    """)
    print(f"  {'Category':<15s}  {'Revenue':>10s}  {'Units':>5s}")
    print(f"  {'-'*15}  {'-'*10}  {'-'*5}")
    for cat, rev, units in cur.fetchall():
        print(f"  {cat:<15s}  {rev:>10,.2f}  {units:>5d}")

    separator("Top 5 orders")
    cur.execute("""
        SELECT e.first_name || ' ' || e.last_name, p.name, o.quantity, o.total_amount, o.order_date
        FROM orders o
        JOIN employees e ON o.employee_id = e.id
        JOIN products p ON o.product_id = p.id
        ORDER BY o.total_amount DESC LIMIT 5
    """)
    print(f"  {'Employee':<20s}  {'Product':<22s}  {'Qty':>3s}  {'Total':>10s}  {'Date'}")
    print(f"  {'-'*20}  {'-'*22}  {'-'*3}  {'-'*10}  {'-'*10}")
    for emp, prod, qty, total, dt in cur.fetchall():
        print(f"  {emp:<20s}  {prod:<22s}  {qty:>3d}  {total:>10,.2f}  {dt}")

    separator("employee_summary view")
    cur.execute("SELECT * FROM employee_summary")
    print(f"  {'ID':>3s}  {'Name':<20s}  {'Department':<15s}  {'Salary':>10s}  {'Hired'}")
    print(f"  {'---':>3s}  {'-'*20}  {'-'*15}  {'-'*10}  {'-'*10}")
    for row in cur.fetchall():
        print(f"  {row[0]:3d}  {row[1]:<20s}  {row[2]:<15s}  {row[3]:>10,.2f}  {row[4]}")

    conn.close()

    print(f"\nOK - all queries passed ({DB_PATH})")


if __name__ == "__main__":
    main()
