"""Queries the SQLite demo database through a running Presto server."""

import prestodb


def get_connection():
    return prestodb.dbapi.connect(
        host="localhost",
        port=8080,
        user="test",
        catalog="sqlite",
        schema="default",
    )


def run_query(cursor, sql, title=None):
    if title:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
    print(f"SQL: {sql.strip()}\n")

    cursor.execute(sql)
    rows = cursor.fetchall()

    if cursor.description:
        col_names = [desc[0] for desc in cursor.description]

        # Calculate column widths
        widths = [len(name) for name in col_names]
        for row in rows:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))

        # Print header
        header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(col_names))
        print(header)
        print("-+-".join("-" * w for w in widths))

        # Print rows
        for row in rows:
            line = " | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row))
            print(line)

    print(f"\n({len(rows)} rows)")
    return rows


def main():
    conn = get_connection()
    cur = conn.cursor()

    run_query(cur, "SHOW CATALOGS", "Catalogs")
    run_query(cur, 'SHOW TABLES FROM sqlite."default"', "Tables")

    run_query(cur, 'SELECT * FROM sqlite."default".departments', "Departments")
    run_query(cur, 'SELECT * FROM sqlite."default".employees ORDER BY salary DESC', "Employees")

    run_query(
        cur,
        """
        SELECT
            d.name AS department,
            COUNT(*) AS headcount,
            ROUND(AVG(e.salary), 2) AS avg_salary,
            MIN(e.salary) AS min_salary,
            MAX(e.salary) AS max_salary
        FROM sqlite."default".employees e
        JOIN sqlite."default".departments d ON e.department_id = d.id
        WHERE e.is_active = 1
        GROUP BY d.name
        ORDER BY avg_salary DESC
        """,
        "Avg salary by department",
    )

    run_query(
        cur,
        """
        SELECT
            e.first_name || ' ' || e.last_name AS employee,
            p.name AS product,
            o.quantity,
            o.total_amount,
            o.order_date
        FROM sqlite."default".orders o
        JOIN sqlite."default".employees e ON o.employee_id = e.id
        JOIN sqlite."default".products p ON o.product_id = p.id
        ORDER BY o.total_amount DESC
        LIMIT 5
        """,
        "Top 5 orders",
    )

    run_query(
        cur,
        """
        SELECT
            p.category,
            SUM(o.total_amount) AS total_revenue,
            SUM(o.quantity) AS units_sold
        FROM sqlite."default".orders o
        JOIN sqlite."default".products p ON o.product_id = p.id
        GROUP BY p.category
        ORDER BY total_revenue DESC
        """,
        "Revenue by product category",
    )

    run_query(cur, 'SELECT * FROM sqlite."default".employee_summary', "Employee summary view")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
