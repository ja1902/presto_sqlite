-- presto-sqlite demo queries
-- Run via Presto CLI:  presto --catalog sqlite --schema default --file query_all.sql

SHOW SCHEMAS FROM sqlite;

SHOW TABLES FROM sqlite."default";

SELECT * FROM sqlite."default".departments;

SELECT * FROM sqlite."default".employees ORDER BY salary DESC;

SELECT
    d.name          AS department,
    COUNT(*)        AS headcount,
    ROUND(AVG(e.salary), 2) AS avg_salary,
    MIN(e.salary)   AS min_salary,
    MAX(e.salary)   AS max_salary
FROM sqlite."default".employees e
JOIN sqlite."default".departments d ON e.department_id = d.id
WHERE e.is_active = 1
GROUP BY d.name
ORDER BY avg_salary DESC;

SELECT
    e.first_name || ' ' || e.last_name AS employee,
    p.name       AS product,
    o.quantity,
    o.total_amount,
    o.order_date
FROM sqlite."default".orders o
JOIN sqlite."default".employees e ON o.employee_id = e.id
JOIN sqlite."default".products  p ON o.product_id  = p.id
ORDER BY o.total_amount DESC
LIMIT 5;

SELECT
    p.category,
    SUM(o.total_amount) AS total_revenue,
    SUM(o.quantity)      AS units_sold
FROM sqlite."default".orders o
JOIN sqlite."default".products p ON o.product_id = p.id
GROUP BY p.category
ORDER BY total_revenue DESC;

SELECT * FROM sqlite."default".employee_summary;
