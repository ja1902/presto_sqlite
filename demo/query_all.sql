-- presto-sqlite demo queries
-- Run via Presto CLI:  presto --server localhost:8080 --file query_all.sql

SHOW CATALOGS;

-- SQLite

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

-- PostgreSQL

SHOW TABLES FROM postgres.public;

SELECT * FROM postgres.public.customers ORDER BY tier, name;

SELECT
    c.name    AS customer,
    c.tier,
    c.country,
    COUNT(*)  AS orders,
    SUM(co.total_amount) AS total_spent
FROM postgres.public.customer_orders co
JOIN postgres.public.customers c ON co.customer_id = c.id
GROUP BY c.name, c.tier, c.country
ORDER BY total_spent DESC;

-- Cross-catalog joins

SELECT
    p.name        AS product,
    p.category,
    p.price       AS unit_price,
    SUM(co.quantity)     AS customer_units,
    SUM(co.total_amount) AS customer_revenue
FROM postgres.public.customer_orders co
JOIN sqlite."default".products p ON co.product_id = p.id
GROUP BY p.name, p.category, p.price
ORDER BY customer_revenue DESC;

SELECT
    p.name        AS product,
    p.category,
    SUM(o.quantity)  AS internal_units,
    SUM(co.quantity) AS customer_units,
    SUM(o.quantity) + SUM(co.quantity) AS total_units
FROM sqlite."default".products p
LEFT JOIN sqlite."default".orders o   ON o.product_id  = p.id
LEFT JOIN postgres.public.customer_orders co ON co.product_id = p.id
GROUP BY p.name, p.category
ORDER BY total_units DESC;
