-- 1. Create view with multiple joins
CREATE VIEW vw_sales_summary AS
SELECT 
    s.sale_id,
    o.order_id,
    o.order_date,
    c.first_name,
    c.last_name,
    p.product_name,
    p.category,
    s.quantity,
    s.unit_price,
    s.discount,
    s.total,
    s.profit,
    st.store_name,
    e.employee_name
FROM sales s
JOIN orders o ON s.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id
JOIN stores st ON o.store_id = st.store_id
JOIN employees e ON o.employee_id = e.employee_id;

-- 2. Create view with aggregation
CREATE VIEW vw_category_sales AS
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(s.total) AS total_revenue,
    AVG(s.total) AS avg_order_value,
    SUM(s.profit) AS total_profit,
    AVG(s.profit) AS avg_profit
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN orders o ON s.order_id = o.order_id
GROUP BY p.category;

-- 3. Create view with window functions
CREATE VIEW vw_product_rankings AS
SELECT 
    product_id,
    product_name,
    category,
    selling_price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS price_rank,
    RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS dense_rank
FROM products;

-- 4. Create view with joins and conditions
CREATE VIEW vw_high_value_customers AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.country,
    COUNT(o.order_id) AS order_count,
    SUM(s.total) AS total_spent,
    AVG(s.total) AS avg_order_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN sales s ON o.order_id = s.order_id
WHERE o.status = 'Completed'
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.country
HAVING SUM(s.total) > 1000
ORDER BY total_spent DESC;

-- 5. Materialized view
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT 
    DATE_TRUNC('day', o.order_date) AS sale_date,
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(s.quantity) AS units_sold,
    SUM(s.total) AS daily_revenue
FROM sales s
JOIN orders o ON s.order_id = o.order_id
JOIN products p ON s.product_id = p.product_id
GROUP BY sale_date, p.category;

-- 6. View with UNION
CREATE VIEW vw_all_product_categories AS
SELECT product_name, category, 'Product' AS type FROM products
UNION
SELECT product_name, category, 'Sale' AS type FROM sales
JOIN products USING(product_id);

-- 7. Updateable view (simple)
CREATE VIEW vw_active_products AS
SELECT product_id, product_name, category, stock
FROM products
WHERE stock > 0;

-- 8. View with subquery
CREATE VIEW vw_top_products_by_category AS
SELECT 
    p1.product_id,
    p1.product_name,
    p1.category,
    p1.selling_price,
    (
        SELECT COUNT(*) 
        FROM sales s 
        WHERE s.product_id = p1.product_id
    ) AS sale_count,
    (
        SELECT SUM(total) 
        FROM sales s 
        WHERE s.product_id = p1.product_id
    ) AS total_revenue
FROM products p1
WHERE p1.selling_price > (
    SELECT AVG(selling_price) 
    FROM products p2 
    WHERE p2.category = p1.category
);

-- 9. View with CTE
CREATE VIEW vw_customer_retention AS
WITH CustomerFirstOrder AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.first_name,
    cf.first_order_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, cf.first_order_date)) AS years_active,
    COUNT(o.order_id) AS total_orders,
    SUM(s.total) AS lifetime_value
FROM customers c
JOIN CustomerFirstOrder cf ON c.customer_id = cf.customer_id
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN sales s ON o.order_id = s.order_id
GROUP BY c.customer_id, c.first_name, cf.first_order_date;

-- 10. Drop view
DROP VIEW IF EXISTS vw_old_view CASCADE;