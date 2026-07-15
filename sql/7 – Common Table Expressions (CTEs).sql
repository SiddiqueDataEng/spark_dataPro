-- 1. Simple CTE
WITH HighValueOrders AS (
    SELECT order_id, total 
    FROM sales 
    WHERE total > 500
)
SELECT * FROM HighValueOrders;

-- 2. Multiple CTEs
WITH 
OrderSummary AS (
    SELECT 
        order_id,
        SUM(total) AS order_total
    FROM sales
    GROUP BY order_id
),
CustomerOrders AS (
    SELECT 
        c.customer_id,
        c.first_name,
        COUNT(o.order_id) AS order_count,
        SUM(os.order_total) AS total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN OrderSummary os ON o.order_id = os.order_id
    GROUP BY c.customer_id, c.first_name
)
SELECT * FROM CustomerOrders 
WHERE total_spent > 1000;

-- 3. Recursive CTE (Employee Hierarchy)
WITH RECURSIVE EmployeeHierarchy AS (
    -- Anchor member: top-level employees
    SELECT 
        employee_id,
        employee_name,
        department,
        1 AS level
    FROM employees 
    WHERE employee_id = 1
    
    UNION ALL
    
    -- Recursive member: find subordinates
    SELECT 
        e.employee_id,
        e.employee_name,
        e.department,
        eh.level + 1
    FROM employees e
    INNER JOIN EmployeeHierarchy eh ON e.employee_id = eh.employee_id
    WHERE e.employee_id IN (
        SELECT employee_id 
        FROM employees 
        WHERE department = eh.department
    )
)
SELECT * FROM EmployeeHierarchy;

-- 4. CTE with window function
WITH RankedProducts AS (
    SELECT 
        product_name,
        category,
        selling_price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank
    FROM products
)
SELECT * FROM RankedProducts 
WHERE rank <= 5;

-- 5. CTE for aggregations
WITH CategorySummary AS (
    SELECT 
        category,
        COUNT(*) AS product_count,
        AVG(selling_price) AS avg_price,
        SUM(stock) AS total_stock
    FROM products
    GROUP BY category
)
SELECT * FROM CategorySummary;

-- 6. CTE for date ranges
WITH LastMonth AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AS start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS end_date
)
SELECT 
    o.* 
FROM orders o, LastMonth lm
WHERE o.order_date BETWEEN lm.start_date AND lm.end_date;

-- 7. CTE with multiple aggregations
WITH 
SalesStats AS (
    SELECT 
        product_id,
        SUM(total) AS total_sales,
        COUNT(*) AS sale_count,
        AVG(total) AS avg_sale
    FROM sales
    GROUP BY product_id
),
ProductSales AS (
    SELECT 
        p.product_name,
        p.category,
        s.total_sales,
        s.sale_count,
        s.avg_sale
    FROM products p
    JOIN SalesStats s ON p.product_id = s.product_id
)
SELECT * FROM ProductSales 
WHERE total_sales > 1000;

-- 8. CTE with CASE
WITH CustomerSegments AS (
    SELECT 
        customer_id,
        first_name,
        CASE 
            WHEN total_spent > 5000 THEN 'VIP'
            WHEN total_spent > 1000 THEN 'Premium'
            ELSE 'Regular'
        END AS segment
    FROM (
        SELECT 
            c.customer_id,
            c.first_name,
            COALESCE(SUM(s.total), 0) AS total_spent
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN sales s ON o.order_id = s.order_id
        GROUP BY c.customer_id, c.first_name
    ) AS customer_spending
)
SELECT segment, COUNT(*) AS customer_count
FROM CustomerSegments
GROUP BY segment;

-- 9-30. Additional CTE variations (continued in other modules)