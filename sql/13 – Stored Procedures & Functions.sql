-- 1. Function returning scalar
CREATE OR REPLACE FUNCTION get_total_sales(customer_id_input INT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    total_sales NUMERIC;
BEGIN
    SELECT SUM(s.total)
    INTO total_sales
    FROM sales s
    JOIN orders o ON s.order_id = o.order_id
    WHERE o.customer_id = customer_id_input;
    
    RETURN COALESCE(total_sales, 0);
END;
$$;

-- 2. Function with multiple parameters
CREATE OR REPLACE FUNCTION get_category_stats(category_name TEXT, min_price NUMERIC)
RETURNS TABLE(
    product_count INT,
    avg_price NUMERIC,
    total_stock INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::INT,
        AVG(selling_price),
        SUM(stock)::INT
    FROM products
    WHERE category = category_name
    AND selling_price >= min_price;
END;
$$;

-- 3. Procedure
CREATE OR REPLACE PROCEDURE update_product_stock(
    product_id_input INT,
    quantity_change INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE products
    SET stock = stock + quantity_change
    WHERE product_id = product_id_input;
    
    -- Log the change (optional)
    INSERT INTO stock_history(product_id, change_amount, change_date)
    VALUES (product_id_input, quantity_change, CURRENT_TIMESTAMP);
END;
$$;

-- 4. Function with loop
CREATE OR REPLACE FUNCTION get_order_summary(order_id_input INT)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    result JSON;
    total NUMERIC := 0;
    item RECORD;
BEGIN
    FOR item IN (
        SELECT 
            p.product_name,
            s.quantity,
            s.total
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        WHERE s.order_id = order_id_input
    ) LOOP
        total := total + item.total;
    END LOOP;
    
    SELECT json_build_object(
        'order_id', order_id_input,
        'total', total,
        'items', (
            SELECT json_agg(json_build_object(
                'product_name', product_name,
                'quantity', quantity,
                'total', total
            ))
            FROM sales s
            JOIN products p ON s.product_id = p.product_id
            WHERE s.order_id = order_id_input
        )
    ) INTO result;
    
    RETURN result;
END;
$$;

-- 5. Procedure with transactions
CREATE OR REPLACE PROCEDURE process_order(
    p_order_id INT,
    p_new_status VARCHAR(20)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_status VARCHAR(20);
BEGIN
    -- Get current status
    SELECT status INTO v_current_status
    FROM orders
    WHERE order_id = p_order_id;
    
    -- Validate status transition
    IF v_current_status = 'Completed' AND p_new_status IN ('Pending', 'Cancelled') THEN
        RAISE EXCEPTION 'Cannot change completed order status';
    END IF;
    
    -- Update status
    UPDATE orders
    SET status = p_new_status
    WHERE order_id = p_order_id;
    
    -- Additional logic for cancellation
    IF p_new_status = 'Cancelled' THEN
        -- Restore stock
        UPDATE products p
        SET stock = stock + s.quantity
        FROM sales s
        WHERE p.product_id = s.product_id
        AND s.order_id = p_order_id;
        
        -- Update total to 0
        UPDATE sales
        SET total = 0
        WHERE order_id = p_order_id;
    END IF;
END;
$$;

-- 6. Function with OUT parameters
CREATE OR REPLACE FUNCTION get_customer_metrics(
    customer_id_input INT,
    OUT total_orders INT,
    OUT total_spent NUMERIC,
    OUT avg_order_value NUMERIC,
    OUT last_order DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT 
        COUNT(o.order_id)::INT,
        COALESCE(SUM(s.total), 0),
        COALESCE(AVG(s.total), 0),
        MAX(o.order_date)
    INTO total_orders, total_spent, avg_order_value, last_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN sales s ON o.order_id = s.order_id
    WHERE c.customer_id = customer_id_input
    GROUP BY c.customer_id;
END;
$$;

-- 7. Procedure with dynamic SQL
CREATE OR REPLACE PROCEDURE create_category_summary_table(category_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    table_name TEXT;
BEGIN
    table_name := 'summary_' || REPLACE(LOWER(category_name), ' ', '_');
    
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I AS
        SELECT 
            product_id,
            product_name,
            selling_price,
            stock
        FROM products
        WHERE category = %L
    ', table_name, category_name);
    
    RAISE NOTICE 'Table % created for category %', table_name, category_name;
END;
$$;

-- 8. Function returning table with joins
CREATE OR REPLACE FUNCTION get_top_customers(min_orders INT, max_results INT DEFAULT 10)
RETURNS TABLE(
    customer_id INT,
    customer_name TEXT,
    order_count BIGINT,
    total_spent NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(DISTINCT o.order_id) AS order_count,
        COALESCE(SUM(s.total), 0) AS total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN sales s ON o.order_id = s.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    HAVING COUNT(DISTINCT o.order_id) >= min_orders
    ORDER BY total_spent DESC
    LIMIT max_results;
END;
$$;

-- 9. Procedure with error handling
CREATE OR REPLACE PROCEDURE safe_delete_customer(customer_id_input INT)
LANGUAGE plpgsql
AS $$
DECLARE
    order_count INT;
BEGIN
    -- Check if customer has orders
    SELECT COUNT(*) INTO order_count
    FROM orders
    WHERE customer_id = customer_id_input;
    
    IF order_count > 0 THEN
        RAISE EXCEPTION 'Cannot delete customer with existing orders';
    END IF;
    
    DELETE FROM customers WHERE customer_id = customer_id_input;
    
    RAISE NOTICE 'Customer % deleted successfully', customer_id_input;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error deleting customer: %', SQLERRM;
        ROLLBACK;
END;
$$;

-- 10. Function with recursive CTE
CREATE OR REPLACE FUNCTION get_employee_hierarchy(employee_id_input INT)
RETURNS TABLE(
    emp_id INT,
    emp_name TEXT,
    level INT,
    path TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE hierarchy AS (
        SELECT 
            employee_id,
            employee_name,
            1 AS level,
            employee_name::TEXT AS path
        FROM employees
        WHERE employee_id = employee_id_input
        
        UNION ALL
        
        SELECT 
            e.employee_id,
            e.employee_name,
            h.level + 1,
            h.path || ' -> ' || e.employee_name
        FROM employees e
        INNER JOIN hierarchy h ON e.employee_id = h.employee_id
    )
    SELECT * FROM hierarchy;
END;
$$;