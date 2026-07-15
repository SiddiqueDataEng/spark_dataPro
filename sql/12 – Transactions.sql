-- 1. Basic transaction with COMMIT
BEGIN;
UPDATE products SET stock = stock - 5 WHERE product_id = 1;
UPDATE sales SET quantity = quantity + 1 WHERE sale_id = 100;
COMMIT;

-- 2. Transaction with ROLLBACK
BEGIN;
UPDATE products SET stock = stock - 10 WHERE product_id = 2;
SAVEPOINT before_discount;
UPDATE sales SET discount = discount + 5 WHERE product_id = 2;
ROLLBACK TO SAVEPOINT before_discount;
COMMIT;

-- 3. Transaction with error handling
BEGIN;
UPDATE products SET stock = stock - 1 WHERE product_id = 3;
INSERT INTO orders (customer_id, employee_id, store_id, order_date, status) 
VALUES (100, 50, 10, CURRENT_DATE, 'Pending');
COMMIT;

-- 4. Transaction isolation levels
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM products WHERE product_id = 4;
-- ... perform operations
COMMIT;

-- 5. Complex transaction
BEGIN;
-- Update inventory
UPDATE products SET stock = stock - 10 WHERE product_id = 5;

-- Record sale
INSERT INTO sales (order_id, product_id, quantity, unit_price, discount, total, profit)
VALUES (100, 5, 10, 50.00, 0, 500.00, 100.00);

-- Update order status
UPDATE orders SET status = 'Completed' WHERE order_id = 100;

-- Check stock level
SELECT stock FROM products WHERE product_id = 5;
COMMIT;

-- 6. Transaction with conditional commit
BEGIN;
UPDATE products SET stock = stock - 5 WHERE product_id = 6;
UPDATE products SET stock = stock + 5 WHERE product_id = 7;
-- Check if stock levels are valid
SELECT stock FROM products WHERE product_id IN (6, 7);
COMMIT;

-- 7. Nested transactions (via savepoints)
BEGIN;
UPDATE products SET stock = stock - 10 WHERE product_id = 8;
SAVEPOINT sp1;
UPDATE sales SET discount = 20 WHERE product_id = 8;
ROLLBACK TO SAVEPOINT sp1;
-- Continue with other operations
UPDATE orders SET status = 'Cancelled' WHERE order_id = 101;
COMMIT;

-- 8. Transaction with timeouts
SET LOCAL lock_timeout = '2s';
BEGIN;
-- Perform operations
COMMIT;

-- 9. Read-only transaction
BEGIN TRANSACTION READ ONLY;
SELECT * FROM orders WHERE order_date > '2025-01-01';
COMMIT;

-- 10. Transaction with deferrable constraints
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
-- Perform operations
COMMIT;