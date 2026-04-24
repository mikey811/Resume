-- DataCamp Data Analyst Associate Practical Exam (DA501P)
-- FoodYum Grocery Store Sales | Certified: April 24, 2026
-- PostgreSQL


-- ============================================================
-- TASK 1
-- Count products with missing year_added
-- Output: missing_year
-- ============================================================

SELECT COUNT(*) AS missing_year
FROM products
WHERE year_added IS NULL;


-- ============================================================
-- TASK 2
-- Clean and standardize the products table
-- Output: clean_data
-- ============================================================

SELECT
  product_id,
  COALESCE(product_type, 'Unknown') AS product_type,
  CASE
    WHEN brand IS NULL OR brand = '-' THEN 'Unknown'
    ELSE brand
  END AS brand,
  COALESCE(
    ROUND(REGEXP_REPLACE(weight, '[^0-9.]', '', 'g')::numeric, 2),
    (
      SELECT ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (
          ORDER BY REGEXP_REPLACE(weight, '[^0-9.]', '', 'g')::numeric
        )::numeric, 2
      )
      FROM products
      WHERE weight IS NOT NULL
    )
  ) AS weight,
  ROUND(
    COALESCE(
      price,
      (
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
        FROM products
        WHERE price IS NOT NULL
      )
    )::numeric, 2
  ) AS price,
  COALESCE(average_units_sold, 0) AS average_units_sold,
  COALESCE(year_added, 2022) AS year_added,
  UPPER(COALESCE(stock_location, 'Unknown')) AS stock_location
FROM products;


-- ============================================================
-- TASK 3
-- Min and max price per product_type
-- Output: min_max_product
-- ============================================================

SELECT
  COALESCE(product_type, 'Unknown') AS product_type,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM products
GROUP BY COALESCE(product_type, 'Unknown');


-- ============================================================
-- TASK 4
-- Meat and Dairy products with average_units_sold > 10
-- Output: average_price_product
-- ============================================================

SELECT
  product_id,
  price,
  average_units_sold
FROM products
WHERE product_type IN ('Meat', 'Dairy')
  AND average_units_sold > 10;
