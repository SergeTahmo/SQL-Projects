-- Step 1: Remove duplicate sales records
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sale_date, store_id, product_id
               ORDER BY sale_id
           ) AS rn
    FROM cocacola_sales_raw
)
DELETE FROM dedup WHERE rn > 1;

-- Step 2: Standardize date columns
ALTER TABLE cocacola_sales_cleaned
ALTER COLUMN sale_date DATE;

-- Step 3: Normalize currency to USD (assuming all USD, placeholder if needed)
-- No currency conversion needed for NYC sales; skip or add if needed

-- Step 4: Calculate total sales revenue
UPDATE cocacola_sales_cleaned
SET total_revenue = unit_price * quantity_sold;

-- Step 5: Standardize product categories (e.g., Soda, Juice, Water)
UPDATE cocacola_sales_cleaned
SET product_category = 
    CASE 
        WHEN LOWER(product_name) LIKE '%cola%' THEN 'Soda'
        WHEN LOWER(product_name) LIKE '%juice%' THEN 'Juice'
        WHEN LOWER(product_name) LIKE '%water%' THEN 'Water'
        ELSE 'Other'
    END;

-- Step 6: Aggregate sales by store and product category
CREATE OR ALTER VIEW vw_nyc_sales_summary AS
SELECT 
    store_id,
    store_name,
    product_category,
    SUM(quantity_sold) AS total_units_sold,
    SUM(total_revenue) AS total_revenue,
    COUNT(DISTINCT sale_date) AS sales_days
FROM cocacola_sales_cleaned
WHERE city = 'New York'
GROUP BY store_id, store_name, product_category;

-- Step 7: Calculate average daily sales velocity by product category and store
CREATE OR ALTER VIEW vw_sales_velocity AS
SELECT
    store_id,
    product_category,
    total_units_sold,
    sales_days,
    CAST(total_units_sold AS FLOAT) / NULLIF(sales_days,0) AS avg_daily_units_sold
FROM vw_nyc_sales_summary;

-- Step 8: Add time intelligence columns for monthly and quarterly analysis
ALTER TABLE cocacola_sales_cleaned
ADD sale_month AS DATENAME(month, sale_date);

ALTER TABLE cocacola_sales_cleaned
ADD sale_quarter AS 'Q' + CAST(DATEPART(QUARTER, sale_date) AS VARCHAR(1));

ALTER TABLE cocacola_sales_cleaned
ADD fiscal_year AS 
    CASE 
        WHEN MONTH(sale_date) >= 4 THEN YEAR(sale_date)
        ELSE YEAR(sale_date) - 1
    END;

-- Step 9: Calculate YoY revenue growth by product category
CREATE OR ALTER VIEW vw_yoy_revenue_growth AS
SELECT
    product_category,
    fiscal_year,
    SUM(total_revenue) AS revenue,
    LAG(SUM(total_revenue)) OVER (PARTITION BY product_category ORDER BY fiscal_year) AS prev_year_revenue,
    ROUND(
        100.0 * (
            SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (PARTITION BY product_category ORDER BY fiscal_year)
        ) / NULLIF(LAG(SUM(total_revenue)) OVER (PARTITION BY product_category ORDER BY fiscal_year),0), 2
    ) AS yoy_growth_pct
FROM cocacola_sales_cleaned
GROUP BY product_category, fiscal_year;

-- Step 10: Track product penetration — stores selling each product category monthly
CREATE OR ALTER VIEW vw_product_penetration AS
SELECT
    sale_month,
    product_category,
    COUNT(DISTINCT store_id) AS stores_selling
FROM cocacola_sales_cleaned
WHERE city = 'New York'
GROUP BY sale_month, product_category;

-- Step 11: Identify top performing stores by revenue and volume
CREATE OR ALTER VIEW vw_top_stores AS
SELECT
    TOP 10 store_id,
    store_name,
    SUM(total_revenue) AS total_revenue,
    SUM(quantity_sold) AS total_units_sold
FROM cocacola_sales_cleaned
WHERE city = 'New York'
GROUP BY store_id, store_name
ORDER BY total_revenue DESC;

-- Step 12: Data quality check - missing product info or prices
CREATE OR ALTER VIEW vw_missing_data_checks AS
SELECT sale_id, product_id, unit_price
FROM cocacola_sales_cleaned
WHERE product_id IS NULL OR unit_price IS NULL;

-- End of Coca-Cola NYC sales performance SQL script
