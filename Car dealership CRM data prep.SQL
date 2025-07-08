-- Step 1: Remove duplicate vehicle sales records
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sale_date, vehicle_vin, customer_id
               ORDER BY sale_id
           ) AS rn
    FROM dealership_sales_raw
)
DELETE FROM dedup WHERE rn > 1;

-- Step 2: Standardize date columns to DATE type
ALTER TABLE dealership_sales_cleaned
ALTER COLUMN sale_date DATE;

ALTER TABLE dealership_sales_cleaned
ALTER COLUMN delivery_date DATE;

-- Step 3: Normalize currency (USD, CAD, EUR) to USD
UPDATE dealership_sales_cleaned
SET sale_price_usd = 
    CASE currency
        WHEN 'CAD' THEN sale_price * 0.75
        WHEN 'EUR' THEN sale_price * 1.1
        ELSE sale_price
    END,
    currency = 'USD';

-- Step 4: Calculate total sale revenue and commission (assuming 5%)
UPDATE dealership_sales_cleaned
SET total_revenue = sale_price_usd,
    commission = sale_price_usd * 0.05;

-- Step 5: Standardize vehicle categories based on model names
UPDATE dealership_sales_cleaned
SET vehicle_category = 
    CASE 
        WHEN LOWER(vehicle_model) LIKE '%suv%' THEN 'SUV'
        WHEN LOWER(vehicle_model) LIKE '%sedan%' THEN 'Sedan'
        WHEN LOWER(vehicle_model) LIKE '%truck%' THEN 'Truck'
        ELSE 'Other'
    END;

-- Step 6: Segment customers by purchase frequency
WITH cust_purchases AS (
    SELECT customer_id, COUNT(*) AS purchases_count
    FROM dealership_sales_cleaned
    GROUP BY customer_id
)
UPDATE dealership_sales_cleaned
SET customer_segment = 
    CASE 
        WHEN cp.purchases_count >= 3 THEN 'Loyal'
        WHEN cp.purchases_count = 2 THEN 'Returning'
        ELSE 'New'
    END
FROM cust_purchases cp
WHERE dealership_sales_cleaned.customer_id = cp.customer_id;

-- Step 7: Add time intelligence columns
ALTER TABLE dealership_sales_cleaned
ADD sale_month AS DATENAME(month, sale_date);

ALTER TABLE dealership_sales_cleaned
ADD sale_quarter AS 'Q' + CAST(DATEPART(QUARTER, sale_date) AS VARCHAR(1));

ALTER TABLE dealership_sales_cleaned
ADD fiscal_year AS 
    CASE 
        WHEN MONTH(sale_date) >= 4 THEN YEAR(sale_date)
        ELSE YEAR(sale_date) - 1
    END;

-- Step 8: Calculate inventory turnover - number of days car was in inventory before sale
UPDATE dealership_sales_cleaned
SET days_in_inventory = DATEDIFF(DAY, vehicle_arrival_date, sale_date);

-- Step 9: Identify stalled inventory (vehicles not sold after 60+ days)
CREATE OR ALTER VIEW vw_stalled_inventory AS
SELECT vehicle_vin, vehicle_model, vehicle_category, vehicle_arrival_date, DATEDIFF(DAY, vehicle_arrival_date, GETDATE()) AS days_in_inventory
FROM dealership_inventory
WHERE DATEDIFF(DAY, vehicle_arrival_date, GETDATE()) > 60
  AND vehicle_vin NOT IN (SELECT vehicle_vin FROM dealership_sales_cleaned);

-- Step 10: Calculate monthly sales per vehicle category and region
CREATE OR ALTER VIEW vw_monthly_sales_by_category AS
SELECT sale_month, region, vehicle_category, COUNT(*) AS units_sold, SUM(total_revenue) AS revenue
FROM dealership_sales_cleaned
GROUP BY sale_month, region, vehicle_category;

-- Step 11: Calculate average commission per sales rep and monthly trend
CREATE OR ALTER VIEW vw_commission_trends AS
SELECT sales_rep_id, sale_month, SUM(commission) AS total_commission, AVG(commission) AS avg_commission
FROM dealership_sales_cleaned
GROUP BY sales_rep_id, sale_month;

-- Step 12: Data quality check - missing customer contact info
CREATE OR ALTER VIEW vw_missing_customer_contacts AS
SELECT customer_id, customer_name, sale_id
FROM dealership_sales_cleaned
WHERE customer_email IS NULL OR customer_phone IS NULL;

-- End of car dealership CRM data prep script
