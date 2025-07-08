-- CRM Sales Pipeline Cleanup and KPI Preparation Script

-- STEP 1: Remove Duplicate Opportunities
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY opportunity_name, client_id, created_date
               ORDER BY opportunity_id
           ) AS rn
    FROM crm_opportunities_raw
)
DELETE FROM dedup
WHERE rn > 1;

-- STEP 2: Standardize Currency to USD
-- Exchange rates: EUR→USD = 1.1, GBP→USD = 1.25, CAD→USD = 0.75
UPDATE crm_opportunities_cleaned
SET amount_usd = 
    CASE currency
        WHEN 'EUR' THEN amount_local * 1.1
        WHEN 'GBP' THEN amount_local * 1.25
        WHEN 'CAD' THEN amount_local * 0.75
        ELSE amount_local
    END,
    currency = 'USD';

-- STEP 3: Clean and Normalize Dates
ALTER TABLE crm_opportunities_cleaned
ALTER COLUMN created_date DATE;

ALTER TABLE crm_opportunities_cleaned
ALTER COLUMN closed_date DATE;

-- STEP 4: Derive Sales Stage Progression Duration
ALTER TABLE crm_opportunities_cleaned
ADD days_to_close AS DATEDIFF(DAY, created_date, closed_date);

-- STEP 5: Segment Customers Based on Revenue
UPDATE crm_opportunities_cleaned
SET customer_segment = 
    CASE 
        WHEN amount_usd >= 100000 THEN 'Enterprise'
        WHEN amount_usd >= 25000 THEN 'Mid-Market'
        ELSE 'SMB'
    END;

-- STEP 6: Track Sales Funnel Stage Conversions
-- Create a report view for Funnel Conversion Tracking
CREATE OR ALTER VIEW vw_crm_funnel_summary AS
SELECT 
    sales_rep_id,
    COUNT(CASE WHEN stage = 'Lead' THEN 1 END) AS leads,
    COUNT(CASE WHEN stage = 'Qualified' THEN 1 END) AS qualified,
    COUNT(CASE WHEN stage = 'Proposal' THEN 1 END) AS proposals,
    COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) AS wins,
    COUNT(CASE WHEN stage = 'Closed Lost' THEN 1 END) AS losses,
    SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END) AS total_won_revenue,
    AVG(days_to_close) AS avg_days_to_close
FROM crm_opportunities_cleaned
GROUP BY sales_rep_id;

-- STEP 7: Win Rate Calculation
CREATE OR ALTER VIEW vw_crm_win_loss_metrics AS
SELECT 
    sales_rep_id,
    COUNT(*) AS total_opps,
    COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) AS won,
    COUNT(CASE WHEN stage = 'Closed Lost' THEN 1 END) AS lost,
    ROUND(
        100.0 * COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) / 
        NULLIF(COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END), 0), 
    2) AS win_rate_pct
FROM crm_opportunities_cleaned
GROUP BY sales_rep_id;

-- STEP 8: Add Time Intelligence Columns
ALTER TABLE crm_opportunities_cleaned
ADD month_name AS DATENAME(month, created_date),
    sales_quarter AS 'Q' + CAST(DATEPART(QUARTER, created_date) AS VARCHAR(1)),
    fiscal_year AS CASE 
                      WHEN MONTH(created_date) >= 4 THEN YEAR(created_date)
                      ELSE YEAR(created_date) - 1 
                  END;

-- STEP 9: Identify Inactive or Stalled Opportunities
-- Stalled: No update in 30+ days and not yet closed
CREATE OR ALTER VIEW vw_crm_stalled_opps AS
SELECT *
FROM crm_opportunities_cleaned
WHERE stage NOT IN ('Closed Won', 'Closed Lost')
  AND DATEDIFF(DAY, last_modified_date, GETDATE()) > 30;

-- STEP 10: Pipeline Velocity Report by Region and Product
CREATE OR ALTER VIEW vw_pipeline_velocity AS
SELECT 
    region,
    product_category,
    COUNT(*) AS total_opportunities,
    AVG(days_to_close) AS avg_sales_cycle,
    SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END) AS total_revenue,
    ROUND(
        100.0 * COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) /
        NULLIF(COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END), 0), 2) AS win_rate_pct
FROM crm_opportunities_cleaned
GROUP BY region, product_category;

-- STEP 11: Classify Opportunity Aging Buckets
ALTER TABLE crm_opportunities_cleaned
ADD aging_bucket AS 
    CASE 
        WHEN days_to_close IS NULL THEN 'Open'
        WHEN days_to_close <= 15 THEN '0-15 Days'
        WHEN days_to_close <= 30 THEN '16-30 Days'
        WHEN days_to_close <= 60 THEN '31-60 Days'
        ELSE '60+ Days'
    END;

-- STEP 12: Data Quality Check - Missing Contact Info
CREATE OR ALTER VIEW vw_missing_contacts AS
SELECT opportunity_id, client_id, stage
FROM crm_opportunities_cleaned
WHERE contact_email IS NULL OR contact_phone IS NULL;

-- STEP 13: YoY Revenue Growth by Region
CREATE OR ALTER VIEW vw_yoy_growth_region AS
SELECT 
    region,
    fiscal_year,
    SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END) AS revenue,
    LAG(SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END)) 
        OVER (PARTITION BY region ORDER BY fiscal_year) AS prev_year_revenue,
    ROUND(
        100.0 * (
            SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END) -
            LAG(SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END)) 
            OVER (PARTITION BY region ORDER BY fiscal_year)
        ) / NULLIF(LAG(SUM(CASE WHEN stage = 'Closed Won' THEN amount_usd ELSE 0 END)) 
            OVER (PARTITION BY region ORDER BY fiscal_year), 0), 
    2) AS yoy_growth_pct
FROM crm_opportunities_cleaned
GROUP BY region, fiscal_year;

-- END OF SCRIPT
