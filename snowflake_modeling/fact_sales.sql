
-- Snowflake SQL: fact_sales table creation

CREATE OR REPLACE TABLE fact_sales AS
SELECT 
  DATE_TRUNC('day', timestamp) AS sale_date,
  product,
  SUM(quantity) AS total_quantity,
  SUM(quantity * price) AS revenue
FROM cleaned_transactions
GROUP BY 1, 2;
