
  
    

        create or replace transient table HOTEL_MGMT.stage_mart.churn_cust
         as
        (-- config{
--     "model_name": "churn_cust",
--     "model_type": "incremental",
--     "unique_key": "customer_id",
--     "target_schema": "mart",
--     "target_table": "churn_cust",

--     "materialized": "table"
-- }

 
WITH customer_activity AS (
    SELECT
        CUSTOMER_ID AS customer_id,
        MAX(PAYMENT_MONTH) AS last_purchase_date
    FROM HOTEL_MGMT.stage.stg_transactions
    GROUP BY CUSTOMER_ID
),
customer_details AS (
    SELECT *
    FROM HOTEL_MGMT.stage.stg_customers
),
max_month AS (
    SELECT MAX(PAYMENT_MONTH) AS max_date
    FROM HOTEL_MGMT.stage.stg_transactions
),
churned_customers AS (
    SELECT
        ca.customer_id,
        ca.last_purchase_date,
        SUM(t.revenue) AS total_revenue,
        CASE
            WHEN ca.last_purchase_date < DATEADD(month, -1, mm.max_date)
            THEN 1 ELSE 0
        END AS is_churned
    FROM customer_activity ca
    JOIN HOTEL_MGMT.stage.stg_transactions t ON ca.customer_id = t.customer_id
    CROSS JOIN max_month mm
    GROUP BY ca.customer_id, ca.last_purchase_date, mm.max_date
)
SELECT
    DATE_TRUNC('month', cc.last_purchase_date) AS period,
    COUNT(DISTINCT CASE WHEN cc.is_churned = 1 THEN cc.customer_id END) AS churned_customers,
    cc.customer_id,
    cd.customername,
    cd.COMPANY,
    cc.total_revenue AS churned_revenue
FROM churned_customers cc
JOIN customer_details cd ON cc.customer_id = cd.customer_id
WHERE cc.is_churned = 1
GROUP BY DATE_TRUNC('month', cc.last_purchase_date), cc.customer_id, cd.customername, cd.COMPANY, cc.total_revenue
ORDER BY period
        );
      
  