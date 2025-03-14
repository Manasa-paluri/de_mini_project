
  
    

        create or replace transient table HOTEL_MGMT.stage_mart.newcust_churn
         as
        (
WITH churn_new_customers AS (
    SELECT
        payment_month,
        customer_id,
        MIN(payment_month) OVER (PARTITION BY customer_id) AS first_purchase_date,
        MAX(payment_month) OVER (PARTITION BY customer_id) AS last_purchase_date
    FROM HOTEL_MGMT.stage.stg_transactions  
    WHERE revenue_type=1
),
aggregated_data AS (
    SELECT
        payment_month,
        COUNT(DISTINCT CASE WHEN first_purchase_date = payment_month THEN customer_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN last_purchase_date = payment_month THEN customer_id END) AS churned_customers
    FROM churn_new_customers
    GROUP BY payment_month
    ORDER BY payment_month
),
time_periods AS (
    SELECT
        payment_month,
        SUM(new_customers) OVER (ORDER BY payment_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS new_customers_lm,
        SUM(churned_customers) OVER (ORDER BY payment_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS churned_customers_lm,
        SUM(new_customers) OVER (ORDER BY payment_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS new_customers_l3m,
        SUM(churned_customers) OVER (ORDER BY payment_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS churned_customers_l3m,
        SUM(new_customers) OVER (ORDER BY payment_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS new_customers_ltm,
        SUM(churned_customers) OVER (ORDER BY payment_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS churned_customers_ltm
    FROM aggregated_data
)
SELECT * FROM time_periods
        );
      
  