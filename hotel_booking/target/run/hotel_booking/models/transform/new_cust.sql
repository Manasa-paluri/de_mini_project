
  
    

        create or replace transient table HOTEL_MGMT.stage.new_cust
         as
        (WITH customer_activity AS (
    SELECT
        CUSTOMER_ID AS customer_id,
        MIN(PAYMENT_MONTH) AS first_purchase_date
    FROM HOTEL_MGMT.stage.stg_transactions
    GROUP BY CUSTOMER_ID
),
 
new_customers AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', first_purchase_date) AS period,
        first_purchase_date
    FROM customer_activity
),
 
customer_details AS (
    SELECT *
    FROM HOTEL_MGMT.stage.stg_customers
),
 
max_month AS (
    SELECT MAX(PAYMENT_MONTH) AS max_date
    FROM HOTEL_MGMT.stage.stg_transactions
)
 
SELECT
    nc.period,
    nc.customer_id,
    cd.customername,
    cd.COMPANY
FROM new_customers nc
JOIN customer_details cd
ON nc.customer_id = cd.customer_id
JOIN max_month mmo
ON nc.first_purchase_date > DATEADD(month, -3, mmo.max_date)
ORDER BY nc.period
        );
      
  