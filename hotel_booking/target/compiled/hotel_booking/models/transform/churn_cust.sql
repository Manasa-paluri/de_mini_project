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
)
 
SELECT
    DATE_TRUNC('month', ca.last_purchase_date) AS period,
    COUNT(DISTINCT ca.customer_id) AS churned_customers,
    ca.customer_id,
    cd.customername,
    cd.COMPANY
FROM customer_activity ca
JOIN customer_details cd
ON ca.customer_id = cd.customer_id
JOIN max_month mmo
ON ca.last_purchase_date < DATEADD(month, -3, mmo.max_date)
GROUP BY DATE_TRUNC('month', ca.last_purchase_date), ca.customer_id, cd.customername, cd.COMPANY
ORDER BY period