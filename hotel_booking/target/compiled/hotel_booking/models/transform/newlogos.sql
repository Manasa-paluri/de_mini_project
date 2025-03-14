
WITH params AS (
    SELECT 1 AS fiscal_year_start_month -- Change this value to set  fiscal year start month
),
customer_activity AS (
    SELECT
        CUSTOMER_ID AS customer_id,
        MIN(PAYMENT_MONTH) AS first_purchase_date
    FROM HOTEL_MGMT.stage.stg_transactions
    GROUP BY CUSTOMER_ID
),
fiscal_years AS (
    SELECT
        ca.customer_id,
        ca.first_purchase_date,
        CASE
            WHEN EXTRACT(MONTH FROM ca.first_purchase_date) >= p.fiscal_year_start_month THEN EXTRACT(YEAR FROM ca.first_purchase_date)
            ELSE EXTRACT(YEAR FROM ca.first_purchase_date) - 1
        END AS fiscal_year
    FROM customer_activity ca
    CROSS JOIN params p
),
customer_details AS (
    SELECT *
    FROM HOTEL_MGMT.stage.stg_customers
)
SELECT
    fy.fiscal_year,
    fy.customer_id,
    cd.customername,
    cd.COMPANY,
    fy.first_purchase_date
FROM fiscal_years fy
JOIN customer_details cd ON fy.customer_id = cd.customer_id
ORDER BY fy.fiscal_year, fy.customer_id;