{{ config(
    materialized='table',
    schema='mart'
) }}
WITH customer_product_activity AS (
    SELECT
        customer_id,
        product_id,
        MAX(payment_month) AS last_purchase_date
    FROM {{ ref('stg_transactions') }}
    WHERE revenue_type = 1
    GROUP BY customer_id, product_id
),
max_month AS (
    SELECT MAX(payment_month) AS max_date
    FROM {{ ref('stg_transactions') }}
    WHERE revenue_type = 1
),
churned_products AS (
    SELECT
        cpa.customer_id,
        cpa.product_id,
        cpa.last_purchase_date,
        CASE
            WHEN cpa.last_purchase_date < DATEADD(month, -1, mm.max_date)
            THEN 1 ELSE 0
        END AS is_churned
    FROM customer_product_activity cpa
    CROSS JOIN max_month mm
),
product_details AS (
    SELECT
        product_id,
        product_family
    FROM {{ ref('stg_products') }}
),
customer_churn AS (
    SELECT
        cp.customer_id,
        SUM(cp.is_churned) AS total_churn_products,
        SUM(CASE WHEN cp.is_churned = 1 THEN t.revenue ELSE 0 END) AS total_churn_revenue
    FROM churned_products cp
    JOIN product_details pd ON TRIM(cp.product_id) = pd.product_id
    JOIN {{ ref('stg_transactions') }} t ON cp.customer_id = t.customer_id AND cp.product_id = t.product_id
    GROUP BY cp.customer_id
)
SELECT
    c.customer_id,
    c.customername,
    COALESCE(cc.total_churn_products, 0) AS total_churn_products,
    COALESCE(cc.total_churn_revenue, 0) AS total_churn_revenue
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_churn cc ON c.customer_id = cc.customer_id
ORDER BY total_churn_products DESC