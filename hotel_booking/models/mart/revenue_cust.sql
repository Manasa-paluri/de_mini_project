{{ config(
    materialized='table',
    schema='mart'
) }}
WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(revenue) AS total_revenue
    FROM {{ref('stg_transactions')}}
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT
        customer_id,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)
SELECT 
    rc.customer_id,
    c.customername,
    rc.total_revenue,
    rc.revenue_rank
FROM ranked_customers rc
JOIN {{ref('stg_customers')}} c ON rc.customer_id = c.customer_id
ORDER BY rc.revenue_rank