{{ config(
    materialized='table',
    schema='mart'
) }}
WITH revenue_changes AS (
    SELECT
        CUSTOMER_ID,
        PAYMENT_MONTH,
        SUM(REVENUE) AS revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY CUSTOMER_ID, PAYMENT_MONTH
),
previous_revenue AS (
    SELECT
        CUSTOMER_ID,
        PAYMENT_MONTH,
        revenue,
        LAG(revenue) OVER (PARTITION BY CUSTOMER_ID ORDER BY PAYMENT_MONTH) AS prev_revenue
    FROM revenue_changes
),
monthly_revenue AS (
    SELECT
        PAYMENT_MONTH,
        SUM(revenue) AS total_revenue,
        SUM(CASE
            WHEN revenue > prev_revenue THEN revenue - prev_revenue
            ELSE 0
        END) AS expansion_revenue,
        SUM(CASE
            WHEN revenue < prev_revenue THEN prev_revenue - revenue
            ELSE 0
        END) AS contraction_revenue
    FROM previous_revenue
    GROUP BY PAYMENT_MONTH
)
SELECT
    PAYMENT_MONTH,
    total_revenue,
    expansion_revenue,
    contraction_revenue,
    (total_revenue + expansion_revenue - contraction_revenue) / total_revenue AS NRR,
    (total_revenue - contraction_revenue) / total_revenue AS GRR
FROM monthly_revenue
