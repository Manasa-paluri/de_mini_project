WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(CASE WHEN payment_month >= DATEADD(month, -1, CURRENT_DATE) THEN revenue ELSE 0 END) AS total_revenue_lm
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
),
churned_customer_revenue AS (
    SELECT
        customer_id,
        SUM(churned_revenue) AS churned_revenue_lm
    FROM (
        SELECT
            customer_id,
             churned_revenue
        FROM {{ ref('churn_cust') }}
     
    ) subquery
    GROUP BY customer_id
),
churned_product_revenue AS (
    SELECT
        customer_id,
        SUM(total_churn_revenue) AS churned_revenue_lm
    FROM (
        SELECT
            customer_id,
            total_churn_revenue
        FROM {{ ref('churn_prod') }}
        
    ) subquery
    GROUP BY customer_id
),
final_revenue AS (
    SELECT
        cr.customer_id,
        cr.total_revenue_lm,
        COALESCE(ccr.churned_revenue_lm, 0) AS churned_customer_revenue_lm,
        COALESCE(cpr.churned_revenue_lm, 0) AS churned_product_revenue_lm,
        cr.total_revenue_lm - COALESCE(ccr.churned_revenue_lm, 0) - COALESCE(cpr.churned_revenue_lm, 0) AS retained_revenue_lm
    FROM customer_revenue cr
    LEFT JOIN churned_customer_revenue ccr ON cr.customer_id = ccr.customer_id
    LEFT JOIN churned_product_revenue cpr ON cr.customer_id = cpr.customer_id
)
SELECT
    customer_id,
    total_revenue_lm,
    churned_customer_revenue_lm,
    churned_product_revenue_lm,
    retained_revenue_lm,
    CASE WHEN total_revenue_lm > 0 THEN (retained_revenue_lm / total_revenue_lm) * 100 ELSE NULL END AS GRR_lm
FROM final_revenue
ORDER BY customer_id;