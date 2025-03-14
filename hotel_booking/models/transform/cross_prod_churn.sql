WITH customer_purchase_history AS (
    SELECT
        t.customer_id,
        t.product_id,
        t.payment_month,
        t.revenue,
        MIN(t.payment_month) OVER (PARTITION BY t.customer_id) AS first_purchase_month,
        MAX(t.payment_month) OVER (PARTITION BY t.customer_id) AS last_purchase_month
    FROM {{ ref('stg_transactions') }} t
),
product_purchase_counts AS (
    SELECT
        customer_id,
        product_id,
        COUNT(DISTINCT payment_month) AS purchase_count,
        MAX(payment_month) AS last_purchase_month
    FROM customer_purchase_history
    GROUP BY customer_id, product_id
),
customer_cross_sell AS (
    SELECT
        customer_id,
        payment_month,
        COUNT(DISTINCT product_id) AS total_cross_sells
    FROM customer_purchase_history
    WHERE payment_month > first_purchase_month
    GROUP BY customer_id
),
churn_new_customers AS (
    SELECT
        payment_month,
        customer_id,
        MIN(payment_month) OVER (PARTITION BY customer_id) AS first_purchase_date,
        MAX(payment_month) OVER (PARTITION BY customer_id) AS last_purchase_date
    FROM {{ ref('stg_transactions') }}
    WHERE revenue_type = 1
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
),
churned_products AS (
    SELECT
        cpa.customer_id,
        cpa.product_id,
        cpa.last_purchase_month,
        CASE
            WHEN cpa.last_purchase_month < DATEADD(month, -1, mm.max_date)
            THEN 1 ELSE 0
        END AS is_churned
    FROM customer_purchase_history cpa
    CROSS JOIN (
        SELECT MAX(payment_month) AS max_date
        FROM {{ ref('stg_transactions') }}
    ) mm
),
product_churn AS (
    SELECT
        customer_id,
        payment_month,
        COUNT(DISTINCT CASE WHEN is_churned = 1 THEN product_id END) AS churned_products
    FROM churned_products
    GROUP BY customer_id
)
SELECT
    tp.payment_month,
    tp.new_customers_lm,
    tp.churned_customers_lm,
    tp.new_customers_l3m,
    tp.churned_customers_l3m,
    tp.new_customers_ltm,
    tp.churned_customers_ltm,
    cs.total_cross_sells,
    pc.churned_products
FROM time_periods tp
LEFT JOIN customer_cross_sell cs ON tp.payment_month = cs.payment_month
LEFT JOIN product_churn pc ON tp.payment_month = pc.payment_month
ORDER BY tp.payment_month;