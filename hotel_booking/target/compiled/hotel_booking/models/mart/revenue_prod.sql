
WITH product_revenue AS (
    SELECT
        product_id,
        SUM(revenue) AS total_revenue
    FROM HOTEL_MGMT.stage.stg_transactions
    GROUP BY product_id
),
ranked_products AS (
    SELECT
        product_id,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM product_revenue
)
SELECT 
    rp.product_id,
    p.product_family,
    rp.total_revenue,
    rp.revenue_rank
FROM ranked_products rp
JOIN HOTEL_MGMT.stage.stg_products p ON rp.product_id = p.product_id
ORDER BY rp.revenue_rank