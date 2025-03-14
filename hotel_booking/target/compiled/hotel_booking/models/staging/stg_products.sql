WITH cte AS(

SELECT *
 FROM hotel_mgmt.raw.products
WHERE PRODUCT_ID IS NOT NULL 
)
SELECT * FROM cte