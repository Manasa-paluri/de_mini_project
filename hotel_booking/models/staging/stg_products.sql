WITH cte AS(

SELECT *
 FROM {{source('hotel','products')}}
WHERE PRODUCT_ID IS NOT NULL 
)
SELECT * FROM cte