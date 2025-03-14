WITH cte AS(

SELECT *
 FROM {{source('hotel','customers')}}
WHERE CUSTOMER_ID IS NOT NULL 
)
SELECT * FROM cte