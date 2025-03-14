WITH cte AS(

SELECT *
 FROM {{source('hotel','transactions')}}
WHERE CUSTOMER_ID IS NOT NULL AND
	PRODUCT_ID IS NOT NULL 
	
)
SELECT * FROM cte