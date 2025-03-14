WITH cte AS(

SELECT *
 FROM hotel_mgmt.raw.transactions
WHERE CUSTOMER_ID IS NOT NULL AND
	PRODUCT_ID IS NOT NULL 
	
)
SELECT * FROM cte