
  
    

        create or replace transient table HOTEL_MGMT.stage.stg_transactions
         as
        (WITH cte AS(

SELECT *
 FROM hotel_mgmt.raw.transactions
WHERE CUSTOMER_ID IS NOT NULL AND
	PRODUCT_ID IS NOT NULL 
	
)
SELECT * FROM cte
        );
      
  