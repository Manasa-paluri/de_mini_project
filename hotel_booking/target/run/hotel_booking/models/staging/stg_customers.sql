
  
    

        create or replace transient table HOTEL_MGMT.stage.stg_customers
         as
        (WITH cte AS(

SELECT *
 FROM hotel_mgmt.raw.customers
WHERE CUSTOMER_ID IS NOT NULL 
)
SELECT * FROM cte
        );
      
  