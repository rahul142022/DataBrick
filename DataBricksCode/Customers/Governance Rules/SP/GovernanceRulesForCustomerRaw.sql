-- Databricks notebook source
UPDATE CustomerRaw
SET Invoice_date = CAST(date_format(to_date(Invoice_date, 'dd-MM-yyyy'), 'yyyy-MM-dd') AS DATE)

-- COMMAND ----------

UPDATE CustomerRaw
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid date Format',Invoice_date) 
WHERE TRY_CAST(Invoice_date AS DATE ) IS NULL AND Invoice_date IS NOT NULL

-- COMMAND ----------

UPDATE CustomerRaw
SET GoodToImport = 0 , 
    Error = CONCAT(Error,'|','Null Invoice Number')  
WHERE invoice_no IS  NULL;

-- COMMAND ----------

UPDATE CustomerRaw
SET GoodToImport = 0 , 
    Error = CONCAT(Error,'|','Null Invoice Number')  
WHERE invoice_no IS  NULL;


-- COMMAND ----------


WITH CTE1 AS (

    SELECT ROW_NUMBER() OVER (PARTITION BY invoice_no ORDER BY Invoice_date DESC ) AS RN , * 
FROM customerraw
)
SELECT * FROM CTE1  WHERE RN> 1

-- COMMAND ----------


