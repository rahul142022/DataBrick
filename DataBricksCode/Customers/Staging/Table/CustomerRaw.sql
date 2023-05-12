-- Databricks notebook source
CREATE TABLE IF NOT EXISTS CustomerRaw
(
invoice_no  		 STRING
,customer_id		 STRING
,gender              STRING
,age                 STRING
,category            STRING
,quantity            STRING
,price               STRING
,payment_method      STRING
,invoice_date        STRING
,shopping_mall       STRING
,FileName            STRING
,CreatedBy           INT
,CreatedDate 		 TIMESTAMP
,GoodToImport 		 INT 
,Error 				STRING
) USING DELTA Location '/mnt/Contatablobstorage/CustomerRaw'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/mnt/Contatablobstorage/CustomerRaw',recurse=True)

-- COMMAND ----------


