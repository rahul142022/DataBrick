-- Databricks notebook source
-- SELECT * FROM PolicyAging_Silver LIMIT 10
-- PolicyAging_Bronze
-- PolicyInfo_Bronze
-- PolicyInfo_Silver
-- SELECT * FROM PolicyInfo_Silver LIMIT 10
SELECT * FROM TransactionGrowthRate_Silver LIMIT 10 

-- COMMAND ----------

TRUNCATE TABLE PolicyInfo_Bronze;
TRUNCATE TABLE PolicyInfo_Silver

-- COMMAND ----------

SELECT * FROM PolicyInfo_Bronze limit 10

-- COMMAND ----------

SELECT * FROM PolicyInfo_Silver limit 10

-- COMMAND ----------

SELECT * FROM LogTaskControlFlow

-- COMMAND ----------



-- COMMAND ----------


