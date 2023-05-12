-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("CreatedBy","11")
-- MAGIC dbutils.widgets.text("Error","")
-- MAGIC CreatedBy = dbutils.widgets.get("CreatedBy")
-- MAGIC Error = dbutils.widgets.get("Error")
-- MAGIC Len_Error = len(Error)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC success = f"UPDATE LogTaskControlFlow SET Status = 'success'  WHERE LogTaskControlFlowKey = '{CreatedBy}'"
-- MAGIC
-- MAGIC Failed = f"UPDATE LogTaskControlFlow SET Status = 'Failed' , Error = '{Error}' WHERE LogTaskControlFlowKey = '{CreatedBy}'"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if (Len_Error == 1 ):
-- MAGIC     spark.sql(success)
-- MAGIC     print(success)
-- MAGIC else:
-- MAGIC     spark.sql(Failed)
-- MAGIC     print(Failed)
