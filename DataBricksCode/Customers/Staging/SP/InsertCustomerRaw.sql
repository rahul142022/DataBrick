-- Databricks notebook source
TRUNCATE TABLE  CustomerRaw

-- COMMAND ----------

-- %fs ls '/mnt/Contatablobstorage/Customer/drop/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("CreatedBy", "123")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import input_file_name
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC date = datetime.now()
-- MAGIC GoodToImport = 1 
-- MAGIC Customers = spark.read.format("csv").load("/mnt/Contatablobstorage/Customer/drop/",header= "true")
-- MAGIC Customers_FileName = Customers.withColumn("FileName", input_file_name()).withColumn("CreatedBy", lit(dbutils.widgets.get("CreatedBy")).cast("int")).withColumn("CreatedDate",lit(date)).withColumn("GoodToImport",lit(GoodToImport).cast("int")).withColumn("Error",lit(None))
-- MAGIC                           

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Customers_FileName.write.format("delta").mode("overwrite").save("/mnt/Contatablobstorage/CustomerRaw")

-- COMMAND ----------


