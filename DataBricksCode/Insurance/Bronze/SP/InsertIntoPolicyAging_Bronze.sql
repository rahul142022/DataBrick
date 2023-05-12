-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("CreatedBy", "123")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import input_file_name
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC date = datetime.now()
-- MAGIC GoodToImport = 1 
-- MAGIC PolicyAging = spark.read.format("csv").load("/mnt/Contatablobstorage/Insurance/Drop/PolicyAging/",header= "true")
-- MAGIC PolicyAging_FileName = PolicyAging.withColumn("FileName", input_file_name()).withColumn("CreatedBy", lit(dbutils.widgets.get("CreatedBy")).cast("int")).withColumn("CreatedDate",lit(date)).withColumn("GoodToImport",lit(GoodToImport).cast("int")).withColumn("Error",lit(None))
-- MAGIC                           

-- COMMAND ----------

-- MAGIC %python
-- MAGIC PolicyAging_FileName.write.format("delta").mode("overwrite").save('/mnt/Contatablobstorage/DeltaTable/PolicyAging_Bronze')

-- COMMAND ----------


