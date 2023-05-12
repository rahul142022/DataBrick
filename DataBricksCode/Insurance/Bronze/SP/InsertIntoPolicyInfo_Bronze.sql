-- Databricks notebook source
TRUNCATE TABLE PolicyInfo_Bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC CreatedBy = dbutils.widgets.get("CreatedBy")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import input_file_name
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC date = datetime.now()
-- MAGIC GoodToImport = 1 
-- MAGIC PolicyInfo = spark.read.format("csv").load("/mnt/Contatablobstorage/Insurance/Drop/PolicyInfo/",header= "true")
-- MAGIC PolicyInfo_FileName = PolicyInfo.withColumn("FileName", input_file_name()).withColumn("CreatedBy", lit(dbutils.widgets.get("CreatedBy")).cast("int")).withColumn("CreatedDate",lit(date)).withColumn("GoodToImport",lit(GoodToImport).cast("int")).withColumn("Error",lit(None))
-- MAGIC                           

-- COMMAND ----------

-- MAGIC %python
-- MAGIC PolicyInfo_FileName.write.format("delta").mode("overwrite").save('/mnt/Contatablobstorage/DeltaTable/PolicyInfo_Bronze')

-- COMMAND ----------

select * from polic
