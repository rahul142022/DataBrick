# Databricks notebook source
dataFactory = dbutils.widgets.get("dataFactory")
schemaName = dbutils.widgets.get("schemaName")
endingActivity = dbutils.widgets.get("endingActivity")
startingActivity = dbutils.widgets.get("startingActivity")
Pipeline = dbutils.widgets.get("Pipeline")
Status = dbutils.widgets.get("Status")

# COMMAND ----------

try:
    InsertCommand = f"INSERT INTO LogTaskControlFlow (StartTime,DataFactory,Pipeline,StartingActivity,EndingActivity,SchemaName,Status) SELECT   GETDATE(),'{dataFactory}','{Pipeline}','{startingActivity}','{endingActivity}','{schemaName}','{Status}'"
    df = spark.sql(InsertCommand)
except Exception as e:
    # Handle the error
    print("Error occurred: ", e)

# COMMAND ----------

sqlCommand = f"SELECT MAX(LogTaskControlFlowKey)  AS LogTaskControlFlowKey FROM LogTaskControlFlow WHERE DataFactory = '{dataFactory}'  AND Pipeline = '{Pipeline}' AND startingActivity = '{startingActivity}' AND endingActivity = '{endingActivity}' AND schemaName = '{schemaName}'"
df = spark.sql(sqlCommand)
#print(sqlCommand)
LogTaskControlFlowKey = df.collect()[0]["LogTaskControlFlowKey"]
dbutils.notebook.exit(LogTaskControlFlowKey)
print(LogTaskControlFlowKey)

# COMMAND ----------


