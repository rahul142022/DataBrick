# Databricks notebook source
# get the scope list 
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="AzureDataBricksScop")

# COMMAND ----------

# JDBC URL of the database
jdbc_url = "jdbc:sqlserver://ml-workspace.database.windows.net:1433;database=ML_Backup"
# Table to read from
table_name = "ME.MLMProspectList"
User_Name = dbutils.secrets.get(scope = "AzureDataBricksScop", key = "AzureUserNameSecret")
Password = dbutils.secrets.get(scope = "AzureDataBricksScop", key = "AzurePassword")

# COMMAND ----------

employees_table = (spark.read
  .format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", table_name)
  .option("user", User_Name)
  .option("password", Password)
  .load()
)

# COMMAND ----------

employees_table.count()

# COMMAND ----------

display(employees_table)

# COMMAND ----------

Using push down query to fetch the data 

# COMMAND ----------

pushdown_query = "(select * from craiglistfile  where zipcode = 61131  and state = 'IL'  and MonthOrder = 38) as emp_alias"

employees_table_pushdown = (spark.read
  .format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", pushdown_query)
  .option("user", User_Name)
  .option("password", Password)
  .load()
)

# COMMAND ----------

display(employees_table_pushdown)
