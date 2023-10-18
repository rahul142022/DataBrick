# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://databricks@contatadev.blob.core.windows.net",
mount_point = "/mnt/Contatablobstorage",
extra_configs = {"fs.azure.account.key.contatadev.blob.core.windows.net":
"IC1uRdkJHrcakDLDUX6qeW7tAS0G6l0HgT7owQH9O/zPMnXAN12AdVgGwWwpAxHfNJUYYAI5tI9LpBBxgekkEg=="})


# COMMAND ----------

dbutils.fs.ls("/mnt/Contatablobstorage")
