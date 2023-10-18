# Databricks notebook source
def mount_adls(container_name):
  storageAccountAccessKey = dbutils.secrets.get(scope = "AzureDataBricksScop", key = "storageAccountAccessKey")
  storage_account_name = "azuresynapsecoe"
  # storageAccountAccessKey = "M+ED8AZHX6TeJcEye2A21zlGXIiTJtXxA4beSbliSoFJCTU/yNHLKmoAA1ZJ9/PPk6qC1uj7aKB3+ASt9eYbsw=="
  #sasToken = <sas-token>
  blobContainerName = "databricks"
  mountPoint = f"/mnt/{storage_account_name}/{container_name}"
  if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    try:
      dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(container_name, storage_account_name),
        mount_point = mountPoint,
        extra_configs = {'fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net': storageAccountAccessKey}
        #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storage_account_name + '.blob.core.windows.net': sasToken}
      )
      print("mount succeeded!")
    except Exception as e:
      print("mount exception", e)



# COMMAND ----------

mount_adls("databricks")

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/azuresynapsecoe/databricks

# COMMAND ----------

dbutils.fs.unmount("/mnt/azuresynapsecoe/databricks")


# COMMAND ----------


