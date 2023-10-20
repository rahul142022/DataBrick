# Databricks notebook source
import logging
import time
import datetime

# COMMAND ----------

MountStorage = "dbfs:/mnt/azuresynapsecoe/databricks/CustomLogs/"
Mode = "debug"

# COMMAND ----------

MountStorage = dbutils.widgets.get("LocationToStoreLog")
Mode = dbutils.widgets.get("LogMode")

# COMMAND ----------

LogFileName = "custom_log"
file_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
p_dir = 'dbfs/test/'
p_filename = LogFileName + file_date+'.log'
p_logfile = p_dir + p_filename 
print(p_logfile)
# create logger with 'Custom_log'


# COMMAND ----------

logger = logging.getLogger('log4j')
logger.setLevel(logging.DEBUG) 
fh = logging.FileHandler('dbfs/test/',mode='w')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
if (logger.hasHandlers()):
     logger.handlers.clear()
logger.addHandler(fh)
logger.addHandler(ch) 

# COMMAND ----------

logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')

# COMMAND ----------

a=12
b= 'Contata'
try:
    c = a+b
except Exception as ex:
    logger.error(ex)
logging.shutdown()

# COMMAND ----------

df= spark.read.text('file:/dbfs/test/')
display(df)

# COMMAND ----------

#.write.mode("append").format("com.databricks.spark.txt").format().(MountStorage)
df.write.mode("append").format("delta").save(MountStorage + '/CustomeErrorLog')

# COMMAND ----------

df_Updated= spark.read.format("delta").load(MountStorage + '/CustomeErrorLog')
display(df_Updated)
