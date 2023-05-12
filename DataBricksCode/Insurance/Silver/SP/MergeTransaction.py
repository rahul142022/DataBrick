# Databricks notebook source
# MAGIC %python 
# MAGIC Sql = """
# MAGIC Merge INTO  TransactionGrowthRate_Silver AS A 
# MAGIC USING 
# MAGIC  (
# MAGIC   SELECT 
# MAGIC        AGENCY_ID
# MAGIC       ,PRIMARY_AGENCY_ID
# MAGIC       ,PROD_ABBR
# MAGIC       ,PROD_LINE
# MAGIC       ,STATE_ABBR
# MAGIC       ,STAT_PROFILE_DATE_YEAR
# MAGIC       ,NB_WRTN_PREM_AMT
# MAGIC       ,WRTN_PREM_AMT
# MAGIC       ,PREV_WRTN_PREM_AMT
# MAGIC       ,PRD_ERND_PREM_AMT
# MAGIC       ,PRD_INCRD_LOSSES_AMT
# MAGIC       ,MONTHS                        
# MAGIC       ,RETENTION_RATIO
# MAGIC       ,LOSS_RATIO
# MAGIC       ,LOSS_RATIO_3YR
# MAGIC       ,GROWTH_RATE_3YR
# MAGIC       ,FileName
# MAGIC       ,CreatedBy
# MAGIC       ,CreatedDate
# MAGIC       ,GoodToImport
# MAGIC   FROM TransactionGrowthRate_Bronze
# MAGIC
# MAGIC )  AS B 
# MAGIC ON A. AGENCY_ID              =   B.AGENCY_ID  AND 
# MAGIC    A.PRIMARY_AGENCY_ID       =   B.PRIMARY_AGENCY_ID  AND 
# MAGIC    A.PROD_ABBR               =   B.PROD_ABBR  AND 
# MAGIC    A.STATE_ABBR              =   B.STATE_ABBR  AND 
# MAGIC    A.STAT_PROFILE_DATE_YEAR  =   B.STAT_PROFILE_DATE_YEAR  AND 
# MAGIC    B.GoodToImport            =   1 
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET
# MAGIC        A.NB_WRTN_PREM_AMT = B.NB_WRTN_PREM_AMT
# MAGIC       ,A.WRTN_PREM_AMT =    B.WRTN_PREM_AMT
# MAGIC       ,A.PREV_WRTN_PREM_AMT = B.PREV_WRTN_PREM_AMT
# MAGIC       ,A.PRD_ERND_PREM_AMT = B.PRD_ERND_PREM_AMT
# MAGIC       ,A.PRD_INCRD_LOSSES_AMT = B.PRD_INCRD_LOSSES_AMT
# MAGIC       ,A.MONTHS = B.MONTHS                       
# MAGIC       ,A.RETENTION_RATIO = B.RETENTION_RATIO
# MAGIC       ,A.LOSS_RATIO = B.LOSS_RATIO
# MAGIC       ,A.LOSS_RATIO_3YR = B.LOSS_RATIO_3YR
# MAGIC       ,A.GROWTH_RATE_3YR = B.GROWTH_RATE_3YR,
# MAGIC    A.CreatedBy    = B.CreatedBy,
# MAGIC    A.CreatedDate  = B.CreatedDate,
# MAGIC    A.FileName     = B.FileName
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT
# MAGIC (
# MAGIC       AGENCY_ID
# MAGIC       ,PRIMARY_AGENCY_ID
# MAGIC       ,PROD_ABBR
# MAGIC       ,PROD_LINE
# MAGIC       ,STATE_ABBR
# MAGIC       ,STAT_PROFILE_DATE_YEAR
# MAGIC       ,NB_WRTN_PREM_AMT
# MAGIC       ,WRTN_PREM_AMT
# MAGIC       ,PREV_WRTN_PREM_AMT
# MAGIC       ,PRD_ERND_PREM_AMT
# MAGIC       ,PRD_INCRD_LOSSES_AMT
# MAGIC       ,MONTHS                        
# MAGIC       ,RETENTION_RATIO
# MAGIC       ,LOSS_RATIO
# MAGIC       ,LOSS_RATIO_3YR
# MAGIC       ,GROWTH_RATE_3YR
# MAGIC       ,FileName
# MAGIC       ,CreatedBy
# MAGIC       ,CreatedDate
# MAGIC )
# MAGIC Values 
# MAGIC (
# MAGIC        B.AGENCY_ID
# MAGIC       ,B.PRIMARY_AGENCY_ID
# MAGIC       ,B.PROD_ABBR
# MAGIC       ,B.PROD_LINE
# MAGIC       ,B.STATE_ABBR
# MAGIC       ,B.STAT_PROFILE_DATE_YEAR
# MAGIC       ,B.NB_WRTN_PREM_AMT
# MAGIC       ,B.WRTN_PREM_AMT
# MAGIC       ,B.PREV_WRTN_PREM_AMT
# MAGIC       ,B.PRD_ERND_PREM_AMT
# MAGIC       ,B.PRD_INCRD_LOSSES_AMT
# MAGIC       ,B.MONTHS                        
# MAGIC       ,B.RETENTION_RATIO
# MAGIC       ,B.LOSS_RATIO
# MAGIC       ,B.LOSS_RATIO_3YR
# MAGIC       ,B.GROWTH_RATE_3YR
# MAGIC       ,B.FileName
# MAGIC       ,B.CreatedBy
# MAGIC       ,B.CreatedDate 		 		
# MAGIC )"""

# COMMAND ----------

# MAGIC %python 
# MAGIC were_errors = False 
# MAGIC try:
# MAGIC     spark.sql(Sql)
# MAGIC except Exception as e:
# MAGIC     were_errors = True
# MAGIC # if were_errors:
# MAGIC #   log failure # you can use data from results variable

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from TransactionGrowthRate_Silver
