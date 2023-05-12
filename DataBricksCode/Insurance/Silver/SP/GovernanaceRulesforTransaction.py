# Databricks notebook source
# MAGIC %sql
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Null PRIMARY_AGENCY_ID ') 
# MAGIC WHERE  PRIMARY_AGENCY_ID IS  NULL;
# MAGIC
# MAGIC -- Fail  when PROD_ABBR is null 
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Null PROD_ABBR ') 
# MAGIC WHERE  PROD_ABBR IS  NULL ;
# MAGIC
# MAGIC -- Fail  when Agency Id is null 
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Null AGENCY_ID ') 
# MAGIC WHERE  AGENCY_ID IS  NULL;
# MAGIC
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Invalid AGENCY_ID: ',AGENCY_ID) 
# MAGIC WHERE TRY_CAST(AGENCY_ID AS STRING ) IS NULL AND AGENCY_ID IS NOT NULL;
# MAGIC
# MAGIC -- Fail  when PROD_ABBR is null 
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Null PROD_ABBR ') 
# MAGIC WHERE  PROD_ABBR IS  NULL;
# MAGIC
# MAGIC -- Fail  when STAT_PROFILE_DATE_YEAR is null 
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Null STAT_PROFILE_DATE_YEAR ') 
# MAGIC WHERE  STAT_PROFILE_DATE_YEAR IS  NULL;
# MAGIC
# MAGIC Merge INTO  TransactionGrowthRate_Bronze AS A 
# MAGIC USING 
# MAGIC  (
# MAGIC   SELECT * FROM 
# MAGIC   (
# MAGIC   SELECT ROW_NUMBER () OVER (PARTITION BY AGENCY_ID,PRIMARY_AGENCY_ID,PROD_ABBR,STATE_ABBR,STAT_PROFILE_DATE_YEAR  ORDER BY (SELECT 1)) AS RN, *
# MAGIC   FROM TransactionGrowthRate_Silver
# MAGIC   )  WHERE RN > 1
# MAGIC )  AS B 
# MAGIC ON A. AGENCY_ID  = B.AGENCY_ID  AND 
# MAGIC    A.PRIMARY_AGENCY_ID  = B.PRIMARY_AGENCY_ID  AND 
# MAGIC    A.PROD_ABBR     = B.PROD_ABBR  AND 
# MAGIC    A.STATE_ABBR     = B.STATE_ABBR  AND 
# MAGIC    A.STAT_PROFILE_DATE_YEAR  = B.STAT_PROFILE_DATE_YEAR
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET A.GoodToImport = 0, Error = CONCAT(A.Error,'|','Duplicate Rows ') ;
# MAGIC
# MAGIC UPDATE TransactionGrowthRate_Bronze
# MAGIC SET GoodToImport = 0, 
# MAGIC     Error = CONCAT(Error,'|','Invalid AGENCY_ID: ',AGENCY_ID) 
# MAGIC WHERE TRY_CAST(AGENCY_ID AS INT ) IS NULL AND AGENCY_ID IS NOT NULL;
