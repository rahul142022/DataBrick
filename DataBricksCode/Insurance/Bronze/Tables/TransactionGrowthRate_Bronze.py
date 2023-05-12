# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE TransactionGrowthRate_Bronze
# MAGIC ( 
# MAGIC  AGENCY_ID                     STRING
# MAGIC ,PRIMARY_AGENCY_ID             STRING
# MAGIC ,PROD_ABBR                     STRING
# MAGIC ,PROD_LINE                     STRING
# MAGIC ,STATE_ABBR                    STRING
# MAGIC ,STAT_PROFILE_DATE_YEAR        STRING
# MAGIC ,NB_WRTN_PREM_AMT              STRING
# MAGIC ,WRTN_PREM_AMT                 STRING
# MAGIC ,PREV_WRTN_PREM_AMT            STRING
# MAGIC ,PRD_ERND_PREM_AMT             STRING
# MAGIC ,PRD_INCRD_LOSSES_AMT          STRING
# MAGIC ,MONTHS                        STRING
# MAGIC ,RETENTION_RATIO               STRING
# MAGIC ,LOSS_RATIO                    STRING
# MAGIC ,LOSS_RATIO_3YR                STRING
# MAGIC ,GROWTH_RATE_3YR               STRING
# MAGIC ,FileName            		       STRING
# MAGIC ,CreatedBy           		       INT
# MAGIC ,CreatedDate 		 		           TIMESTAMP
# MAGIC ,GoodToImport 		 		         INT 
# MAGIC ,Error 						             STRING
# MAGIC )
# MAGIC USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/TransactionGrowthRate_Bronze'
