# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE TransactionGrowthRate_Silver
# MAGIC ( 
# MAGIC  TransactionKey                BIGINT GENERATED ALWAYS AS IDENTITY (start WITH 0  INCREMENT by 1)
# MAGIC ,AGENCY_ID                     INT
# MAGIC ,PRIMARY_AGENCY_ID             INT
# MAGIC ,PROD_ABBR                     STRING
# MAGIC ,PROD_LINE                     STRING
# MAGIC ,STATE_ABBR                    STRING
# MAGIC ,STAT_PROFILE_DATE_YEAR        INT
# MAGIC ,NB_WRTN_PREM_AMT              DECIMAL(16,2)
# MAGIC ,WRTN_PREM_AMT                 DECIMAL(16,2)
# MAGIC ,PREV_WRTN_PREM_AMT            DECIMAL(16,2)
# MAGIC ,PRD_ERND_PREM_AMT             DECIMAL(16,2)
# MAGIC ,PRD_INCRD_LOSSES_AMT          DECIMAL(16,2)
# MAGIC ,MONTHS                        INT
# MAGIC ,RETENTION_RATIO               STRING
# MAGIC ,LOSS_RATIO                    STRING
# MAGIC ,LOSS_RATIO_3YR                STRING
# MAGIC ,GROWTH_RATE_3YR               STRING
# MAGIC ,FileName            		       STRING
# MAGIC ,CreatedBy           		       INT
# MAGIC ,CreatedDate 		 		           TIMESTAMP
# MAGIC )
# MAGIC USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/TransactionGrowthRate_Silver'
