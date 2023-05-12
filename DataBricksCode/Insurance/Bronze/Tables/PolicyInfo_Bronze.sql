-- Databricks notebook source
CREATE TABLE PolicyInfo_Bronze
( 
 AGENCY_ID					         STRING 
,PRIMARY_AGENCY_ID           STRING 
,PROD_ABBR                   STRING 
,PROD_LINE                   STRING 
,STATE_ABBR                  STRING 
,STAT_PROFILE_DATE_YEAR      STRING 
,RETENTION_POLY_QTY          STRING 
,POLY_INFORCE_QTY            STRING 
,PREV_POLY_INFORCE_QTY       STRING 
,FileName            		     STRING
,CreatedBy           		     INT
,CreatedDate 		 		         TIMESTAMP
,GoodToImport 		 		       INT 
,Error 						           STRING
)
USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/PolicyInfo_Bronze'

-- COMMAND ----------


