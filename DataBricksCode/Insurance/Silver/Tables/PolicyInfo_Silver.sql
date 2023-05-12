-- Databricks notebook source
CREATE TABLE PolicyInfo_Silver
( 
PolicyInfoKey   			BIGINT GENERATED ALWAYS AS IDENTITY (start WITH 0  INCREMENT by 1)
,AGENCY_ID					          INT 
,PRIMARY_AGENCY_ID           INT 
,PROD_ABBR                   STRING 
,PROD_LINE                   STRING 
,STATE_ABBR                  STRING 
,STAT_PROFILE_DATE_YEAR      INT 
,RETENTION_POLY_QTY          INT 
,POLY_INFORCE_QTY            INT 
,PREV_POLY_INFORCE_QTY       INT 
,FileName            		 STRING
,CreatedBy           		 INT
,CreatedDate 		 		 TIMESTAMP
) USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/PolicyInfo_Silver'

-- COMMAND ----------


