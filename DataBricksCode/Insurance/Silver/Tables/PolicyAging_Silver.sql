-- Databricks notebook source
CREATE TABLE PolicyAging_Silver
(
 PolicyAgingKey                    BIGINT GENERATED ALWAYS AS IDENTITY (start WITH 0  INCREMENT by 1)
,AGENCY_ID						          INT
,PRIMARY_AGENCY_ID              INT
,PROD_ABBR                      STRING
,PROD_LINE                      STRING
,STATE_ABBR                     STRING
,STAT_PROFILE_DATE_YEAR         INT
,AGENCY_APPOINTMENT_YEAR        INT
,ACTIVE_PRODUCERS               INT
,MAX_AGE                        INT
,MIN_AGE                        INT 
,VENDOR_IND                     STRING
,VENDOR                         STRING
,PL_START_YEAR                  INT
,PL_END_YEAR                    INT
,COMMISIONS_START_YEAR          INT
,COMMISIONS_END_YEAR            INT
,CL_START_YEAR                  INT
,CL_END_YEAR                    INT
,ACTIVITY_NOTES_START_YEAR      INT
,ACTIVITY_NOTES_END_YEAR        INT
,CL_BOUND_CT_MDS                INT
,CL_QUO_CT_MDS                  INT
,CL_BOUND_CT_SBZ                INT
,CL_QUO_CT_SBZ                  INT
,CL_BOUND_CT_eQT                INT
,CL_QUO_CT_eQT                  INT
,PL_BOUND_CT_ELINKS             INT
,PL_QUO_CT_ELINKS               INT
,PL_BOUND_CT_PLRANK             INT
,PL_QUO_CT_PLRANK               INT
,PL_BOUND_CT_eQTte              INT
,PL_QUO_CT_eQTte                INT
,PL_BOUND_CT_APPLIED            INT
,PL_QUO_CT_APPLIED              INT
,PL_BOUND_CT_TRANSACTNOW        INT
,PL_QUO_CT_TRANSACTNOW          INT
,FileName            		        STRING
,CreatedBy           		        INT
,CreatedDate 		 		            TIMESTAMP
)USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/PolicyAging_Silver'

-- COMMAND ----------

-- DROP TABLE PolicyAging_Silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/mnt/Contatablobstorage/DeltaTable/PolicyAging_Silver',recurse=True)

-- COMMAND ----------


