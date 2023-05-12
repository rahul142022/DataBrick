-- Databricks notebook source
CREATE TABLE Isurance_Gold
(
 IsuranceKey                    BIGINT GENERATED ALWAYS AS IDENTITY (start WITH 0  INCREMENT by 1)
,AGENCY_ID						          INT
,PRIMARY_AGENCY_ID              INT
,PROD_ABBR                      STRING
,PROD_LINE                      STRING
,STATE_ABBR                     STRING
,STAT_PROFILE_DATE_YEAR      	INT  ------- Info
,RETENTION_POLY_QTY          	INT 
,POLY_INFORCE_QTY            	INT 
,PREV_POLY_INFORCE_QTY       	INT 
,NB_WRTN_PREM_AMT              DECIMAL(16,2)--TransactionGrowthRate
,WRTN_PREM_AMT                 DECIMAL(16,2)
,PREV_WRTN_PREM_AMT            DECIMAL(16,2)
,PRD_ERND_PREM_AMT             DECIMAL(16,2)
,PRD_INCRD_LOSSES_AMT          DECIMAL(16,2)
,MONTHS                        INT
,RETENTION_RATIO               STRING
,LOSS_RATIO                    STRING
,LOSS_RATIO_3YR                STRING
,GROWTH_RATE_3YR               STRING
,AGENCY_APPOINTMENT_YEAR        INT ---- Aging 
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
,CreatedBy                      INT
,CreatedDate                    TIMESTAMP
,UpdatedBy                      INT
,UpdatedDate                    TIMESTAMP

)USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/Isurance_Gold'



-- COMMAND ----------

-- %python
-- dbutils.fs.rm('/mnt/Contatablobstorage/DeltaTable/Isurance_Gold',recurse=True)


-- COMMAND ----------

-- DROP TABLE Isurance_Gold

-- COMMAND ----------


