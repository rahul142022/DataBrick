-- Databricks notebook source
CREATE TABLE PolicyAging_Bronze
(
 AGENCY_ID						          STRING
,PRIMARY_AGENCY_ID              STRING
,PROD_ABBR                      STRING
,PROD_LINE                      STRING
,STATE_ABBR                     STRING
,STAT_PROFILE_DATE_YEAR         STRING
,AGENCY_APPOINTMENT_YEAR        STRING
,ACTIVE_PRODUCERS               STRING
,MAX_AGE                        STRING
,MIN_AGE                        STRING
,VENDOR_IND                     STRING
,VENDOR                         STRING
,PL_START_YEAR                  STRING
,PL_END_YEAR                    STRING
,COMMISIONS_START_YEAR          STRING
,COMMISIONS_END_YEAR            STRING
,CL_START_YEAR                  STRING
,CL_END_YEAR                    STRING
,ACTIVITY_NOTES_START_YEAR      STRING
,ACTIVITY_NOTES_END_YEAR        STRING
,CL_BOUND_CT_MDS                STRING
,CL_QUO_CT_MDS                  STRING
,CL_BOUND_CT_SBZ                STRING
,CL_QUO_CT_SBZ                  STRING
,CL_BOUND_CT_eQT                STRING
,CL_QUO_CT_eQT                  STRING
,PL_BOUND_CT_ELINKS             STRING
,PL_QUO_CT_ELINKS               STRING
,PL_BOUND_CT_PLRANK             STRING
,PL_QUO_CT_PLRANK               STRING
,PL_BOUND_CT_eQTte              STRING
,PL_QUO_CT_eQTte                STRING
,PL_BOUND_CT_APPLIED            STRING
,PL_QUO_CT_APPLIED              STRING
,PL_BOUND_CT_TRANSACTNOW        STRING
,PL_QUO_CT_TRANSACTNOW          STRING
,FileName            		        STRING
,CreatedBy           		        INT
,CreatedDate 		 		            TIMESTAMP
,GoodToImport 		 		          INT 
,Error 						              STRING
)USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/PolicyAging_Bronze'

-- COMMAND ----------


