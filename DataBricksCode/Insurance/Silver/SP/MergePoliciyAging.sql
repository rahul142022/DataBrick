-- Databricks notebook source
-- MAGIC %python 
-- MAGIC Sql = """
-- MAGIC Merge INTO  PolicyAging_Silver AS A 
-- MAGIC USING 
-- MAGIC  (
-- MAGIC   SELECT 
-- MAGIC        AGENCY_ID					
-- MAGIC       ,PRIMARY_AGENCY_ID          
-- MAGIC       ,PROD_ABBR                  
-- MAGIC       ,PROD_LINE                  
-- MAGIC       ,STATE_ABBR                 
-- MAGIC       ,STAT_PROFILE_DATE_YEAR     
-- MAGIC       ,AGENCY_APPOINTMENT_YEAR    
-- MAGIC       ,ACTIVE_PRODUCERS           
-- MAGIC       ,MAX_AGE                    
-- MAGIC       ,MIN_AGE                    
-- MAGIC       ,VENDOR_IND                 
-- MAGIC       ,VENDOR                     
-- MAGIC       ,PL_START_YEAR              
-- MAGIC       ,PL_END_YEAR                
-- MAGIC       ,COMMISIONS_START_YEAR      
-- MAGIC       ,COMMISIONS_END_YEAR        
-- MAGIC       ,CL_START_YEAR              
-- MAGIC       ,CL_END_YEAR                
-- MAGIC       ,ACTIVITY_NOTES_START_YEAR  
-- MAGIC       ,ACTIVITY_NOTES_END_YEAR    
-- MAGIC       ,CL_BOUND_CT_MDS            
-- MAGIC       ,CL_QUO_CT_MDS              
-- MAGIC       ,CL_BOUND_CT_SBZ            
-- MAGIC       ,CL_QUO_CT_SBZ              
-- MAGIC       ,CL_BOUND_CT_eQT            
-- MAGIC       ,CL_QUO_CT_eQT              
-- MAGIC       ,PL_BOUND_CT_ELINKS         
-- MAGIC       ,PL_QUO_CT_ELINKS           
-- MAGIC       ,PL_BOUND_CT_PLRANK         
-- MAGIC       ,PL_QUO_CT_PLRANK           
-- MAGIC       ,PL_BOUND_CT_eQTte          
-- MAGIC       ,PL_QUO_CT_eQTte            
-- MAGIC       ,PL_BOUND_CT_APPLIED        
-- MAGIC       ,PL_QUO_CT_APPLIED          
-- MAGIC       ,PL_BOUND_CT_TRANSACTNOW    
-- MAGIC       ,PL_QUO_CT_TRANSACTNOW      
-- MAGIC       ,FileName            		
-- MAGIC       ,CreatedBy           		
-- MAGIC       ,CreatedDate
-- MAGIC       ,GoodToImport 		
-- MAGIC
-- MAGIC   FROM PolicyAging_Bronze
-- MAGIC
-- MAGIC )  AS B 
-- MAGIC ON A. AGENCY_ID              =   B.AGENCY_ID  AND 
-- MAGIC    A.PRIMARY_AGENCY_ID       =   B.PRIMARY_AGENCY_ID  AND 
-- MAGIC    A.PROD_ABBR               =   B.PROD_ABBR  AND 
-- MAGIC    A.STATE_ABBR              =   B.STATE_ABBR  AND 
-- MAGIC    A.STAT_PROFILE_DATE_YEAR  =   B.STAT_PROFILE_DATE_YEAR  AND 
-- MAGIC    B.GoodToImport            =   1 
-- MAGIC WHEN MATCHED THEN 
-- MAGIC UPDATE SET
-- MAGIC    A.MAX_AGE      = B.MAX_AGE, 
-- MAGIC    A.MIN_AGE      =  B.MIN_AGE,
-- MAGIC    A.VENDOR_IND   = B.VENDOR_IND,
-- MAGIC    A.CreatedBy    = B.CreatedBy,
-- MAGIC    A.CreatedDate  = B.CreatedDate,
-- MAGIC    A.FileName     = B.FileName
-- MAGIC WHEN NOT MATCHED
-- MAGIC THEN INSERT
-- MAGIC (
-- MAGIC       AGENCY_ID					
-- MAGIC       ,PRIMARY_AGENCY_ID          
-- MAGIC       ,PROD_ABBR                  
-- MAGIC       ,PROD_LINE                  
-- MAGIC       ,STATE_ABBR                 
-- MAGIC       ,STAT_PROFILE_DATE_YEAR     
-- MAGIC       ,AGENCY_APPOINTMENT_YEAR    
-- MAGIC       ,ACTIVE_PRODUCERS           
-- MAGIC       ,MAX_AGE                    
-- MAGIC       ,MIN_AGE                    
-- MAGIC       ,VENDOR_IND                 
-- MAGIC       ,VENDOR                     
-- MAGIC       ,PL_START_YEAR              
-- MAGIC       ,PL_END_YEAR                
-- MAGIC       ,COMMISIONS_START_YEAR      
-- MAGIC       ,COMMISIONS_END_YEAR        
-- MAGIC       ,CL_START_YEAR              
-- MAGIC       ,CL_END_YEAR                
-- MAGIC       ,ACTIVITY_NOTES_START_YEAR  
-- MAGIC       ,ACTIVITY_NOTES_END_YEAR    
-- MAGIC       ,CL_BOUND_CT_MDS            
-- MAGIC       ,CL_QUO_CT_MDS              
-- MAGIC       ,CL_BOUND_CT_SBZ            
-- MAGIC       ,CL_QUO_CT_SBZ              
-- MAGIC       ,CL_BOUND_CT_eQT            
-- MAGIC       ,CL_QUO_CT_eQT              
-- MAGIC       ,PL_BOUND_CT_ELINKS         
-- MAGIC       ,PL_QUO_CT_ELINKS           
-- MAGIC       ,PL_BOUND_CT_PLRANK         
-- MAGIC       ,PL_QUO_CT_PLRANK           
-- MAGIC       ,PL_BOUND_CT_eQTte          
-- MAGIC       ,PL_QUO_CT_eQTte            
-- MAGIC       ,PL_BOUND_CT_APPLIED        
-- MAGIC       ,PL_QUO_CT_APPLIED          
-- MAGIC       ,PL_BOUND_CT_TRANSACTNOW    
-- MAGIC       ,PL_QUO_CT_TRANSACTNOW      
-- MAGIC       ,FileName            		
-- MAGIC       ,CreatedBy           		
-- MAGIC       ,CreatedDate 		 		
-- MAGIC )
-- MAGIC Values 
-- MAGIC (
-- MAGIC          AGENCY_ID					
-- MAGIC          ,PRIMARY_AGENCY_ID          
-- MAGIC          ,PROD_ABBR                  
-- MAGIC          ,PROD_LINE                  
-- MAGIC          ,STATE_ABBR                 
-- MAGIC          ,STAT_PROFILE_DATE_YEAR     
-- MAGIC          ,AGENCY_APPOINTMENT_YEAR    
-- MAGIC          ,ACTIVE_PRODUCERS           
-- MAGIC          ,MAX_AGE                    
-- MAGIC          ,MIN_AGE                    
-- MAGIC          ,VENDOR_IND                 
-- MAGIC          ,VENDOR                     
-- MAGIC          ,PL_START_YEAR              
-- MAGIC          ,PL_END_YEAR                
-- MAGIC          ,COMMISIONS_START_YEAR      
-- MAGIC          ,COMMISIONS_END_YEAR        
-- MAGIC          ,CL_START_YEAR              
-- MAGIC          ,CL_END_YEAR                
-- MAGIC          ,ACTIVITY_NOTES_START_YEAR  
-- MAGIC          ,ACTIVITY_NOTES_END_YEAR    
-- MAGIC          ,CL_BOUND_CT_MDS            
-- MAGIC          ,CL_QUO_CT_MDS              
-- MAGIC          ,CL_BOUND_CT_SBZ            
-- MAGIC          ,CL_QUO_CT_SBZ              
-- MAGIC          ,CL_BOUND_CT_eQT            
-- MAGIC          ,CL_QUO_CT_eQT              
-- MAGIC          ,PL_BOUND_CT_ELINKS         
-- MAGIC          ,PL_QUO_CT_ELINKS           
-- MAGIC          ,PL_BOUND_CT_PLRANK         
-- MAGIC          ,PL_QUO_CT_PLRANK           
-- MAGIC          ,PL_BOUND_CT_eQTte          
-- MAGIC          ,PL_QUO_CT_eQTte            
-- MAGIC          ,PL_BOUND_CT_APPLIED        
-- MAGIC          ,PL_QUO_CT_APPLIED          
-- MAGIC          ,PL_BOUND_CT_TRANSACTNOW    
-- MAGIC          ,PL_QUO_CT_TRANSACTNOW      
-- MAGIC          ,FileName            		
-- MAGIC          ,CreatedBy           		
-- MAGIC          ,CreatedDate 		 		
-- MAGIC )"""

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python 
-- MAGIC were_errors = False 
-- MAGIC try:
-- MAGIC     spark.sql(Sql)
-- MAGIC except Exception as e:
-- MAGIC     were_errors = true
-- MAGIC # if were_errors:
-- MAGIC #   log failure # you can use data from results variable

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print (were_errors)

-- COMMAND ----------


