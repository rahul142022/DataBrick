-- Databricks notebook source
-- SELECT * FROM policyinfo_bronze limit 10 

-- COMMAND ----------

-- SELECT * FROM PolicyInfo_Silver

-- COMMAND ----------

-- TRUNCATE TABLE PolicyInfo_Silver

-- COMMAND ----------

Merge INTO  PolicyInfo_Silver AS A 
USING 
 (
  SELECT 
         AGENCY_ID				
         ,PRIMARY_AGENCY_ID         
         ,PROD_ABBR                  
         ,PROD_LINE                  
         ,STATE_ABBR                 
         ,STAT_PROFILE_DATE_YEAR    
         ,RETENTION_POLY_QTY        
         ,POLY_INFORCE_QTY          
         ,PREV_POLY_INFORCE_QTY  
         ,GoodToImport
         ,FileName, 
         CreatedBy,
         CreatedDate
  FROM PolicyInfo_Bronze

)  AS B 
ON A. AGENCY_ID              =   B.AGENCY_ID  AND 
   A.PRIMARY_AGENCY_ID       =   B.PRIMARY_AGENCY_ID  AND 
   A.PROD_ABBR               =   B.PROD_ABBR  AND 
   A.STATE_ABBR              =   B.STATE_ABBR  AND 
   A.STAT_PROFILE_DATE_YEAR  =   B.STAT_PROFILE_DATE_YEAR  AND 
   B.GoodToImport            =   1 
WHEN MATCHED THEN 
UPDATE SET
   A.RETENTION_POLY_QTY = B.RETENTION_POLY_QTY, 
   A.POLY_INFORCE_QTY   =  B.POLY_INFORCE_QTY,
   A.PREV_POLY_INFORCE_QTY = B.PREV_POLY_INFORCE_QTY
WHEN NOT MATCHED
THEN INSERT
(
            AGENCY_ID				
         ,PRIMARY_AGENCY_ID         
         ,PROD_ABBR                  
         ,PROD_LINE                  
         ,STATE_ABBR                 
         ,STAT_PROFILE_DATE_YEAR    
         ,RETENTION_POLY_QTY        
         ,POLY_INFORCE_QTY          
         ,PREV_POLY_INFORCE_QTY  ,FileName, CreatedBy,CreatedDate
)
Values 
(
            AGENCY_ID				
         ,PRIMARY_AGENCY_ID         
         ,PROD_ABBR                  
         ,PROD_LINE                  
         ,STATE_ABBR                 
         ,STAT_PROFILE_DATE_YEAR    
         ,RETENTION_POLY_QTY        
         ,POLY_INFORCE_QTY          
         ,PREV_POLY_INFORCE_QTY  ,FileName, CreatedBy,CreatedDate
)

-- COMMAND ----------

SELECT * FROM PolicyInfo_Silver limit 10

-- COMMAND ----------


