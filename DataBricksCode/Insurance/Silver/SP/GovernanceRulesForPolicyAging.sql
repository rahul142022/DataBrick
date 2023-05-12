-- Databricks notebook source
-- Fail  when PRIMARY_AGENCY_ID is null 
UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PRIMARY_AGENCY_ID ') 
WHERE  PRIMARY_AGENCY_ID IS  NULL;

-- Fail  when PROD_ABBR is null 
UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PROD_ABBR ') 
WHERE  PROD_ABBR IS  NULL ;

-- Fail  when Agency Id is null 
UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null AGENCY_ID ') 
WHERE  AGENCY_ID IS  NULL;

UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid AGENCY_ID: ',AGENCY_ID) 
WHERE TRY_CAST(AGENCY_ID AS STRING ) IS NULL AND AGENCY_ID IS NOT NULL;

-- Fail  when PROD_ABBR is null 
UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PROD_ABBR ') 
WHERE  PROD_ABBR IS  NULL;

-- Fail  when STAT_PROFILE_DATE_YEAR is null 
UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null STAT_PROFILE_DATE_YEAR ') 
WHERE  STAT_PROFILE_DATE_YEAR IS  NULL;

Merge INTO  PolicyAging_Bronze AS A 
USING 
 (
  SELECT * FROM 
  (
  SELECT ROW_NUMBER () OVER (PARTITION BY AGENCY_ID,PRIMARY_AGENCY_ID,PROD_ABBR,STATE_ABBR,STAT_PROFILE_DATE_YEAR  ORDER BY (SELECT 1)) AS RN, *
  FROM PolicyAging_Bronze
  )  WHERE RN > 1
)  AS B 
ON A. AGENCY_ID  = B.AGENCY_ID  AND 
   A.PRIMARY_AGENCY_ID  = B.PRIMARY_AGENCY_ID  AND 
   A.PROD_ABBR     = B.PROD_ABBR  AND 
   A.STATE_ABBR     = B.STATE_ABBR  AND 
   A.STAT_PROFILE_DATE_YEAR  = B.STAT_PROFILE_DATE_YEAR
WHEN MATCHED THEN
UPDATE SET A.GoodToImport = 0, Error = CONCAT(A.Error,'|','Duplicate Rows ') ;

UPDATE PolicyAging_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid AGENCY_ID: ',AGENCY_ID) 
WHERE TRY_CAST(AGENCY_ID AS INT ) IS NULL AND AGENCY_ID IS NOT NULL;

-- COMMAND ----------



-- COMMAND ----------

-- WITH CTE1 AS (
--   SELECT ROW_NUMBER () OVER (PARTITION BY AGENCY_ID,PRIMARY_AGENCY_ID,PROD_ABBR,STATE_ABBR,STAT_PROFILE_DATE_YEAR  ORDER BY (SELECT 1)) AS RN, *
--   FROM PolicyAging_Silver

-- )
-- UPDATE  PolicyAging_Silver  AS A  
-- SET GoodToImport = 0 
-- FROM CTE1 AS B
-- WHERE A.AGENCY_ID =  B.AGENCY_ID AND 
--       RN > 1

-- COMMAND ----------



-- COMMAND ----------


