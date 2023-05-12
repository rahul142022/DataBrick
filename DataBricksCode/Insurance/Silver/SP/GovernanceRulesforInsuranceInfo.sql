-- Databricks notebook source
 ---SELECT * FROM PolicyInfo_Bronze WHERE GoodToImport = 0  limit 10

-- COMMAND ----------

-- Fail  when PRIMARY_AGENCY_ID is null 
UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PRIMARY_AGENCY_ID ') 
WHERE  PRIMARY_AGENCY_ID IS  NULL;

-- Fail  when PROD_ABBR is null 
UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PROD_ABBR ') 
WHERE  PROD_ABBR IS  NULL ;

-- Fail  when Agency Id is null 
UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null AGENCY_ID ') 
WHERE  AGENCY_ID IS  NULL;

UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid AGENCY_ID: ',AGENCY_ID) 
WHERE TRY_CAST(AGENCY_ID AS INT ) IS NULL AND AGENCY_ID IS NOT NULL;

-- Fail  when PROD_ABBR is null 
UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null PROD_ABBR ') 
WHERE  PROD_ABBR IS  NULL;

-- Fail  when STAT_PROFILE_DATE_YEAR is null 
UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Null STAT_PROFILE_DATE_YEAR ') 
WHERE  STAT_PROFILE_DATE_YEAR IS  NULL;

UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid RETENTION_POLY_QTY: ',RETENTION_POLY_QTY) 
WHERE TRY_CAST(RETENTION_POLY_QTY AS INT ) IS NULL AND RETENTION_POLY_QTY IS NOT NULL;

UPDATE PolicyInfo_Bronze
SET GoodToImport = 0, 
    Error = CONCAT(Error,'|','Invalid RETENTION_POLY_QTY: ',POLY_INFORCE_QTY) 
WHERE TRY_CAST(POLY_INFORCE_QTY AS INT ) IS NULL AND POLY_INFORCE_QTY IS NOT NULL;


Merge INTO  PolicyInfo_Bronze AS A 
USING 
 (
  SELECT * FROM 
  (
  SELECT ROW_NUMBER () OVER (PARTITION BY AGENCY_ID,PRIMARY_AGENCY_ID,PROD_ABBR,STATE_ABBR,STAT_PROFILE_DATE_YEAR  ORDER BY (SELECT 1)) AS RN, *
  FROM PolicyInfo_Bronze
  )  WHERE RN > 1
)  AS B 
ON A. AGENCY_ID  = B.AGENCY_ID  AND 
   A.PRIMARY_AGENCY_ID  = B.PRIMARY_AGENCY_ID  AND 
   A.PROD_ABBR     = B.PROD_ABBR  AND 
   A.STATE_ABBR     = B.STATE_ABBR  AND 
   A.STAT_PROFILE_DATE_YEAR  = B.STAT_PROFILE_DATE_YEAR
WHEN MATCHED THEN
UPDATE SET GoodToImport = 0;
