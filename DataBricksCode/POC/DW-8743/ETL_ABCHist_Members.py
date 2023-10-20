# Databricks notebook source
# Import Modules
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import input_file_name
from datetime import datetime

# COMMAND ----------

#Get the values of global perameters 
# %run "./Repos/dhruvj@contata.com/DataBrick/DataBricksCode/POC/FRM Location/Bronze/00_Global_Variable"
Source_Folder_Path =  "/mnt/azuresynapsecoe/databricks/"

# COMMAND ----------

# Create Widgets for getting ModifiedBy values from ADF (Like a input parameter) and assign that value to ModifiedBy
dbutils.widgets.text("ModifiedBy", "123")
ModifiedBy = dbutils.widgets.get("ModifiedBy")
# assign the current date to variable
Today = datetime.now()
# assign the GoodToImport by default 1 
GoodToImport = 1 

# COMMAND ----------

# Create the path where source file available
Source_Path = f"{Source_Folder_Path}ABC/raw/"
Member_Delta = f"{Source_Folder_Path}/Delta/ABC/Member_Bronze/"
print(Source_Path)
# Read JSON 
Json_Data = spark.read.json(Source_Path)
Json_Data.createOrReplaceTempView("Json_Data_Results")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Json_Data_Results
# MAGIC

# COMMAND ----------

# Member data parse from JSON and store that into Delta table 
def Insert_ABC_Member_Data_Into_Delta_Table():
    Sql_Member = f"""
                    SELECT  
                    AgreementGuid
                    ,ClubGuid
                    ,VendorSyncDate
                    ,Member['MemberGuid'] 				     AS MemberGuid 
                    ,Member['MembershipType']                AS MembershipType 
                    ,Member['Status']                        AS Status
                    ,Member['FirstName']                     AS FirstName 
                    ,Member['MiddleInitial']                 AS MiddleInitial 
                    ,Member['LastName']                      AS LastName 
                    ,Member['KeyFob']                        AS KeyFob 
                    ,Member['Number']                        AS Number 
                    ,Member['Address1']                      AS Address1 
                    ,Member['Address2']                      AS Address2 
                    ,Member['City']                          AS City 
                    ,Member['State']                         AS State
                    ,Member['ZipCode']                       AS ZipCode 
                    ,Member['Country']                       AS Country 
                    ,Member['DateOfBirth']                   AS DateOfBirth 
                    ,Member['Gender']                        AS Gender 
                    ,Member['Employer']                      AS Employer 
                    ,Member['Occupation']                    AS Occupation 
                    ,Member['Email']                         AS Email 
                    ,Member['EmergencyComment']         AS EmergencyComment
                    ,Member['EmergencyPhone']                AS EmergencyPhone 
                    ,Member['EmergencyPhoneExt']             AS EmergencyPhoneExt 
                    ,Member['HomePhone']                     AS HomePhone 
                    ,Member['HomePhoneExt']                  AS HomePhoneExt 
                    ,Member['MobilePhone' ]                  AS MobilePhone 
                    ,Member['WorkPhone']                     AS WorkPhone 
                    ,Member['WorkPhoneExt']                  AS WorkPhoneExt 
                    ,Member['IsPrimary']                     AS IsPrimary 
                    FROM Json_Data_Results
    """
    print(Member_Delta)
    try:
        df_Member = spark.sql(Sql_Member)
        df_Member.write.format("delta").mode("overwrite").save(Member_Delta)
        
    except Exception as e:
        # Handle the error
        print("Error occurred: ", e)

# COMMAND ----------

# Execute Function for ingeting Member data into Delta table 
Insert_ABC_Member_Data_Into_Delta_Table()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/mnt/azuresynapsecoe/databricks//Delta/ABC/Member_Bronze/`

# COMMAND ----------

# Member data parse from JSON and store that into Delta table 
def Insert_ABC_Contract_Data_Into_Delta_Table():
    Sql_Contract = f"""
            SELECT 
            AgreementGuid
            ClubGuid
            ,VendorSyncDate
            ,Contracts[0]
            ,Contracts[0]['ContractGuid'] 			AS  ContractGuid 
            ,Contracts[0]['ContractClass']           AS  ContractClass
            ,Contracts[0]['ContractType']            AS  ContractType 
            ,Contracts[0]['Status']                  AS  Status
            ,Contracts[0]['BeginDate']               AS  BeginDate
            ,Contracts[0]['EndDate']                 AS  EndDate
            ,Contracts[0]['DueDay']                  AS  DueDay
            ,Contracts[0]['FirstPaymentDate']        AS  FirstPaymentDate
            ,Contracts[0]['Frequency']               AS  Frequency
            ,Contracts[0]['PayAmount']               AS  PayAmount
            ,Contracts[0]['PayAmountCoupon']         AS  PayAmountCoupon
            ,Contracts[0]['PayCount']                AS  PayCount
            ,Contracts[0]['RenewToOpen']             AS  RenewToOpen
            ,Contracts[0]['SignDate']                AS  SignDate
            ,Contracts[0]['Term1']                    AS  Term
            FROM Json_Data_Results
            """


# COMMAND ----------

# MAGIC           %sql
# MAGIC           SELECT 
# MAGIC             AgreementGuid
# MAGIC             ClubGuid
# MAGIC             ,VendorSyncDate
# MAGIC             ,Contracts[0]
# MAGIC             ,Contracts[0]['ContractGuid'] 			AS  ContractGuid 
# MAGIC             ,Contracts[0]['ContractClass']           AS  ContractClass
# MAGIC             ,Contracts[0]['ContractType']            AS  ContractType 
# MAGIC             ,Contracts[0]['Status']                  AS  Status
# MAGIC             ,Contracts[0]['BeginDate']               AS  BeginDate
# MAGIC             ,Contracts[0]['EndDate']                 AS  EndDate
# MAGIC             ,Contracts[0]['DueDay']                  AS  DueDay
# MAGIC             ,Contracts[0]['FirstPaymentDate']        AS  FirstPaymentDate
# MAGIC             ,Contracts[0]['Frequency']               AS  Frequency
# MAGIC             ,Contracts[0]['PayAmount']               AS  PayAmount
# MAGIC             ,Contracts[0]['PayAmountCoupon']         AS  PayAmountCoupon
# MAGIC             ,Contracts[0]['PayCount']                AS  PayCount
# MAGIC             ,Contracts[0]['RenewToOpen']             AS  RenewToOpen
# MAGIC             ,Contracts[0]['SignDate']                AS  SignDate
# MAGIC             ,Contracts[0]['Term']                    AS  Term
# MAGIC             FROM Json_Data_Results

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM Json_Data_Results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,Contract.*  FROM (
# MAGIC SELECT        AgreementGuid
# MAGIC             ,ClubGuid
# MAGIC             ,VendorSyncDate, explode(Contracts)  AS Contract FROM Json_Data_Results
# MAGIC )

# COMMAND ----------


