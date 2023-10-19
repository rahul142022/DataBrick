-- Databricks notebook source
UPDATE FRM_Location	
			SET 
					ModifiedOn							=	NULLIF(ModifiedOn, ''),
					LocationName						=	NULLIF(TRIM(LocationName), ''),
					Company								=	NULLIF(TRIM(Company),''),
					StatusReason						=	NULLIF(TRIM(StatusReason),''),
					City								=	NULLIF(TRIM(City),''),
					MainPhone							=	NULLIF(MainPhone,''),
					CreatedOn							=	NULLIF(CreatedOn,''),
					Owner								=	NULLIF(TRIM(Owner), ''),
					PrimaryContact					=   NULLIF(CASE 
																	WHEN PrimaryContact LIKE '%"%' 
																		THEN REPLACE(REPLACE (PrimaryContact,'"',''), ' ','"')
																	ELSE PrimaryContact END,''),
					ActiveCMS							=	NULLIF(REPLACE(REPLACE(ActiveCMS, ' ', ''), '-', '') ,''),
					EMailAddress						=	NULLIF(TRIM(EMailAddress),''),
					AgreementRenewalDate				=	NULLIF(AgreementRenewalDate,''),
					AverageContractValue				=	REPLACE(TRANSLATE(NULLIF(AverageContractValue,''),'(),$','-^^^'),'^',''),
					AverageMemberValue					=	REPLACE(TRANSLATE(NULLIF(AverageMemberValue,''),'(),$','-^^^'),'^',''),
					BillingAccountNumber				=	NULLIF(BillingAccountNumber,''),
					BOE									=	REPLACE(TRANSLATE(NULLIF(BOE,''),'(),$','-^^^'),'^',''),
					Brand								=	NULLIF(Brand,''),	
					Country								=	NULLIF(CASE Country WHEN 'Canada' THEN 'CAN' WHEN 'US' THEN 'USA' WHEN 'UK' THEN 'CYM' ELSE Country END,''),
					--Handling for Draft Percent which come in format like (8.0000000000000002E-2)
					DraftPercent						=	CAST(CAST(REPLACE(TRANSLATE(NULLIF(DraftPercent,''),'(),$','-^^^'),'^','')AS FLOAT) AS NUMERIC(19,6)),
					Dues								=	REPLACE(TRANSLATE(NULLIF(Dues,''),'(),$','-^^^'),'^',''),
					ExitedAgreements					=	NULLIF(ExitedAgreements,''),
					Latitude							=	REPLACE(TRANSLATE(NULLIF(Latitude,''),'(),$','-^^^'),'^',''),
					LocationNumber						=	NULLIF(LocationNumber,''),
					Longitude							=	REPLACE(TRANSLATE(NULLIF(Longitude,''),'(),$','-^^^'),'^',''),
					MonthlyAttrition					=	REPLACE(TRANSLATE(NULLIF(MonthlyAttrition,''),'(),$','-^^^'),'^',''),
					NewAgreements						=	NULLIF(NewAgreements,''),
					--Handling for PIF Percent which come in format like (5.0000000000000003E-2)
					PIFPercent							=	CAST(CAST(REPLACE(TRANSLATE(NULLIF(PIFPercent,''),'(),$','-^^^'),'^','')AS FLOAT) AS NUMERIC(19,6)),
					RemitAmount							=	REPLACE(TRANSLATE(NULLIF(RemitAmount,''),'(),$','-^^^'),'^',''),
					RenewalFee							=	REPLACE(TRANSLATE(NULLIF(RenewalFee,''),'(),$','-^^^'),'^',''),
					RenewalStatus						=	NULLIF(RenewalStatus,''),
					Rolling12MonthAttrition				=	REPLACE(TRANSLATE(NULLIF(Rolling12MonthAttrition,''),'(),$','-^^^'),'^',''),
					SqFt								=	CAST(REPLACE(REPLACE(REPLACE(TRANSLATE(NULLIF(SqFt,''),'(),$','-^^^'),'sf',''),' ',''),'^','') AS STRING),
					State								=	NULLIF(State,''),
					Status							=	NULLIF(Status,''),
					TotalRent							=	REPLACE(TRANSLATE(NULLIF(TotalRent,''),'(),$','-^^^'),'^',''),
					ZIPPostalCode						=	NULLIF(ZIPPostalCode,''),
					AFLPAutoChecklist					=	NULLIF(AFLPAutoChecklist,''),
					AFLPlastPostLaunchDripEmailSent		=	NULLIF(AFLPlastPostLaunchDripEmailSent,''),
					AFLPMarketing						=	NULLIF(AFLPMarketing,''),
					AgreementSigned						=	NULLIF(AgreementSigned,''),
					ClubOS								=	NULLIF(ClubOS,''),
					Currency							=	NULLIF(Currency,''),
					TimeZone							=	NULLIF(TimeZone,''),
					WebsiteURL							=	NULLIF(WebsiteURL,''),
					AFLPAgreement						=	NULLIF(AFLPAgreement,''),
				    CurrentFBC						=	NULLIF(TRIM(CurrentFBC),''),
				    Differentiator					=	NULLIF(Differentiator,''),
					TemporaryClosure					=	NULLIF(TemporaryClosure,''),
					ClosureStartDate					=	NULLIF(ClosureStartDate,''),
					ClosureEndDate					=	NULLIF(ClosureEndDate,''),
					ClubOSStartDate					=   NULLIF(ClubOSStartDate,''),		
					AlloyBillingStartDate				=   CASE 
																WHEN 
																	NULLIF(AlloyBillingStartDate,'') IS NULL 
																	AND  FADate >= '2019-03-27' 
																	AND MonthlyFeeStartDate IS NOT NULL 
																	THEN MonthlyFeeStartDate 
																ELSE NULLIF(AlloyBillingStartDate,'') 
															END,
					FADate							=	NULLIF(FADate,''),	
					Address1							=   NULLIF(TRIM(Address1),''),
					Address2							=   NULLIF(TRIM(Address2),''),
					LeaseTerm							=   NULLIF(TRIM(LeaseTerm),''),
					LeaseExpirationDate				=   NULLIF(LeaseExpirationDate,''),
					FAExpDate							=   NULLIF(FAExpDate,''),
					WaxingAccountNumber					=   NULLIF(WaxingAccountNumber,''),
					ExpressLocation						=   NULLIF(ExpressLocation,''),
					LocationType						=   NULLIF(LocationType,''),
					OpenStatus							=   NULLIF(OpenStatus,''),
					LegalEntity							=   NULLIF(LegalEntity,''),
					MarketType							=   NULLIF(MarketType,''),
					FinancingType						=   NULLIF(FinancingType,''),
					NumberOfRooms						=   NULLIF(NumberOfRooms,''),
					RealEstateStatus					=   NULLIF(RealEstateStatus			,''),				
					ActualOpening				    	=   NULLIF(ActualOpening				,''),    
					ADA								=   CASE WHEN ADA = 'Yes' THEN 1 WHEN ADA = 'No' THEN 0 ELSE NULL END,
					ADADate                       	=   NULLIF(ADADate                    ,''),   
					CerologistTrainingDate         	=   NULLIF(CerologistTrainingDate      ,''),   
					FeeStartDate                  	=   NULLIF(FeeStartDate               ,''),   
					AdFeeStartDate                	=   NULLIF(AdFeeStartDate             ,''),   
					TechFeeStartDate              	=   NULLIF(TechFeeStartDate           ,''),   
					MinimumRoyalty                	=   NULLIF(MinimumRoyalty             ,''),   
					UnopenedStudioFeeStartDate    	=   NULLIF(UnopenedStudioFeeStartDate ,''),   
					UnopenedStudioFee             	=   NULLIF(UnopenedStudioFee          ,''),   
					CharityFeeStartDate           	=   NULLIF(CharityFeeStartDate        ,''),   
					CharityFee                    	=   NULLIF(CharityFee                 ,''),   
					BaseFeePercent                	=   NULLIF(BaseFeePercent             ,''),   
					AdvertisingPercent            	=   NULLIF(AdvertisingPercent         ,''),   
					TechFee                       	=   NULLIF(TechFee                    ,''),   
					IncentivizedIncomeThreshold   	=   NULLIF(IncentivizedIncomeThreshold,''),   
					LateFeePercent					=   NULLIF(LateFeePercent				,''),
					LayoutApprovedDate               =    NULLIF(LayoutApprovedDate 		,''),
					UnopenedClubFeeStartDate			=   NULLIF(UnopenedClubFeeStartDate	,''),
					UnopenedClubMonthlyFee			=   NULLIF(UnopenedClubMonthlyFee		,''),
					HeartFirstRecipient				=   NULLIF(HeartFirstRecipient		,''),
					MonthlyFeeStartDate				=   NULLIF(MonthlyFeeStartDate		,''),
					SoftwareFee						=   NULLIF(SoftwareFee				,''),
					SoftwareFeeTotal					=   NULLIF(SoftwareFeeTotal			,''),
					PrincipalOwner					=   NULLIF(PrincipalOwner				,''),
					Region							=	NULLIF(Region						,''),
					FBCRegion							=	NULLIF(FBCRegion					,''),
					BaseCamp							=	NULLIF(BaseCamp,''),
					Presale							=	NULLIF(Presale,''),
					PresaleStartDate					=	NULLIF(PresaleStartDate,''),
					LastSiteVisitDate					=	NULLIF(LastSiteVisitDate,''),
					AFRequiredOnSiteTrainingDate		=	NULLIF(AFRequiredOnSiteTrainingDate,''),
					AFOnSiteTrainingCompleted			=	NULLIF(AFOnSiteTrainingCompleted,''),
					FDDPrice							=   REPLACE(TRANSLATE(NULLIF(FDDPrice,''),'(),$','-^^^'),'^',''),
					TermDate							=   NULLIF(TermDate,''),
					BOEVerified						=	NULLIF(BOEVerified				,''),
					CloseDate							=   NULLIF(CloseDate,''),
					PrimaryClubContactEmail			=   NULLIF(TRIM(PrimaryClubContactEmail),''),
					ExceptionOnMonthlyBilling			=	NULLIF(TRIM(ExceptionOnMonthlyBilling),''),
					RequiredFASigning					=	NULLIF(TRIM(RequiredFASigning),''),
					FranchiseFeeType					=	NULLIF(TRIM(FranchiseFeeType),''),
					InitialPayment					=	REPLACE(TRANSLATE(NULLIF(InitialPayment,''),'(),$','-^^^'),'^',''),
					BalanceDue						=	REPLACE(TRANSLATE(NULLIF(BalanceDue,''),'(),$','-^^^'),'^',''),
					BalanceCollected					=	NULLIF(TRIM(BalanceCollected),''),
					TotalDevelopmentFee				=	REPLACE(TRANSLATE(NULLIF(TotalDevelopmentFee,''),'(),$','-^^^'),'^',''),
					RevenueRecognition				=	REPLACE(TRANSLATE(NULLIF(RevenueRecognition,''),'(),$','-^^^'),'^',''),
					CharityAddendum					=	CASE WHEN CharityAddendum = 'Yes' THEN 1 WHEN CharityAddendum = 'No' THEN 0 ELSE NULL END,
					CharityDiscount					=	REPLACE(TRANSLATE(NULLIF(CharityDiscount,''),'(),$','-^^^'),'^',''),
					EmergingMarketDiscount			=	REPLACE(TRANSLATE(NULLIF(EmergingMarketDiscount,''),'(),$','-^^^'),'^',''),
					VeteranName						=	NULLIF(TRIM(VeteranName),''),
					AHBillingClub						=	CASE WHEN AHBillingClub = 'Yes' THEN 1 WHEN AHBillingClub = 'No' THEN 0 ELSE NULL END,
					WebMaintenanceFee					=	REPLACE(TRANSLATE(NULLIF(WebMaintenanceFee,''),'(),$','-^^^'),'^',''),
					TotalCollectedWithoutTaxes		=	REPLACE(TRANSLATE(NULLIF(TotalCollectedWithoutTaxes,''),'(),$','-^^^'),'^',''),
					RemitDate							=	NULLIF(RemitDate,''),
					InstallmentAccounts				=	REPLACE(TRANSLATE(NULLIF(InstallmentAccounts,''),'(),$','-^^^'),'^',''),
					PIFAccounts						=	REPLACE(TRANSLATE(NULLIF(PIFAccounts,''),'(),$','-^^^'),'^',''),
					ClubConnectPremiumService			=	CASE WHEN ClubConnectPremiumService = 'Yes' THEN 1 WHEN ClubConnectPremiumService = 'No' THEN 0 ELSE NULL END,
					TermDate_ClubHistory				= NULLIF(TermDate_ClubHistory,''),
					CloseDate_ClubHistory				= NULLIF(CloseDate_ClubHistory,''),
					TermDate_StudioHistory			= NULLIF(TermDate_StudioHistory,''),
					CloseDate_StudioHistory			= NULLIF(CloseDate_StudioHistory,''),
					Billing_Platform_Exception		= NULLIF(Billing_Platform_Exception,''),
					DMA								= NULLIF(DMA,''),
					Provision_Project_Status			= NULLIF(Provision_Project_Status,''),
					Refresh_Completed					= NULLIF(Refresh_Completed,''),
					KPSoftworks						= NULLIF(KPSoftworks, ''),
					KPSoftworksStartDate				= NULLIF(KPSoftworksStartDate, ''),
					KPSoftworksEndDate				= NULLIF(KPSoftworksEndDate, '')


-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "FRM_Location"

-- COMMAND ----------

SELECT AverageContractValue,*  FROM FRM_Location limit 10 

-- AverageContractValue

-- COMMAND ----------

UPDATE FRM_Location
SET GoodToImport  = 1
 WHERE LocationId = '{E86F63A4-B793-EA11-80D5-000D3A1710FF}'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC # Use DESCRIBE command to get information about the Delta table
-- MAGIC describe_query = f"DESCRIBE {table_name}"
-- MAGIC describe_results = spark.sql(describe_query)
-- MAGIC
-- MAGIC # Convert the describe results to a DataFrame
-- MAGIC describe_df = describe_results.toPandas()
-- MAGIC
-- MAGIC # Date_Filter
-- MAGIC Date_Filter = ['ModifiedOn','CreatedOn','AgreementRenewalDate','AgreementRenewalDate','ADADate', 'CerologistTrainingDate','CerologistTrainingDate','FeeStartDate','AdFeeStartDate','TechFeeStartDate','UnopenedStudioFeeStartDate','CharityFeeStartDate','LayoutApprovedDate','UnopenedClubFeeStartDate','MonthlyFeeStartDate']
-- MAGIC describe_df_date = describe_df[describe_df['col_name'].isin(Date_Filter)]
-- MAGIC describe_df_date['New_Data_Type'] = 'timestamp'
-- MAGIC
-- MAGIC # Int_Filter
-- MAGIC Int_Filter = ['AverageContractValue']
-- MAGIC describe_df_Int = describe_df[describe_df['col_name'].isin(Int_Filter)]
-- MAGIC describe_df_Int['New_Data_Type'] = 'DECIMAL(19,6)'
-- MAGIC ## Concat date Filter and Int filter
-- MAGIC describe_results = pd.concat([describe_df_date, describe_df_Int],axis=0, ignore_index=True)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC
-- MAGIC flag_column_name = "GoodToImport"
-- MAGIC Error_column_name = "Error"
-- MAGIC
-- MAGIC
-- MAGIC for index, row in describe_results.iterrows():
-- MAGIC     column_name = row['col_name']
-- MAGIC     data_type = row['New_Data_Type']
-- MAGIC     # print(data_type)
-- MAGIC     sql  = f"""UPDATE {table_name} SET {flag_column_name} = 0 ,
-- MAGIC                 {Error_column_name} = CONCAT({Error_column_name},'| Error Column Name is {column_name}')     
-- MAGIC                 WHERE {column_name} IS NOT NULL AND 
-- MAGIC                       TRY_CAST({column_name} AS {data_type}) IS NULL  AND  
-- MAGIC                       {flag_column_name} = 1
-- MAGIC             """ 
-- MAGIC         # Update the flag column to 1 if the data type is valid
-- MAGIC     spark.sql(sql)
-- MAGIC

-- COMMAND ----------

SELECT * FROM FRM_Location WHERE GoodToImport = 0   

-- COMMAND ----------

			UPDATE FRM_Location
			SET Status = 'Inactive'
			WHERE LocationID = '{32599B72-C6DE-ED11-A89E-0022481C554E}'  AND 
				 (SELECT COUNT(1) FROM FRM_Location WHERE LocationNumber = 'BCF10203') > 1;

		UPDATE FRM_Location
			SET  GoodToImport = 0
					,Error = CONCAT(Error,' || NULL Location GUID')
		WHERE  LocationID IS NULL 
    



-- COMMAND ----------



-- COMMAND ----------


