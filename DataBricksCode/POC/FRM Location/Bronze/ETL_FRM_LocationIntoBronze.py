# Databricks notebook source
# Import Modules
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import input_file_name
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Repos/dhruvj@contata.com/DataBrick/DataBricksCode/POC/FRM Location/Bronze/00_Global_Variable"

# COMMAND ----------

dbutils.widgets.text("ModifiedBy", "123")
ModifiedBy = dbutils.widgets.get("ModifiedBy")
delta_table_name = "FRM_Location"

# COMMAND ----------

Source_Path = f"{Source_Folder_Path}FRM/raw/"
date = datetime.now()
GoodToImport = 1 

# COMMAND ----------

circuits_schema = StructType(fields = [
		StructField('Location ID'		,StringType(), True),
		StructField('1 mile pop'		,StringType(), True),
		StructField('3 mile pop'		,StringType(), True),
		StructField('5 mile pop'		,StringType(), True),
		StructField('Actual Opening'    ,StringType(), True),
		StructField('AverageTerm'       ,StringType(), True),
		StructField('Avg  monthly gross personal training revenue'  ,StringType(), True),
		StructField('BOE Verified'						,StringType(), True),
		StructField('Close Date'						,StringType(), True),
		StructField('Competition'						,StringType(), True),
		StructField('E-mail Address'					,StringType(), True),
		StructField('Exchange Rate'						,StringType(), True),
		StructField('Existing Sales Rep'				,StringType(), True),
		StructField('Express Location'					,StringType(), True),
		StructField('Financing Type'					,StringType(), True),
		StructField('Gross Rent'					    ,StringType(), True),
		StructField('Hours of Operation'				,StringType(), True),
		StructField('Lack of marketing'					,StringType(), True),
		StructField('Landlord Issues'				   ,StringType(), True),
		StructField('Legal Entity'					   ,StringType(), True),
		StructField('Location Type'					   ,StringType(), True),
		StructField('Market Type'					   ,StringType(), True),
		StructField('Median income > $50000'		   ,StringType(), True),
		StructField('Members  Dues and PIF'           ,StringType(), True),
		StructField('Monthly Fee Amt'                 ,StringType(), True),
		StructField('Multi-Purpose Room'              ,StringType(), True),
		StructField('Multi-Purpose Sq Ft'             ,StringType(), True),
		StructField('No Operating Capital'            ,StringType(), True),
		StructField('No Sales Revenue'                ,StringType(), True),
		StructField('Number of Rooms'                 ,StringType(), True),
		StructField('Online Signup'                   ,StringType(), True),
		StructField('Open Status'                     ,StringType(), True),
		StructField('Open Temporary Pending Relo'     ,StringType(), True),
		StructField('Opened On'                       ,StringType(), True),
		StructField('Original Franchisee Open Date'   ,StringType(), True),
		StructField('Other'                           ,StringType(), True),
		StructField('Other Equipment'                 ,StringType(), True),
		StructField('Other Operating System'          ,StringType(), True),
		StructField('Owner'                           ,StringType(), True),
		StructField('Partnership Dispute'             ,StringType(), True),
		StructField('Payroll'                         ,StringType(), True),
		StructField('Percent Owner 1'                 ,StringType(), True),
		StructField('Percent Owner 2'                 ,StringType(), True),
		StructField('Percent Owner 3'                 ,StringType(), True),
		StructField('Percent Owner 4'                 ,StringType(), True),
		StructField('Percent Owner 5'                 ,StringType(), True),
		StructField('Percent Owner 6'                 ,StringType(), True),
		StructField('Person  Training Revenue'        ,StringType(), True),
		StructField('Personal Issues'                 ,StringType(), True),
		StructField('Personal Training'               ,StringType(), True),
		StructField('Personal Training Revenue'       ,StringType(), True),
		StructField('Primary Contact'                 ,StringType(), True),
		StructField('Principal Operator'              ,StringType(), True),
		StructField('Principal Owner'            ,StringType(), True),
		StructField('Problem %'                  ,StringType(), True),
		StructField('Profit Loss'                ,StringType(), True),
		StructField('Remit Amount Last Quarter'  ,StringType(), True),
		StructField('Remit Amount Quarter Change',StringType(), True),
		StructField('Sale Date'                  ,StringType(), True),
		StructField('Satellite Club'             ,StringType(), True),
		StructField('Site'                       ,StringType(), True),
		StructField('Stage'                      ,StringType(), True),
		StructField('Team Workouts'              ,StringType(), True),
		StructField('Territory'                  ,StringType(), True),
		StructField('Total Contracts'            ,StringType(), True),
		StructField('Transfer Fee'               ,StringType(), True),
		StructField('Utilities'                  ,StringType(), True),
		StructField('Veteran'                    ,StringType(), True),
		StructField('Virtual Coaching'           ,StringType(), True),
		StructField('Waxing Account#'            ,StringType(), True),
		StructField('Website'                    ,StringType(), True),
		StructField('Active CMS'                 ,StringType(), True),
		StructField('AFLP Auto Checklist'        ,StringType(), True),
		StructField('AFLP In-club Training Lead' ,StringType(), True),
		StructField('AFLP last Post-Launch Drip email sent',StringType(), True),
		StructField('AFLP Marketing'             ,StringType(), True),
		StructField('AFLP Notes'                 ,StringType(), True),
		StructField('Agreement Renewal Date'     ,StringType(), True),
		StructField('Agreement Signed'           ,StringType(), True),
		StructField('Average Contract Value'     ,StringType(), True),
		StructField('Average Member Value'       ,StringType(), True),
		StructField('Billing Account Number'     ,StringType(), True),
		StructField('BOE'                        ,StringType(), True),
		StructField('Brand'                      ,StringType(), True),
		StructField('City'                       ,StringType(), True),
		StructField('Club OS'                    ,StringType(), True),
		StructField('Club Ready'                 ,StringType(), True),
		StructField('Company'                    ,StringType(), True),
		StructField('Country'                    ,StringType(), True),
		StructField('Created On'                 ,StringType(), True),
		StructField('Currency'                   ,StringType(), True),
		StructField('Draft %'                    ,StringType(), True),
		StructField('Dues'                       ,StringType(), True),
		StructField('Exited Agreements'          ,StringType(), True),
		StructField('Latitude'                   ,StringType(), True),
		StructField('Location'                   ,StringType(), True),
		StructField('Location #'                 ,StringType(), True),
		StructField('Longitude'                  ,StringType(), True),
		StructField('Main Phone'                 ,StringType(), True),
		StructField('Modified On'                ,StringType(), True),
		StructField('Monthly Attrition'          ,StringType(), True),
		StructField('NEW Agreements'             ,StringType(), True),
		StructField('PIF %'                      ,StringType(), True),
		StructField('Remit Amount'               ,StringType(), True),
		StructField('Renewal Fee'                ,StringType(), True),
		StructField('Renewal Status'             ,StringType(), True),
		StructField('Rolling 12 Month Attrition' ,StringType(), True),
		StructField('Sq Ft'                      ,StringType(), True),
		StructField('State'                      ,StringType(), True),
		StructField('Status'                     ,StringType(), True),
		StructField('Status Reason'              ,StringType(), True),
		StructField('Time Zone'                  ,StringType(), True),
		StructField('Total Rent'                 ,StringType(), True),
		StructField('Website URL'                ,StringType(), True),
		StructField('ZIP Postal Code'            ,StringType(), True),
		StructField('AFLP Agreement'             ,StringType(), True),
		StructField('Current C2I'                ,StringType(), True),
		StructField('Differentiator'             ,StringType(), True),
		StructField('Temporary Closure'          ,StringType(), True),
		StructField('Closure Start Date'         ,StringType(), True),
		StructField('Closure End Date'           ,StringType(), True),
		StructField('Club OS Start Date'         ,StringType(), True),
		StructField('Alloy Billing Start date'   ,StringType(), True),
		StructField('FA Date'                    ,StringType(), True),
		StructField('Address 1'                  ,StringType(), True),
		StructField('Address 2'                  ,StringType(), True),
		StructField('Lease Term'                 ,StringType(), True),
		StructField('Lease Expiration Date'      ,StringType(), True),
		StructField('FA Exp Date'                ,StringType(), True),
		StructField('ADA'                        ,StringType(), True),
		StructField('Ad Fee Start Date (WTC)'    ,StringType(), True),
		StructField('ADA Date'                   ,StringType(), True),
		StructField('Advertising % (WTC)'        ,StringType(), True),
		StructField('Base Fee % (WTC)'           ,StringType(), True),
		StructField('Cerologist Training (WTC)'  ,StringType(), True),
		StructField('Charity Fee'                ,StringType(), True),
		StructField('Charity Fee Start Date'     ,StringType(), True),
		StructField('Fee Start Date (WTC)'       ,StringType(), True),
		StructField('General Ad Fund (AF)'           ,StringType(), True),
		StructField('General Ad Fund Start Date (AF)',StringType(), True),
		StructField('Heart First Recipient'          ,StringType(), True),
		StructField('Incentivized Income Threshold (contains a string of formulas) (WTC)' ,StringType(), True),
		StructField('Late Fee % (WTC)'                         ,StringType(), True),
		StructField('Layout Approved Date'                     ,StringType(), True),
		StructField('Lease Term (Months)'                      ,StringType(), True),
		StructField('Local Ad Fund Amt (AF)'                   ,StringType(), True),
		StructField('Local Ad Fund Start Date (AF)'            ,StringType(), True),
		StructField('Minimum Royalty (WTC)'                    ,StringType(), True),
		StructField('Monthly Fee Start Date (AF)'              ,StringType(), True),
		StructField('Real Estate Status'                       ,StringType(), True),
		StructField('Software Fee � Tax Rate (AF)'             ,StringType(), True),
		StructField('Software Fee (AF)'                        ,StringType(), True),
		StructField('Software Fee Total (AF)'                  ,StringType(), True),
		StructField('Tech Fee (WTC)'                           ,StringType(), True),
		StructField('Tech Fee Start Date (WTC)'                ,StringType(), True),
		StructField('Unopened Club Fee � Start Date (AF)'      ,StringType(), True),
		StructField('Unopened Club Monthly Fee (AF)'           ,StringType(), True),
		StructField('Unopened Studio Fee (WTC)'                ,StringType(), True),
		StructField('Unopened Studio Fee Start Date (WTC)'     ,StringType(), True),
		StructField('Region'                                   ,StringType(), True),
		StructField('FBC-Region'                               ,StringType(), True),
		StructField('Basecamp'                                 ,StringType(), True),
		StructField('Presale Start Date'                       ,StringType(), True),
		StructField('Presale'                                  ,StringType(), True),
		StructField('Last Site Visit date'                     ,StringType(), True),
		StructField('AF Required On-Site Training Date'        ,StringType(), True),
		StructField('AF On-Site Training Completed'            ,StringType(), True),
		StructField('Local Ad Tier'                            ,StringType(), True),
		StructField('FDD Price'                                ,StringType(), True),
		StructField('Term Date'                                ,StringType(), True),
		StructField('Primary Club Contact E-mail'              ,StringType(), True),
		StructField('Exception on Monthly Billing'             ,StringType(), True),
		StructField('Required FA Signing'                      ,StringType(), True),
		StructField('Franchise fee Type'                       ,StringType(), True),
		StructField('Initial Payment'                          ,StringType(), True),
		StructField('Balance Due'                              ,StringType(), True),
		StructField('Balance Collected'                        ,StringType(), True),
		StructField('Total Development Fee'                    ,StringType(), True),
		StructField('Revenue Recognition'                      ,StringType(), True),
		StructField('Charity Addendum'                         ,StringType(), True),
		StructField('Charity Discount'                         ,StringType(), True),
		StructField('Emerging Market Discount'                 ,StringType(), True),
		StructField('Veteran Name'                             ,StringType(), True),
		StructField('AH Billing Club'                          ,StringType(), True),
		StructField('Web Maintenance Fee'                      ,StringType(), True),
		StructField('Total Collected w o Taxes'                ,StringType(), True),
		StructField('Remit Date'                               ,StringType(), True),
		StructField('Installment Accounts'                     ,StringType(), True),
		StructField('PIF Accounts'                             ,StringType(), True),
		StructField('Club Connect Premium Service'             ,StringType(), True),
		StructField('Term Date (Club History)'                 ,StringType(), True),
		StructField('Close Date (Club History)'                ,StringType(), True),
		StructField('Term Date (Studio History)'               ,StringType(), True),
		StructField('Close Date (Studio History)'              ,StringType(), True),
		StructField('Virtual Music License'                    ,StringType(), True),
		StructField('Live Stream Local'                        ,StringType(), True),
		StructField('In-Studio Music License'                  ,StringType(), True),
		StructField('Physical Therapy'                         ,StringType(), True),
		StructField('Billing Platform Exception'               ,StringType(), True),
		# StructField('Exception on Monthly Billing#'            ,StringType(), True),
		StructField('DMA'                                      ,StringType(), True),
		StructField('Provision Project Status'                 ,StringType(), True),
		StructField('Refresh Completed'                        ,StringType(), True),
		StructField('KP Softworks'                             ,StringType(), True),
		StructField('KP Softworks Start Date'                  ,StringType(), True),
		StructField('KP Softworks End'						   ,StringType(), True),
		StructField('KP Softworks End Date'					   ,StringType(), True),
  
])

# COMMAND ----------

Location_DF = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .option('inferSchema', True) \
    .option('sep',"|")\
    .csv(Source_Path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add another columns 

# COMMAND ----------

Location_DF_Data = Location_DF.withColumn("FileName", input_file_name())\
            .withColumn("ModifiedBy", lit(ModifiedBy).cast("int"))\
            .withColumn("ModifiedDate",lit(date))\
            .withColumn("GoodToImport",lit(GoodToImport).cast("int"))\
            .withColumn("Error",lit(None)
            )

# COMMAND ----------

# Location_DF_Data.createOrReplaceTempView("FRM_Location")

# COMMAND ----------

    Location_DF_Renamed =   Location_DF_Data \
    	.withColumnRenamed('Location ID','LocationID')\
		.withColumnRenamed('1 mile pop','OneMilePop')\
		.withColumnRenamed('3 mile pop','ThreeMilePop')\
		.withColumnRenamed('5 mile pop','FiveMilePop')\
		.withColumnRenamed('Actual Opening','ActualOpening')\
		.withColumnRenamed('AverageTerm','AverageTerm')\
		.withColumnRenamed('Avg  monthly gross personal training revenue','AvgMonthlyGrossPersonalTrainingReveue')\
		.withColumnRenamed('BOE Verified','BOEVerified')\
		.withColumnRenamed('Close Date','CloseDate')\
		.withColumnRenamed('Competition',	'Competition')\
		.withColumnRenamed('Competition',	'Competition')\
		.withColumnRenamed('E-mail Address','EMailAddress')\
		.withColumnRenamed('Exchange Rate','ExchangeRate')\
		.withColumnRenamed('Existing Sales Rep','ExistingSalesRep')\
		.withColumnRenamed('Express Location','ExpressLocation')\
		.withColumnRenamed('Financing Type','FinancingType')\
		.withColumnRenamed('Gross Rent','GrossRent')\
		.withColumnRenamed('Hours of Operation','HoursOfOperation')\
		.withColumnRenamed('Lack of marketing','LackOfMarketing')\
		.withColumnRenamed('Landlord Issues','LandlordIssues')\
		.withColumnRenamed('Legal Entity','LegalEntity')\
		.withColumnRenamed('Location Type','LocationType')\
		.withColumnRenamed('Market Type','MarketType')\
		.withColumnRenamed('Median income > $50000','MedianIncome')\
		.withColumnRenamed('Members  Dues and PIF','MembersDuesAndPIF')\
		.withColumnRenamed('Monthly Fee Amt','MonthlyFeeAmt')\
		.withColumnRenamed('Multi-Purpose Room','MultiPurposeRoom')\
		.withColumnRenamed('Multi-Purpose Sq Ft','MultiPurposeSqFt')\
		.withColumnRenamed('No Operating Capital','NoOperatingCapital')\
		.withColumnRenamed('No Sales Revenue','NoSalesRevenue')\
		.withColumnRenamed('Number of Rooms','NumberOfRooms')\
		.withColumnRenamed('Online Signup','OnlineSignup')\
		.withColumnRenamed('Open Status','OpenStatus')\
		.withColumnRenamed('Open Temporary Pending Relo','OpenTemporaryPendingRelo')\
		.withColumnRenamed('Opened On','OpenedOn')\
		.withColumnRenamed('Original Franchisee Open Date','OriginalFranchiseeOpenDate')\
		.withColumnRenamed('Other','Other')\
		.withColumnRenamed('Other Equipment','OtherEquipment')\
		.withColumnRenamed('Other Operating System','OtherOperatingSystem')\
		.withColumnRenamed('Owner','Owner')\
		.withColumnRenamed('Partnership Dispute','PartnershipDispute')\
		.withColumnRenamed('Payroll','Payroll')\
		.withColumnRenamed('Percent Owner 1','PercentOwnerOne')\
		.withColumnRenamed('Percent Owner 2','PercentOwnerTwo')\
		.withColumnRenamed('Percent Owner 3','PercentOwnerThree')\
		.withColumnRenamed('Percent Owner 4','PercentOwnerFour')\
		.withColumnRenamed('Percent Owner 5','PercentOwnerFive')\
		.withColumnRenamed('Percent Owner 6','PercentOwnerSix')\
		.withColumnRenamed('Person  Training Revenue','PersonTrainingRevenue')\
		.withColumnRenamed('Personal Issues','PersonalIssues')\
		.withColumnRenamed('Personal Training','PersonalTraining')\
		.withColumnRenamed('Personal Training Revenue','PersonalTrainingRevenue')\
		.withColumnRenamed('Primary Contact','PrimaryContact')\
		.withColumnRenamed('Principal Operator','PrincipalOperator')\
		.withColumnRenamed('Principal Owner','PrincipalOwner')\
		.withColumnRenamed('Problem %','ProblemPercent')\
		.withColumnRenamed('Profit Loss','ProfitLoss')\
		.withColumnRenamed('Remit Amount Last Quarter','RemitAmountLastQuarter')\
		.withColumnRenamed('Remit Amount Quarter Change','RemitAmountQuarterChange')\
		.withColumnRenamed('Sale Date','SaleDate')\
		.withColumnRenamed('Satellite Club','SatelliteClub')\
		.withColumnRenamed('Site','Site')\
		.withColumnRenamed('Stage','Stage')\
		.withColumnRenamed('Team Workouts','TeamWorkouts')\
		.withColumnRenamed('Territory','Territory')\
		.withColumnRenamed('Total Contracts','TotalContracts')\
		.withColumnRenamed('Transfer Fee','TransferFee')\
		.withColumnRenamed('Utilities','Utilities')\
		.withColumnRenamed('Veteran',	'Veteran')\
		.withColumnRenamed('Virtual Coaching','VirtualCoaching')\
		.withColumnRenamed('Waxing Account#','WaxingAccountNumber')\
		.withColumnRenamed('Website','Website')\
		.withColumnRenamed('Active CMS','ActiveCMS')\
		.withColumnRenamed('AFLP Auto Checklist','AFLPAutoChecklist')\
		.withColumnRenamed('AFLP In-club Training Lead','AFLPInclubTrainingLead')\
		.withColumnRenamed('AFLP last Post-Launch Drip email sent','AFLPLastPostLaunchDripEmailSent')\
		.withColumnRenamed('AFLP Marketing','AFLPMarketing')\
		.withColumnRenamed('AFLP Notes','AFLPNotes')\
		.withColumnRenamed('Agreement Renewal Date','AgreementRenewalDate')\
		.withColumnRenamed('Agreement Signed','AgreementSigned')\
		.withColumnRenamed('Average Contract Value','AverageContractValue')\
		.withColumnRenamed('Average Member Value','AverageMemberValue')\
		.withColumnRenamed('Billing Account Number','BillingAccountNumber')\
		.withColumnRenamed('BOE','BOE')\
		.withColumnRenamed('Brand','Brand')\
		.withColumnRenamed('City','City')\
		.withColumnRenamed('Club OS','ClubOS')\
		.withColumnRenamed('Club Ready','ClubReady')\
		.withColumnRenamed('Company','Company')\
		.withColumnRenamed('Country','Country')\
		.withColumnRenamed('Created On','CreatedOn')\
		.withColumnRenamed('Currency','Currency')\
		.withColumnRenamed('Draft %','DraftPercent')\
	    .withColumnRenamed('Dues','Dues')\
		.withColumnRenamed('Exited Agreements','ExitedAgreements')\
		.withColumnRenamed('Latitude','Latitude')\
		.withColumnRenamed('Location','LocationName')\
		.withColumnRenamed('Location #','LocationNumber')\
		.withColumnRenamed('Longitude','Longitude')\
		.withColumnRenamed('Main Phone','MainPhone')\
		.withColumnRenamed('Modified On','ModifiedOn')\
		.withColumnRenamed('Monthly Attrition','MonthlyAttrition')\
		.withColumnRenamed('NEW Agreements','NEWAgreements')\
		.withColumnRenamed('PIF %','PIFPercent')\
		.withColumnRenamed('Remit Amount','RemitAmount')\
		.withColumnRenamed('Renewal Fee','RenewalFee')\
		.withColumnRenamed('Renewal Status','RenewalStatus')\
		.withColumnRenamed('Rolling 12 Month Attrition','Rolling12MonthAttrition')\
		.withColumnRenamed('Sq Ft','SqFt')\
		.withColumnRenamed('Status','Status')\
		.withColumnRenamed('Status Reason','StatusReason')\
		.withColumnRenamed('Time Zone','TimeZone')\
		.withColumnRenamed('Total Rent','TotalRent')\
		.withColumnRenamed('Website URL','WebsiteURL')\
		.withColumnRenamed('ZIP Postal Code','ZIPPostalCode')\
		.withColumnRenamed('AFLP Agreement','AFLPAgreement')\
		.withColumnRenamed('Current C2I',							'CurrentFBC')\
		.withColumnRenamed('Differentiator','Differentiator')\
		.withColumnRenamed('Temporary Closure','TemporaryClosure')\
		.withColumnRenamed('Closure Start Date','ClosureStartDate')\
		.withColumnRenamed('Closure End Date','ClosureEndDate')\
		.withColumnRenamed('Club OS Start Date',		'ClubOSStartDate')\
		.withColumnRenamed('Alloy Billing Start date',	'AlloyBillingStartDate')\
		.withColumnRenamed('FA Date',	'FADate')\
		.withColumnRenamed('Address 1','Address1')\
		.withColumnRenamed('Address 2','Address2')\
		.withColumnRenamed('Lease Term','LeaseTerm')\
		.withColumnRenamed('Lease Expiration Date','LeaseExpirationDate')\
	    .withColumnRenamed('ADA',												  'ADA')\
		.withColumnRenamed('FA Exp Date','FAExpDate')\
		.withColumnRenamed('Ad Fee Start Date (WTC)',							  'AdFeeStartDate')\
		.withColumnRenamed('ADA Date',											  'ADADate')\
		.withColumnRenamed('Advertising % (WTC)',								  'AdvertisingPercent')\
		.withColumnRenamed('Base Fee % (WTC)',									  'BaseFeePercent')\
		.withColumnRenamed('Cerologist Training (WTC)',							  'CerologistTrainingDate')\
		.withColumnRenamed('Charity Fee',										  'CharityFee')\
		.withColumnRenamed('Charity Fee Start Date',							  'CharityFeeStartDate')\
		.withColumnRenamed('Fee Start Date (WTC)',								  'FeeStartDate')\
		.withColumnRenamed('General Ad Fund (AF)',								  'GeneralAdFund')\
		.withColumnRenamed('General Ad Fund Start Date (AF)',					  'GeneralAdFundStartDate')\
		.withColumnRenamed('Heart First Recipient',								  'HeartFirstRecipient')\
		.withColumnRenamed('Incentivized Income Threshold (contains a string of formulas) (WTC)','IncentivizedIncomeThreshold')\
		.withColumnRenamed('Late Fee % (WTC)','LateFeePercent')\
		.withColumnRenamed('Layout Approved Date','LayoutApprovedDate')\
		.withColumnRenamed('Lease Term (Months)',								  'LeaseTermInMonths')\
		.withColumnRenamed('Local Ad Fund Amt (AF)',							  'LocalAdUndAmt')\
		.withColumnRenamed('Local Ad Fund Start Date (AF)',						  'LocalAdFundStartDate')\
		.withColumnRenamed('Minimum Royalty (WTC)',								  'MinimumRoyalty')\
		.withColumnRenamed('Monthly Fee Start Date (AF)',						  'MonthlyFeeStartDate')\
		.withColumnRenamed('Real Estate Status',								  'RealEstateStatus')\
		.withColumnRenamed('Software Fee  Tax Rate (AF)',						  'SoftwareFeeTaxRate')\
		.withColumnRenamed('Software Fee (AF)',									  'SoftwareFee')\
		.withColumnRenamed('Software Fee Total (AF)',							  'SoftwareFeeTotal')\
		.withColumnRenamed('Tech Fee (WTC)',									  'TechFee')\
		.withColumnRenamed('Tech Fee Start Date (WTC)',							  'TechFeeStartDate')\
		.withColumnRenamed('Unopened Club Fee  Start Date (AF)',			  'UnopenedClubFeeStartDate')\
		.withColumnRenamed('Unopened Club Monthly Fee (AF)',					  'UnopenedClubMonthlyFee')\
		.withColumnRenamed('Unopened Studio Fee (WTC)',							  'UnopenedStudioFee')\
		.withColumnRenamed('Unopened Studio Fee Start Date (WTC)','UnopenedStudioFeeStartDate')\
		.withColumnRenamed('Region','Region')\
		.withColumnRenamed('FBC-Region','FBCRegion')\
		.withColumnRenamed('Basecamp','BaseCamp')\
		.withColumnRenamed('Presale','Presale')\
		.withColumnRenamed('Presale Start Date','PresaleStartDate')\
		.withColumnRenamed('Last Site Visit date', 'LastSiteVisitDate')\
		.withColumnRenamed('AF Required On-Site Training Date', 'AFRequiredOnSiteTrainingDate')\
		.withColumnRenamed('AF On-Site Training Completed','AFOnSiteTrainingCompleted')\
		.withColumnRenamed('FDD Price','FDDPrice')\
		.withColumnRenamed('Term Date','TermDate')\
		.withColumnRenamed('Primary Club Contact E-mail','PrimaryClubContactEmail')\
		.withColumnRenamed('Exception on Monthly Billing','ExceptionOnMonthlyBilling')\
		.withColumnRenamed('Required FA Signing','RequiredFASigning')\
		.withColumnRenamed('Franchise fee Type','FranchiseFeeType')\
		.withColumnRenamed('Initial Payment','InitialPayment')\
		.withColumnRenamed('Balance Due','BalanceDue')\
		.withColumnRenamed('Balance Collected','BalanceCollected')\
		.withColumnRenamed('Total Development Fee','TotalDevelopmentFee')\
		.withColumnRenamed('Revenue Recognition','RevenueRecognition')\
		.withColumnRenamed('Charity Addendum','CharityAddendum')\
		.withColumnRenamed('Charity Discount','CharityDiscount')\
		.withColumnRenamed('Emerging Market Discount','EmergingMarketDiscount')\
		.withColumnRenamed('Veteran Name','VeteranName')\
		.withColumnRenamed('AH Billing Club','AHBillingClub')\
		.withColumnRenamed('Web maintenance Fee','WebMaintenanceFee')\
		.withColumnRenamed('Total Collected w/o Taxes','TotalCollectedWithoutTaxes')\
		.withColumnRenamed('Remit Date','RemitDate')\
		.withColumnRenamed('Installment Accounts','InstallmentAccounts')\
		.withColumnRenamed('PIF Accounts','PIFAccounts')\
		.withColumnRenamed('Club Connect Premium Service','ClubConnectPremiumService')\
		.withColumnRenamed('Term Date (Club History)','TermDate_ClubHistory')\
		.withColumnRenamed('Close Date (Club History)','CloseDate_ClubHistory')\
		.withColumnRenamed('Term Date (Studio History)','TermDate_StudioHistory')\
		.withColumnRenamed('Close Date (Studio History)','CloseDate_StudioHistory')\
		.withColumnRenamed('Billing Platform Exception','Billing_Platform_Exception')\
    .withColumnRenamed('Exception on Monthly Billing#','Billing_Platform_Exception')\
		.withColumnRenamed('Provision Project Status','Provision_Project_Status')\
		.withColumnRenamed('Refresh Completed','Refresh_Completed')\
		.withColumnRenamed('KP Softworks','KPSoftworks')\
		.withColumnRenamed('KP Softworks Start Date','KPSoftworksStartDate')\
		.withColumnRenamed('KP Softworks End Date','KPSoftworksEndDate')\
    .withColumnRenamed('Software Fee � Tax Rate (AF)','SoftwareFeeTaxRate')\
 		.withColumnRenamed('Unopened Club Fee � Start Date (AF)','UnopenedClubFeeStartDate')\
    .withColumnRenamed('Local Ad Tier','LocalAdTier')\
    .withColumnRenamed('Total Collected w o Taxes','TotalCollectedWithoutTaxes')\
    .withColumnRenamed('Virtual Music License','VirtualMusicLicense')\
    .withColumnRenamed('Live Stream Local','LiveStreamLocal')\
    .withColumnRenamed('In-Studio Music License','InStudioMusicLicense')\
    .withColumnRenamed('Physical Therapy','PhysicalTherapy')\
    .withColumnRenamed('KP Softworks End','KPSoftworksEnd')\
            
       	
               
           
       
      

# COMMAND ----------



# COMMAND ----------

# display(Location_DF_Renamed)

# COMMAND ----------

Location_DF_Renamed.write.format("delta").mode("overwrite").save('/mnt/azuresynapsecoe/databricks/Delta/FRM/Location/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM FRM_Location limit 10 

# COMMAND ----------


