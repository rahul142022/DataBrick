-- Databricks notebook source
CREATE TABLE FRM_Location 
(
LocationID											STRING
,OneMilePop                                         STRING
,ThreeMilePop                                       STRING
,FiveMilePop                                        STRING
,ActualOpening                                      STRING
,AverageTerm                                        STRING
,AvgMonthlyGrossPersonalTrainingReveue              STRING
,BOEVerified                                        STRING
,CloseDate                                          STRING
,Competition                                        STRING
,EMailAddress                                       STRING
,ExchangeRate                                       STRING
,ExistingSalesRep                                   STRING
,ExpressLocation                                    STRING
,FinancingType                                      STRING
,GrossRent                                          STRING
,HoursOfOperation                                   STRING
,LackOfMarketing                                    STRING
,LandlordIssues                                     STRING
,LegalEntity                                        STRING
,LocationType                                       STRING
,MarketType                                         STRING
,MedianIncome                                       STRING
,MembersDuesAndPIF                                  STRING
,MonthlyFeeAmt                                      STRING
,MultiPurposeRoom                                   STRING
,MultiPurposeSqFt                                   STRING
,NoOperatingCapital                                 STRING
,NoSalesRevenue                                     STRING
,NumberOfRooms                                      STRING
,OnlineSignup                                       STRING
,OpenStatus                                         STRING
,OpenTemporaryPendingRelo                           STRING
,OpenedOn                                           STRING
,OriginalFranchiseeOpenDate                         STRING
,Other                                              STRING
,OtherEquipment                                     STRING
,OtherOperatingSystem                               STRING
,Owner                                              STRING
,PartnershipDispute                                 STRING
,Payroll                                            STRING
,PercentOwnerOne                                    STRING
,PercentOwnerTwo                                    STRING
,PercentOwnerThree                                  STRING
,PercentOwnerFour                                   STRING
,PercentOwnerFive                                   STRING
,PercentOwnerSix                                    STRING
,PersonTrainingRevenue                              STRING
,PersonalIssues                                     STRING
,PersonalTraining                                   STRING
,PersonalTrainingRevenue                            STRING
,PrimaryContact                                     STRING
,PrincipalOperator                                  STRING
,PrincipalOwner                                     STRING
,ProblemPercent                                     STRING
,ProfitLoss                                         STRING
,RemitAmountLastQuarter                             STRING
,RemitAmountQuarterChange                           STRING
,SaleDate                                           STRING
,SatelliteClub                                      STRING
,Site                                               STRING
,Stage                                              STRING
,TeamWorkouts                                       STRING
,Territory                                          STRING
,TotalContracts                                     STRING
,TransferFee                                        STRING
,Utilities                                          STRING
,Veteran                                            STRING
,VirtualCoaching                                    STRING
,WaxingAccountNumber                                STRING
,Website                                            STRING
,ActiveCMS                                          STRING
,AFLPAutoChecklist                                  STRING
,AFLPInclubTrainingLead                             STRING
,AFLPLastPostLaunchDripEmailSent  STRING
,AFLPMarketing									STRING
,AFLPNotes                                      STRING
,AgreementRenewalDate                           STRING
,AgreementSigned                                STRING
,AverageContractValue                           STRING
,AverageMemberValue                             STRING
,BillingAccountNumber                           STRING
,BOE                                            STRING
,Brand                                          STRING
,City                                           STRING
,ClubOS                                         STRING
,ClubReady                                      STRING
,Company                                        STRING
,Country                                        STRING
,CreatedOn                                      STRING
,Currency                                       STRING
,DraftPercent                                   STRING
,Dues                                           STRING
,ExitedAgreements                               STRING
,Latitude                                       STRING
,LocationName                                   STRING
,LocationNumber                                 STRING
,Longitude                                      STRING
,MainPhone                                      STRING
,ModifiedOn                                     STRING
,MonthlyAttrition                               STRING
,NEWAgreements                                  STRING
,PIFPercent                                     STRING
,RemitAmount                                    STRING
,RenewalFee                                     STRING
,RenewalStatus                                  STRING
,Rolling12MonthAttrition                        STRING
,SqFt                                           STRING
,State                                          STRING
,Status                                         STRING
,StatusReason                                   STRING
,TimeZone                                       STRING
,TotalRent                                      STRING
,WebsiteURL                                     STRING
,ZIPPostalCode                                  STRING
,AFLPAgreement                                  STRING
,CurrentFBC                                     STRING
,Differentiator                                 STRING
,TemporaryClosure                               STRING
,ClosureStartDate                               STRING
,ClosureEndDate                                 STRING
,ClubOSStartDate                                STRING
,AlloyBillingStartDate                          STRING
,FADate                                         STRING
,Address1                                       STRING
,Address2                                       STRING
,LeaseTerm                                      STRING
,LeaseExpirationDate                            STRING
,FAExpDate                                      STRING
,ADA                                            STRING
,AdFeeStartDate                                 STRING
,ADADate                                        STRING
,AdvertisingPercent                             STRING
,BaseFeePercent                                 STRING
,CerologistTrainingDate                         STRING
,CharityFee                                     STRING
,CharityFeeStartDate                            STRING
,FeeStartDate                                   STRING
,GeneralAdFund                                  STRING
,GeneralAdFundStartDate                         STRING
,HeartFirstRecipient                            STRING
,IncentivizedIncomeThreshold                    STRING
,LateFeePercent                                 STRING
,LayoutApprovedDate                             STRING
,LeaseTermInMonths                              STRING
,LocalAdUndAmt                                  STRING
,LocalAdFundStartDate                           STRING
,MinimumRoyalty                                 STRING
,MonthlyFeeStartDate                            STRING
,RealEstateStatus                               STRING
,SoftwareFeeTaxRate                             STRING
,SoftwareFee                                    STRING
,SoftwareFeeTotal                               STRING
,TechFee                                        STRING
,TechFeeStartDate                               STRING
,UnopenedClubFeeStartDate                       STRING
,UnopenedClubMonthlyFee                         STRING
,UnopenedStudioFee                              STRING
,UnopenedStudioFeeStartDate                     STRING
,Region                                         STRING
,FBCRegion                                      STRING
,BaseCamp                                       STRING
,PresaleStartDate                               STRING
,Presale                                        STRING
,LastSiteVisitDate                              STRING
,AFRequiredOnSiteTrainingDate                   STRING
,AFOnSiteTrainingCompleted                      STRING
,LocalAdTier                                    STRING
,FDDPrice                                       STRING
,TermDate                                       STRING
,PrimaryClubContactEmail                        STRING
,ExceptionOnMonthlyBilling                      STRING
,RequiredFASigning                              STRING
,FranchiseFeeType                               STRING
,InitialPayment                                 STRING
,BalanceDue                                     STRING
,BalanceCollected                               STRING
,TotalDevelopmentFee                            STRING
,RevenueRecognition                             STRING
,CharityAddendum                                STRING
,CharityDiscount                                STRING
,EmergingMarketDiscount                         STRING
,VeteranName                                    STRING
,AHBillingClub                                  STRING
,WebMaintenanceFee                              STRING
,TotalCollectedWithoutTaxes                     STRING
,RemitDate                                      STRING
,InstallmentAccounts                            STRING
,PIFAccounts                                    STRING
,ClubConnectPremiumService                      STRING
,TermDate_ClubHistory                           STRING
,CloseDate_ClubHistory                          STRING
,TermDate_StudioHistory                         STRING
,CloseDate_StudioHistory                        STRING
,VirtualMusicLicense                            STRING
,LiveStreamLocal                                STRING
,InStudioMusicLicense                           STRING
,PhysicalTherapy                                STRING
,Billing_Platform_Exception                     STRING
,DMA                                            STRING
,Provision_Project_Status                       STRING
,Refresh_Completed                              STRING
,KPSoftworks                                    STRING
,KPSoftworksStartDate                           STRING
,KPSoftworksEnd                                 STRING
,KPSoftworksEndDate                             STRING
,FileName                                                    STRING
,ModifiedBy                                                  INT
,ModifiedDate                                                 TIMESTAMP
,GoodToImport                                                INT
,Error                                                       STRING
)USING DELTA Location '/mnt/azuresynapsecoe/databricks/Delta/FRM/Location'

-- COMMAND ----------

DROP TABLE FRM_Location

-- COMMAND ----------

ALTER TABLE FRM_Location SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/mnt/azuresynapsecoe/databricks/Delta/FRM/Location', True)

-- COMMAND ----------


