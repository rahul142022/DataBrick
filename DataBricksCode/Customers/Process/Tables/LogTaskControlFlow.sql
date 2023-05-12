-- Databricks notebook source
CREATE TABLE IF NOT EXISTS LogTaskControlFlow
(
	LogTaskControlFlowKey			BIGINT GENERATED ALWAYS AS IDENTITY (start WITH 0  INCREMENT by 1)
	,StartTime						TIMESTAMP 
	,DataFactory					STRING
	,Pipeline						    STRING
	,StartingActivity				STRING
	,EndingActivity					STRING
	,SchemaName						STRING
	,Status								STRING
	,Error 								STRING
)USING DELTA Location '/mnt/Contatablobstorage/DeltaTable/LogTaskControlFlow'



-- COMMAND ----------


