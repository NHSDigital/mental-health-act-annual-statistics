-- Databricks notebook source
-- # %py
-- # db_source  = dbutils.widgets.get("db_source")
-- # rp_enddate = dbutils.widgets.get("rp_enddate")
-- # rp_startdate = dbutils.widgets.get("rp_startdate")
-- # year = dbutils.widgets.get("year")
-- # personal_db = dbutils.widgets.get("personal_db")

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
'$year' AS YEAR
,'Transfers on section' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.detentions a
WHERE
MHA_Logic_Cat_full in ('H')

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
'$year' AS YEAR
,'Transfers on section' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.detentions a
WHERE
MHA_Logic_Cat_full in ('H')
GROUP BY 
ORG_TYPE_cODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
'$year' AS YEAR
,'Transfers on section' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,OrgIDProv as OrgID
,Name as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.detentions a
WHERE
MHA_Logic_Cat_full in ('H')
GROUP BY 
ORG_TYPE_CODE,
ORGIDPROV,
NAME