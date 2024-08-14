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
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto A

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'Ethnicity' as Demographic
,CASE
WHEN EthnicCategory IN ('A','B','C') THEN 'White'
WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
ELSE 'Unknown'
END  as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
(EthnicCategory is not null and EthnicCategory <> '-1')
AND AgeRepPeriodEnd is not null
AND Der_Gender in ('1','2')
GROUP BY
CASE
WHEN EthnicCategory IN ('A','B','C') THEN 'White'
WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
ELSE 'Unknown'
END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'Ethnicity' as Demographic
,CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
    WHEN EthnicCategory = 'C' THEN 'Any Other White background'
    WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
    WHEN EthnicCategory = 'E' THEN 'White and Black African'
    WHEN EthnicCategory = 'F' THEN 'White and Asian'
    WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
    WHEN EthnicCategory = 'H' THEN 'Indian'
    WHEN EthnicCategory = 'J' THEN 'Pakistani'
    WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
    WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
    WHEN EthnicCategory = 'M' THEN 'Caribbean'
    WHEN EthnicCategory = 'N' THEN 'African'
    WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
    WHEN EthnicCategory = 'R' THEN 'Chinese'
    WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
    WHEN EthnicCategory = 'Z' THEN 'Not Stated'
    WHEN EthnicCategory = '99' THEN 'Not Known'
    ELSE 'Unknown'
    END as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
(EthnicCategory is not null and EthnicCategory <> '-1')
AND AgeRepPeriodEnd is not null
AND Der_Gender in ('1','2')
GROUP BY
CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
    WHEN EthnicCategory = 'C' THEN 'Any Other White background'
    WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
    WHEN EthnicCategory = 'E' THEN 'White and Black African'
    WHEN EthnicCategory = 'F' THEN 'White and Asian'
    WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
    WHEN EthnicCategory = 'H' THEN 'Indian'
    WHEN EthnicCategory = 'J' THEN 'Pakistani'
    WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
    WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
    WHEN EthnicCategory = 'M' THEN 'Caribbean'
    WHEN EthnicCategory = 'N' THEN 'African'
    WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
    WHEN EthnicCategory = 'R' THEN 'Chinese'
    WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
    WHEN EthnicCategory = 'Z' THEN 'Not Stated'
    WHEN EthnicCategory = '99' THEN 'Not Known'
    ELSE 'Unknown'
    END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'Age' as Demographic
,CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
END  as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
-- where 
-- AgeRepPeriodEnd is not null 
GROUP BY
CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'Gender' as Demographic
,Der_Gender as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
GROUP BY Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DEmographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,orgidProv as OrgID
,NAME as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
GROUP BY 
ORG_TYPE_CODE,
orgidProv,
NAME

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Revocations of CTO' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
CommTreatOrdEndReason = '02' 


-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Revocations of CTO' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
CommTreatOrdEndReason = '02' 
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Discharges from CTO' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
CommTreatOrdEndReason = '01' 

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Discharges from CTO' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
where 
CommTreatOrdEndReason = '01' 
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

-- CREATE OR REPLACE TEMPORARY VIEW MHS405CTO_RECALL_LATEST AS
-- (
-- SELECT			 B.MHS405UniqID
-- 				,B.UniqMonthID
-- 				,B.OrgIDProv
-- 				,B.Person_ID
-- 				,B.UniqMHActEpisodeID
-- 				,B.StartDateCommTreatOrdRecall
-- 				,B.EndDateCommTreatOrdRecall
-- 				,B.RecordStartDate
-- 				,B.RecordNumber

-- FROM			$db_source.MHS405CommTreatOrderRecall
-- 					AS B
-- 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'
-- )

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'CTO recalls to hospital' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM
$db_output.cto A
INNER JOIN $db_output.mhs405_latest B ON A.CTO_UniqMHActEpisodeID = B.UniqMHActEpisodeID

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'CTO recalls to hospital' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM
$db_output.cto A
INNER JOIN $db_output.mhs405_latest B ON A.CTO_UniqMHActEpisodeID = B.UniqMHActEpisodeID
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,CASE
	WHEN A.LegalStatusCode = '03' then 'Section 3 to CTO'
	WHEN A.LegalStatusCode in ('09','10') then 'Section 37 to CTO'
	WHEN A.LegalStatusCode in ('15','16') then 'Section 47 to CTO'
	WHEN A.LegalStatusCode in ('17','18') then 'Section 48 to CTO'
	WHEN A.LegalStatusCode is null or A.LegalStatusCode in ('01','98') then 'Informal to CTO'
	ELSE  'Other sections to CTO'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
GROUP BY
CASE
	WHEN A.LegalStatusCode = '03' then 'Section 3 to CTO'
	WHEN A.LegalStatusCode in ('09','10') then 'Section 37 to CTO'
	WHEN A.LegalStatusCode in ('15','16') then 'Section 47 to CTO'
	WHEN A.LegalStatusCode in ('17','18') then 'Section 48 to CTO'
	WHEN A.LegalStatusCode is null or A.LegalStatusCode in ('01','98') then 'Informal to CTO'
	ELSE  'Other sections to CTO'
	END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Uses of CTOs' as Measure
,CASE
	WHEN A.LegalStatusCode = '03' then 'Section 3 to CTO'
	WHEN A.LegalStatusCode in ('09','10') then 'Section 37 to CTO'
	WHEN A.LegalStatusCode in ('15','16') then 'Section 47 to CTO'
	WHEN A.LegalStatusCode in ('17','18') then 'Section 48 to CTO'
	WHEN A.LegalStatusCode is null or A.LegalStatusCode in ('01','98') then 'Informal to CTO'
	ELSE  'Other sections to CTO'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
FROM 
$db_output.cto a
GROUP BY
ORG_TYPE_CODE,
CASE
	WHEN A.LegalStatusCode = '03' then 'Section 3 to CTO'
	WHEN A.LegalStatusCode in ('09','10') then 'Section 37 to CTO'
	WHEN A.LegalStatusCode in ('15','16') then 'Section 47 to CTO'
	WHEN A.LegalStatusCode in ('17','18') then 'Section 48 to CTO'
	WHEN A.LegalStatusCode is null or A.LegalStatusCode in ('01','98') then 'Informal to CTO'
	ELSE  'Other sections to CTO'
	END