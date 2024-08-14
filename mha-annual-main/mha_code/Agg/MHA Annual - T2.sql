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
,'Short term orders' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E','F')

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E','F')
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All place of safety orders' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E')

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All place of safety orders' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E')
GROUP BY
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,CASE
	WHEN LegalStatusCode = '19' then 'Section 135'
	WHEN LegalStatusCode = '20' then 'Section 136'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E')
GROUP BY
CASE
	WHEN LegalStatusCode = '19' then 'Section 135'
	WHEN LegalStatusCode = '20' then 'Section 136'
	END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,CASE
	WHEN LegalStatusCode = '19' then 'Section 135'
	WHEN LegalStatusCode = '20' then 'Section 136'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E')
GROUP BY
ORG_TYPE_CODE,
CASE
	WHEN LegalStatusCode = '19' then 'Section 135'
	WHEN LegalStatusCode = '20' then 'Section 136'
	END


-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'Section 136' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,orgidProv as OrgID
,NAME as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
GROUP BY
ORG_TYPE_CODE,
orgidProv,
NAME

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'Section 136' as MeasureSubcategory
,'Gender' as Demographic
,Der_Gender DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
-- and Gender in ('1','01','2','02')
GROUP BY Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'Section 136' as MeasureSubcategory
,'Ethnicity' as Demographic
,CASE
WHEN EthnicCategory IN ('A','B','C') THEN 'White'
WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
ELSE 'Unknown'
END as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
and (EthnicCategory is not null and EthnicCategory <>'-1')
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
,'Short term orders' as Measure
,'Section 136' as MeasureSubcategory
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
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
and (EthnicCategory is not null and EthnicCategory <>'-1')
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
,'Short term orders' as Measure
,'Section 136' as MeasureSubcategory
,'Age' as Demographic
,CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
END as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
-- and AgeRepPeriodEnd is not null
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
,'Short term orders' as Measure
,'All uses of section 5' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('F') and LegalStatusCode in ('05','06')

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All uses of section 5' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('F') and LegalStatusCode in ('05','06')
GROUP BY 
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,CASE
	WHEN LegalStatusCode = '04' then 'Section 4'
	WHEN LegalStatusCode = '05' then 'Section 5(2)'
	WHEN LegalStatusCode = '06' then 'Section 5(4)'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('F')
GROUP BY
CASE
	WHEN LegalStatusCode = '04' then 'Section 4'
	WHEN LegalStatusCode = '05' then 'Section 5(2)'
	WHEN LegalStatusCode = '06' then 'Section 5(4)'
	END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted 

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,CASE
	WHEN LegalStatusCode = '04' then 'Section 4'
	WHEN LegalStatusCode = '05' then 'Section 5(2)'
	WHEN LegalStatusCode = '06' then 'Section 5(4)'
	END as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('F')
GROUP BY
ORG_TYPE_CODE,
CASE
	WHEN LegalStatusCode = '04' then 'Section 4'
	WHEN LegalStatusCode = '05' then 'Section 5(2)'
	WHEN LegalStatusCode = '06' then 'Section 5(4)'
	END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'STP' AS OrganisationBreakdown
,'' as DataSource
,CASE
	WHEN STP_code is null then 'UNKNOWN'
	ELSE STP_code END as OrgID
,CASE
	WHEN STP_NAME is null then 'UNKNOWN'
	ELSE STP_NAME END as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E','F')
GROUP BY
STP_CODE,
STP_NAME

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
'$year' AS YEAR
,'Short term orders' as Measure
,'All' as MeasureSubcategory
,'Ethnicity' as Demographic
,CASE
WHEN EthnicCategory IN ('A','B','C') THEN 'White'
WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
ELSE 'Unknown'
END as DemographicBreakdown
,'STP' AS OrganisationBreakdown
,'' as DataSource
,CASE
	WHEN STP_code is null then 'UNKNOWN'
	ELSE STP_code END as OrgID
,CASE
	WHEN STP_NAME is null then 'UNKNOWN'
	ELSE STP_NAME END as OrgName
,COUNT(DISTINCT A.UniqMHActEpisodeID)
FROM
$db_output.short_term_orders A
WHERE
MHA_Logic_Cat_full in ('E','F')
GROUP BY
STP_CODE,
STP_NAME,
CASE
WHEN EthnicCategory IN ('A','B','C') THEN 'White'
WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
ELSE 'Unknown'
END