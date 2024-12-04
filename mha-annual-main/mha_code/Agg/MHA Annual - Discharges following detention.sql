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
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT 
'$year' AS YEAR
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
GROUP BY
ORG_TYPE_CODE

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT 
'$year' AS YEAR
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'All' as Demographic
,'All' as DemographicBreakdown
,ORG_TYPE_CODE AS OrganisationBreakdown
,'' as DataSource
,OrgIDProv as OrgID
,Name as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
GROUP BY
ORG_TYPE_CODE
,ORGIDPROV
,NAME

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT 
'$year' AS YEAR
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'Gender' as Demographic
,Der_Gender as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
-- AND Gender IN ('1','01','2','02')
GROUP BY Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT 
'$year' AS YEAR
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'Age' as Demographic
,CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
ELSE 'Unknown'
END as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
-- AND AgeRepPeriodEnd is not null
GROUP BY 
CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
ELSE 'Unknown'
END

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT 
'$year' AS YEAR
,'Discharges following detention' as Measure
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
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
and EthnicCategory is not null
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
,'Discharges following detention' as Measure
,'All' as MeasureSubcategory
,'Ethnicity' as Demographic
,CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
    WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
    WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
    WHEN EthnicCategory = 'E' THEN 'White and Black African'
    WHEN EthnicCategory = 'F' THEN 'White and Asian'
    WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
    WHEN EthnicCategory = 'H' THEN 'Indian'
    WHEN EthnicCategory = 'J' THEN 'Pakistani'
    WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
    WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
    WHEN EthnicCategory = 'M' THEN 'Caribbean'
    WHEN EthnicCategory = 'N' THEN 'African'
    WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
    WHEN EthnicCategory = 'R' THEN 'Chinese'
    WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
    WHEN EthnicCategory = 'Z' THEN 'Not Stated'
    WHEN EthnicCategory = '99' THEN 'Not Known'
    ELSE 'Unknown'
    END as DemographicBreakdown
,'All submissions' AS OrganisationBreakdown
,'' as DataSource
,'All prov' as OrgID
,'All providers' as OrgName
,COUNT(DISTINCT UniqHospProvSpellID)
FROM
$db_output.detentions
WHERE
DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))
AND EthnicCategory is not null
AND AgeRepPeriodEnd is not null
AND Der_Gender in ('1','2')
GROUP BY 
CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
    WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
    WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
    WHEN EthnicCategory = 'E' THEN 'White and Black African'
    WHEN EthnicCategory = 'F' THEN 'White and Asian'
    WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
    WHEN EthnicCategory = 'H' THEN 'Indian'
    WHEN EthnicCategory = 'J' THEN 'Pakistani'
    WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
    WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
    WHEN EthnicCategory = 'M' THEN 'Caribbean'
    WHEN EthnicCategory = 'N' THEN 'African'
    WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
    WHEN EthnicCategory = 'R' THEN 'Chinese'
    WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
    WHEN EthnicCategory = 'Z' THEN 'Not Stated'
    WHEN EthnicCategory = '99' THEN 'Not Known'
    ELSE 'Unknown'
    END