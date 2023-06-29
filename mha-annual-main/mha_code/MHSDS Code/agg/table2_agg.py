# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national all short term orders count
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
 MHA_Logic_Cat_full in ('E','F') ---place of safety order or other short term holding order only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type all short term orders count
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
 MHA_Logic_Cat_full in ('E','F') ---place of safety order or other short term holding order only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national place of safety orders count
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
 MHA_Logic_Cat_full in ('E') ---place of safety order only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type place of safety orders count
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
 MHA_Logic_Cat_full in ('E') ---place of safety order only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section type national short term orders count
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
 MHA_Logic_Cat_full in ('E') ---place of safety order only
 GROUP BY
 CASE
 	WHEN LegalStatusCode = '19' then 'Section 135'
 	WHEN LegalStatusCode = '20' then 'Section 136'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section type organisation type short term orders count
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
 MHA_Logic_Cat_full in ('E') ---place of safety order only
 GROUP BY
 ORG_TYPE_CODE,
 CASE
 	WHEN LegalStatusCode = '19' then 'Section 135'
 	WHEN LegalStatusCode = '20' then 'Section 136'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 136 provider short term orders count
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
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20' ---place of safety order only where legalstatuscode is section 136
 GROUP BY
 ORG_TYPE_CODE,
 orgidProv,
 NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 136 gender short term orders count
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
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20' ---place of safety order only where legalstatuscode is section 136
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 136 higher ethnicity short term orders count
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
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20' ---place of safety order only where legalstatuscode is section 136
 and (EthnicCategory is not null and EthnicCategory <>'-1')
 AND AgeRepPeriodEnd is not null
 AND Der_Gender in ('1','2')
 ---as per standardisation methodology, count at ethnicity needs to be where all other demographics are known or have values that can be standardised (such as Male and Female only)
 GROUP BY
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 ELSE 'Unknown'
 END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 136 lower ethnicity short term orders count
 SELECT
 '$year' AS YEAR
 ,'Short term orders' as Measure
 ,'Section 136' as MeasureSubcategory
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
 ,COUNT(DISTINCT A.UniqMHActEpisodeID)
 FROM
 $db_output.short_term_orders A
 WHERE
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20' ---place of safety order only where legalstatuscode is section 136
 and (EthnicCategory is not null and EthnicCategory <>'-1')
 AND AgeRepPeriodEnd is not null
 AND Der_Gender in ('1','2')
 ---as per standardisation methodology, count at ethnicity needs to be where all other demographics are known or have values that can be standardised (such as Male and Female only)
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 136 age band short term orders count
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
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20' ---place of safety order only where legalstatuscode is section 136
 GROUP BY
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
 WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
 WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
 WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
 WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
 END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 5 national short term orders count
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
 MHA_Logic_Cat_full in ('F') and LegalStatusCode in ('05','06') ---other short term holding order only where legalstatuscode is section5(2) or section 5(4)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section 5 organisation type short term orders count
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
 MHA_Logic_Cat_full in ('F') and LegalStatusCode in ('05','06') ---other short term holding order only where legalstatuscode is section5(2) or section 5(4)
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section type national short term orders count
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
 MHA_Logic_Cat_full in ('F') ---other short term holding order only
 GROUP BY
 CASE
 	WHEN LegalStatusCode = '04' then 'Section 4'
 	WHEN LegalStatusCode = '05' then 'Section 5(2)'
 	WHEN LegalStatusCode = '06' then 'Section 5(4)'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---section type organisation type short term orders count
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
 MHA_Logic_Cat_full in ('F') ---other short term holding order only
 GROUP BY
 ORG_TYPE_CODE,
 CASE
 	WHEN LegalStatusCode = '04' then 'Section 4'
 	WHEN LegalStatusCode = '05' then 'Section 5(2)'
 	WHEN LegalStatusCode = '06' then 'Section 5(4)'
 	END