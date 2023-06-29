# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID) as MHSDS_COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P') 
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---gender all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'Gender' as Demographic
 ,Der_Gender as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID) as MHSDS_COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---age band all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
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
 ,COUNT(DISTINCT UniqMHActEpisodeID) as MHSDS_Count
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---higher ethnicity all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
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
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 and (EthnicCategory is not null and EthnicCategory <> '-1')
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
 ---lower ethnicity all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
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
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 and (EthnicCategory is not null and EthnicCategory <> '-1')
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
 ---ccg/subicb all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'CCG' AS OrganisationBreakdown
 ,'' as DataSource
 ,CASE 
   WHEN IC_REC_CCG IS NULL THEN 'UNKNOWN'
   ELSE IC_REC_CCG END as OrgID
 ,CASE 
   WHEN CCG_NAME IS NULL THEN 'UNKNOWN'
   ELSE CCG_NAME END as OrgName
 ,COUNT(DISTINCT A.UniqMHActEpisodeID)
 FROM
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 IC_REC_CCG,
 CCG_NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---total ccg/subicb all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'CCG' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,SUM(metric_value)
 FROM (SELECT CASE 
   WHEN IC_REC_CCG IS NULL THEN 'UNKNOWN'
   ELSE IC_REC_CCG END as OrgID
 ,COUNT(DISTINCT A.UniqMHActEpisodeID) as metric_value
 FROM
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 CASE 
   WHEN IC_REC_CCG IS NULL THEN 'UNKNOWN'
   ELSE IC_REC_CCG END)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---stp/icb all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'STP' AS OrganisationBreakdown
 ,'' as DataSource
 ,CASE
 	WHEN STP_code is null then 'UNKNOWN'
 	ELSE STP_code END as OrgID
 ,CASE
 	WHEN STP_NAME is null then 'UNKNOWN' --
 	ELSE STP_NAME END as OrgName
 ,COUNT(DISTINCT A.UniqMHActEpisodeID)
 FROM
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 STP_CODE,
 STP_NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---total stp/icb all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'STP' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,SUM(metric_value)
 FROM (SELECT CASE 
   WHEN STP_CODE IS NULL THEN 'UNKNOWN'
   ELSE STP_CODE END as OrgID
 ,COUNT(DISTINCT A.UniqMHActEpisodeID) as metric_value
 FROM
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 CASE 
   WHEN STP_CODE IS NULL THEN 'UNKNOWN'
   ELSE STP_CODE END)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---deprivation all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'IMD' as Demographic
 ,CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END as DemographicBreakdown
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
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 STP_CODE,
 STP_NAME,
 CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---stp/icb and gender all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'Gender' as Demographic
 ,Der_Gender as DemographicBreakdown
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
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 STP_CODE,
 STP_NAME,
 Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---stp/icb and age band all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
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
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY
 STP_CODE,
 STP_NAME,
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
 WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
 WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
 WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
 WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
 ELSE 'Unknown'
 END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---stp/icb and higher ethnicity all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
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
 $db_output.detentions A
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---stp/icb and depriavtion all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'IMD' as Demographic
 ,CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID) as MHSDS_COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY 
 CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE as OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY 
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---provider all detentions count
 SELECT
 '$year' AS YEAR
 ,'All detentions' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE as OrganisationBreakdown
 ,'' as DataSource
 ,orgidProv as OrgID
 ,NAME as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 GROUP BY 
 ORG_TYPE_CODE
 ,orgidProv
 ,NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions on admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('A','P') ---detention on admission to hospital or criminal justice admission only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions on admission count 
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','P') ---detention on admission to hospital or criminal justice admission only
 GROUP BY 
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---provider detentions on admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,orgidProv as OrgID
 ,NAME as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','P') ---detention on admission to hospital or criminal justice admission only 
 GROUP BY 
 ORG_TYPE_CODE
 ,orgidProv
 ,NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions on admission under part 2 count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All detentions under Part 2' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions on admission under part 2 count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All detentions under Part 2' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital
 GROUP BY 
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions on admission under part 3 count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All detentions under Part 3' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('P') ---criminal justice admission only
 and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14') ---Part 3 LegalStatusCodes

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions on admission under part 3 count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,'All detentions under Part 3' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('P') ---criminal justice admission only 
 and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14') ---Part 3 LegalStatusCodes
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---section of the act national detentions on admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	WHEN LegalStatusCode = '07' then 'Section 35'
 	WHEN LegalStatusCode = '08' then 'Section 36'
 	WHEN LegalStatusCode = '09' then 'Section 37 with S41 restrictions'
 	WHEN LegalStatusCode = '10' then 'Section 37'
 	WHEN LegalStatusCode = '38' then 'Section 45A'
 	WHEN LegalStatusCode = '15' then 'Section 47 with S49 restrictions'
 	WHEN LegalStatusCode = '16' then 'Section 47'
 	WHEN LegalStatusCode = '17' then 'Section 48 with S49 restrictions'
 	WHEN LegalStatusCode = '18' then 'Section 48'
 	WHEN LegalStatusCode in ('12','13','14')  then 'Detentions under other sections (38,44 and 46)'
 	ELSE 'Detentions under other acts'
 	END as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('A','P') ---detention on admission to hospital or criminal justice admission only 
 GROUP BY
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	WHEN LegalStatusCode = '07' then 'Section 35'
 	WHEN LegalStatusCode = '08' then 'Section 36'
 	WHEN LegalStatusCode = '09' then 'Section 37 with S41 restrictions'
 	WHEN LegalStatusCode = '10' then 'Section 37'
 	WHEN LegalStatusCode = '38' then 'Section 45A'
 	WHEN LegalStatusCode = '15' then 'Section 47 with S49 restrictions'
 	WHEN LegalStatusCode = '16' then 'Section 47'
 	WHEN LegalStatusCode = '17' then 'Section 48 with S49 restrictions'
 	WHEN LegalStatusCode = '18' then 'Section 48'
 	WHEN LegalStatusCode in ('12','13','14')  then 'Detentions under other sections (38,44 and 46)'
 	ELSE 'Detentions under other acts'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---section of the act organisation type detentions on admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions on admission to hospital' as Measure
 ,CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	WHEN LegalStatusCode = '07' then 'Section 35'
 	WHEN LegalStatusCode = '08' then 'Section 36'
 	WHEN LegalStatusCode = '09' then 'Section 37 with S41 restrictions'
 	WHEN LegalStatusCode = '10' then 'Section 37'
 	WHEN LegalStatusCode = '38' then 'Section 45A'
 	WHEN LegalStatusCode = '15' then 'Section 47 with S49 restrictions'
 	WHEN LegalStatusCode = '16' then 'Section 47'
 	WHEN LegalStatusCode = '17' then 'Section 48 with S49 restrictions'
 	WHEN LegalStatusCode = '18' then 'Section 48'
 	WHEN LegalStatusCode in ('12','13','14')  then 'Detentions under other sections (38,44 and 46)'
 	ELSE 'Detentions under other acts'
 	END as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','P') ---detention on admission to hospital or criminal justice admission only
 GROUP BY 
 ORG_TYPE_CODE,
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	WHEN LegalStatusCode = '07' then 'Section 35'
 	WHEN LegalStatusCode = '08' then 'Section 36'
 	WHEN LegalStatusCode = '09' then 'Section 37 with S41 restrictions'
 	WHEN LegalStatusCode = '10' then 'Section 37'
 	WHEN LegalStatusCode = '38' then 'Section 45A'
 	WHEN LegalStatusCode = '15' then 'Section 47 with S49 restrictions'
 	WHEN LegalStatusCode = '16' then 'Section 47'
 	WHEN LegalStatusCode = '17' then 'Section 48 with S49 restrictions'
 	WHEN LegalStatusCode = '18' then 'Section 48'
 	WHEN LegalStatusCode in ('12','13','14')  then 'Detentions under other sections (38,44 and 46)'
 	ELSE 'Detentions under other acts'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions following admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions following admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B') ---detentions subsequent to admission only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions following admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions following admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('B') ---detentions subsequent to admission only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---provider detentions following admission count
 SELECT
 '$year' AS YEAR
 ,'Detentions following admission to hospital' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,orgidProv as OrgID
 ,NAME OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B') ---detentions subsequent to admission only
 GROUP BY 
 ORG_TYPE_CODE
 ,orgidProv
 ,NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status national detentions following admission to hospital count
 SELECT
 '$year' AS YEAR
 ,'Detentions following admission to hospital' as Measure
 ,CASE
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '02' then 'Section 5(2) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '03' then 'Section 5(2) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '02' then 'Section 5(4) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '03' then 'Section 5(4) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '02' then 'Section 4 to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '03' then 'Section 4 to 3'  
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN LegalStatusCode = '02' then 'Other section to 2'
 	WHEN LegalStatusCode = '03' then 'Other section to 3'
 	END as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B') ---detentions subsequent to admission only
 GROUP BY
 CASE
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '02' then 'Section 5(2) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '03' then 'Section 5(2) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '02' then 'Section 5(4) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '03' then 'Section 5(4) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '02' then 'Section 4 to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '03' then 'Section 4 to 3'  
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN LegalStatusCode = '02' then 'Other section to 2'
 	WHEN LegalStatusCode = '03' then 'Other section to 3'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status orgaisation type detentions following admission to hospital count
 SELECT
 '$year' AS YEAR
 ,'Detentions following admission to hospital' as Measure
 ,CASE
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '02' then 'Section 5(2) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '03' then 'Section 5(2) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '02' then 'Section 5(4) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '03' then 'Section 5(4) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '02' then 'Section 4 to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '03' then 'Section 4 to 3'  
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN LegalStatusCode = '02' then 'Other section to 2'
 	WHEN LegalStatusCode = '03' then 'Other section to 3'
 	END as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('B') ---detentions subsequent to admission only
 GROUP BY
 ORG_TYPE_CODE,
 CASE
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN (PrevLegalStatus is null or PrevLegalStatus in ('1','98','99')) and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '02' then 'Section 5(2) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') and LegalStatusCode = '03' then 'Section 5(2) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '02' then 'Section 5(4) to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') and LegalStatusCode = '03' then 'Section 5(4) to 3'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '02' then 'Section 4 to 2'  
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('04') and LegalStatusCode = '03' then 'Section 4 to 3'  
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '02' then 'Informal to section 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass and LegalStatusCode = '03' then 'Informal to section 3' 
 	WHEN LegalStatusCode = '02' then 'Other section to 2'
 	WHEN LegalStatusCode = '03' then 'Other section to 3'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions following use of place of safety order count
 SELECT
 '$year' AS YEAR
 ,'Detentions following use of Place of Safety Order' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('C') ---detention following place of safety order only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions following use of place of safety order only
 SELECT
 '$year' AS YEAR
 ,'Detentions following use of Place of Safety Order' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' AS DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('C') ---detention following place of safety order only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status national detentions following use of place of safety order count
 SELECT
 '$year' AS YEAR
 ,'Detentions following use of Place of Safety Order' as Measure
 ,CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	END  as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('C') ---detention following place of safety order only
 GROUP BY
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	END  

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status organisation type detentions following use of place of safety order count
 SELECT
 '$year' AS YEAR
 ,'Detentions following use of Place of Safety Order' as Measure
 ,CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	END  as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('C') ---detention following place of safety order only
 GROUP BY
 ORG_TYPE_CODE
 ,CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national detentions following revocation of community treatment order or conditional discharge count
 SELECT
 '$year' AS YEAR
 ,'Detentions following revocation of CTO' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('D') ---detentions revocation of cto or coniditonal discharge only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type detentions following revocation of community treatment order or conditional discharge count
 SELECT
 '$year' AS YEAR
 ,'Detentions following revocation of CTO' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT UniqMHActEpisodeID)
 FROM
 $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('D') ---detentions revocation of cto or coniditonal discharge only
 GROUP BY ORG_TYPE_CODE