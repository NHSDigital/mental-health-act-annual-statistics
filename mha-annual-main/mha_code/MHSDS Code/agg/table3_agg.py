# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national uses of community treatment orders count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---higher ethnicity uses of community treatment orders count
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
 ---lower ethnicity uses of community treatment orders count
 SELECT
 '$year' AS YEAR
 ,'Uses of CTOs' as Measure
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
 ,COUNT(DISTINCT A.CTO_UniqMHActEpisodeID)
 FROM 
 $db_output.cto a
 where 
 (EthnicCategory is not null and EthnicCategory <> '-1')
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
 ---age band uses of community treatment orders count
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
 ---gender uses of community treatment orders count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type uses of community treatment orders count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---provider uses of community treatment orders count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national revocations of community treatment orders count
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
 CommTreatOrdEndReason = '02' ---community treatment order revoked

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type revocations of community treatment orders count
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
 CommTreatOrdEndReason = '02' ---community treatment order revoked
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national discharges from community treatment orders count
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
 CommTreatOrdEndReason = '01' ---patient discharged

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type from community treatment orders count
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
 CommTreatOrdEndReason = '01' ---patient discharged
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---national community treatment order recalls to hospital
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
 INNER JOIN $db_output.mhs405_latest B ON A.CTO_UniqMHActEpisodeID = B.UniqMHActEpisodeID ---inner join with community treament order recalls in the financial year

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---organisation type community treatment order recalls to hospital
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
 INNER JOIN $db_output.mhs405_latest B ON A.CTO_UniqMHActEpisodeID = B.UniqMHActEpisodeID ---inner join with community treament order recalls in the financial year
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---changes in legal status during detention national uses of community treatment order count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 ---changes in legal status during detention organisation type uses of community treatment order count
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