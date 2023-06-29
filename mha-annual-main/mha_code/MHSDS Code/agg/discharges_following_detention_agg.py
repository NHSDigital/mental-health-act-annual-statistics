# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---provider discharges following detention count
 SELECT 
 '$year' AS YEAR
 ,'Discharges following detention' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,OrgIDProv as OrgID
 ,NAME as OrgName
 ,COUNT(DISTINCT UniqHospProvSpellID)
 FROM
 $db_output.detentions
 WHERE
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY ORG_TYPE_CODE, OrgIDProv, NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---gender discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---age band discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
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
 ---higher ethnicity discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 and EthnicCategory is not null
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
 ---lower ethnicity discharges following detention count
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
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharge in financial year
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell ---end of detention before or on date of discharge
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell ---end of detention after or on date of admission
 AND MHA_Logic_Cat_Full IN ('A','B','C','D','P')
 ---detention on admission to hospital, subsequent to admission, following place of safety order, following revocation of cto or coniditonal discharge or criminal justice admission only
 and EthnicCategory is not null
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