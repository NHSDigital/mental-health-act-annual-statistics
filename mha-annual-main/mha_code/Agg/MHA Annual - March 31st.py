# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to the Act on 31st March' as Measure --MHS08
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs08_prep

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People detained in hospital on 31st March' as Measure --MHS09
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs09_prep

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to Community Treatment Orders (CTOs) on 31st March' as Measure --MHS10
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,'All submissions' AS OrganisationBreakdown
 ,'' as DataSource
 ,'All prov' as OrgID
 ,'All providers' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs10_prep

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to the Act on 31st March' as Measure --MHS08
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,"All prov" as OrgID
 ,"All providers" as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs08prov_prep
 GROUP BY ProvType

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People detained in hospital on 31st March' as Measure --MHS09
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,"All prov" as OrgID
 ,"All providers" as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs09prov_prep
 GROUP BY ProvType

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to Community Treatment Orders (CTOs) on 31st March' as Measure --MHS10
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,"All prov" as OrgID
 ,"All providers" as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs10prov_prep
 GROUP BY ProvType

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to the Act on 31st March' as Measure --MHS08
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,OrgIDProv as OrgID
 ,'' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs08prov_prep
 GROUP BY ProvType, OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People detained in hospital on 31st March' as Measure --MHS09
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,OrgIDProv as OrgID
 ,'' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs09prov_prep
 GROUP BY ProvType, OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted 
 
 SELECT
 '$year' AS YEAR
 ,'People subject to Community Treatment Orders (CTOs) on 31st March' as Measure --MHS10
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ProvType AS OrganisationBreakdown
 ,'' as DataSource
 ,OrgIDProv as OrgID
 ,'' as OrgName
 ,COUNT(DISTINCT Person_ID) as MHSDS_COUNT
 FROM
 $db_output.mha_mhs10prov_prep
 GROUP BY ProvType, OrgIDProv