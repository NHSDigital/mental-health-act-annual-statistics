# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national trasnfers on section count
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
 MHA_Logic_Cat_full in ('H') ---transfer on section only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type trasnfers on section count
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
 MHA_Logic_Cat_full in ('H') ---transfer on section only
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---provider trasnfers on section count
 SELECT
 '$year' AS YEAR
 ,'Transfers on section' as Measure
 ,'All' as MeasureSubcategory
 ,'All' as Demographic
 ,'All' as DemographicBreakdown
 ,ORG_TYPE_CODE AS OrganisationBreakdown
 ,'' as DataSource
 ,OrgIDProv as OrgID
 ,NAME as OrgName
 ,COUNT(DISTINCT A.UniqMHActEpisodeID)
 FROM
 $db_output.detentions a
 WHERE
 MHA_Logic_Cat_full in ('H') ---transfer on section only
 GROUP BY 
 ORG_TYPE_CODE, OrgIDProv, NAME