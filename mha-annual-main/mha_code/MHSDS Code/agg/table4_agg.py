# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national uses of section 2 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
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
 MHA_Logic_Cat_full in ('A','B','C','L') ---detention on admission to hospital, subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '02' ---legalstatuscode is section 2

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type uses of section 2 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
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
 MHA_Logic_Cat_full in ('A','B','C','L') ---detention on admission to hospital, subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '02' ---legalstatuscode is section 2
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national section 2 on admission count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
 ,'Section 2 on admission' as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital only
 and LegalStatusCode = '02' ---legalstatuscode is section 2

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type section 2 on admission count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
 ,'Section 2 on admission' as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital only
 and LegalStatusCode = '02' ---legalstatuscode is section 2
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status during detention national uses of section 2 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
 ,CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 2 following admission (informal to s2)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 2' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 2 following admission (informal to s2)' 
 	ELSE 'Changes from other sections to Section 2'
 	END as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('B','C') ---detetions subsequent to admission or following place of safety order
 and LegalStatusCode = '02' ---legalstatuscode is section 2
 GROUP BY
 CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 2 following admission (informal to s2)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 2' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 2 following admission (informal to s2)' 
 	ELSE 'Changes from other sections to Section 2'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status during detention organisation type uses of section 2 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 2' as Measure
 ,CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 2 following admission (informal to s2)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 2' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 2 following admission (informal to s2)' 
 	ELSE 'Changes from other sections to Section 2'
 	END as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('B','C') ---detetions subsequent to admission or following place of safety order
 and LegalStatusCode = '02' ---legalstatuscode is section 2
 GROUP BY
 CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 2 following admission (informal to s2)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 2' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 2' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '02' THEN 'Section 136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '02' THEN 'Section 135 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 2' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 2 following admission (informal to s2)' 
 	ELSE 'Changes from other sections to Section 2'
 	END, 
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national uses of section 3 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
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
 MHA_Logic_Cat_full in ('A','B','C','L') ---detention on admission to hospital, subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '03' ---legalstatuscode is section 3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type uses of section 3 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
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
 MHA_Logic_Cat_full in ('A','B','C','L') ---detention on admission to hospital, subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '03' ---legalstatuscode is section 3
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---national section 3 on admission count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
 ,'Section 3 on admission' as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital only
 and LegalStatusCode = '03' ---legalstatuscode is section 3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---organisation type section 3 on admission count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
 ,'Section 3 on admission' as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('A') ---detention on admission to hospital only
 and LegalStatusCode = '03' ---legalstatuscode is section 3
 GROUP BY ORG_TYPE_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status during detention national uses of section 3 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
 ,CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 3 following admission (informal to s3)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 3' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('02') then 'Section 2 to 3' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 3 following admission (informal to s3)' 
 	ELSE 'Changes from other section to 3'
 	END as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('B','C','L') ---detention subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '03' ---legalstatuscode is section 3
 GROUP BY
 CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 3 following admission (informal to s3)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 3' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('02') then 'Section 2 to 3' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 3 following admission (informal to s3)' 
 	ELSE 'Changes from other section to 3'
 	END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_unformatted
 ---changes in legal status during detention organisation type uses of section 3 count
 SELECT
 '$year' AS YEAR
 ,'Uses of section 3' as Measure
 ,CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 3 following admission (informal to s3)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 3' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('02') then 'Section 2 to 3' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 3 following admission (informal to s3)' 
 	ELSE 'Changes from other section to 3'
 	END as MeasureSubcategory
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
 MHA_Logic_Cat_full in ('B','C','L') ---detention subsequent to admission, following place of safety order or section 3 subsequent to section 2 only
 and LegalStatusCode = '03' ---legalstatuscode is section 3
 GROUP BY
 ORG_TYPE_CODE,
 CASE
 	WHEN PrevLegalStatus is null or PrevLegalStatus in ('01','98') OR (PrevMHAEndDate < StartDateMHActLegalStatusClass AND PrevLegalStatus IN ('02','03')) then 'Section 3 following admission (informal to s3)' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('05') then 'Section 5(2) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('06') then 'Section 5(4) to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('19') then 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('20') then 'Section 136 to 3' 
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '20' and LegalStatusCode = '03' THEN 'Section 136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus = '19' and LegalStatusCode = '03' THEN 'Section 135 to 3'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('04') then 'Section 4 to 3' 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-1) and PrevMHAEndDate and PrevLegalStatus in ('02') then 'Section 2 to 3' 
 	WHEN PrevMHAEndDate < StartDateMHActLegalStatusClass then 'Section 3 following admission (informal to s3)' 
 	ELSE 'Changes from other section to 3'
 	END