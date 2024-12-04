-- Databricks notebook source
--create widget text year default "2020-21"; 
--create widget text rp_enddate default "2021-03-31"; 
--create widget text rp_startdate default "2020-04-01";
--create widget text db_source default "mhsds_database";
--create widget text personal_db default "chinyere_agu_100064";

-- COMMAND ----------


INSERT INTO $db_output.mha_unformatted

SELECT
'$year' as YEAR
,'People subject to repeat detentions' as MEASURE
,CASE 
  WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
  ELSE CAST(DETENTION_COUNT AS string) 
  END AS MEASURESUBCATEGORY
,'All' as DEMOGRAPHIC
,'All' as DEMOGRAPHICBREAKDOWN
,'All submissions' as ORGANISATIONBREAKDOWN
,'MHSDS' AS DATASOURCE
,'All prov' as ORGCODE
,'All providers' as ORGNAME
,COUNT(Person_ID) AS  MHSDS_COUNT
FROM
      (SELECT
      B.Person_ID,
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender,
      COUNT(DISTINCT B.UniqMHActEpisodeID) AS DETENTION_COUNT
      FROM
      $db_output.detentions AS B 
      LEFT JOIN $db_output.mhs001_latest C ON B.Person_ID = C.PERSON_ID
      WHERE
      MHA_Logic_Cat_full in ('A','B','C','D','P')
      GROUP BY 
      B.PERSON_ID, 
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender) AS A
GROUP BY 
CASE WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
ELSE CAST(DETENTION_COUNT AS string) END 
order by 1,2

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted -- CA changed the ethnic descriptions here to match those in the MHA_CSV_Full table so that it's pulling out accurately in the Master notebook

SELECT
DISTINCT
'$year' as YEAR
,'People subject to repeat detentions' as MEASURE
,CASE 
  WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
  ELSE CAST(DETENTION_COUNT AS string) 
  END AS MEASURESUBCATEGORY
,'Ethnicity' as DEMOGRAPHIC
,CASE
  WHEN EthnicCategory IN ('A','B','C') THEN 'White'
  WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
  WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
  WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
  WHEN EthnicCategory IN ('R','S','Z','99') THEN 'Other Ethnic Groups'
--   ELSE 'Unknown'
  END as DEMOGRAPHICBREAKDOWN
,'All submissions' as ORGANISATIONBREAKDOWN
,'MHSDS' AS DATASOURCE
,'All prov' as ORGCODE
,'All providers' as ORGNAME
,COUNT(Person_ID) AS  MHSDS_COUNT
FROM
(SELECT
      B.Person_ID,
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender,
      COUNT(DISTINCT B.UniqMHActEpisodeID) AS DETENTION_COUNT
      FROM
      $db_output.detentions AS B 
      LEFT JOIN $db_output.mhs001_latest C ON B.Person_ID = C.PERSON_ID
      WHERE
      MHA_Logic_Cat_full in ('A','B','C','D','P')
      GROUP BY 
      B.PERSON_ID, 
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender) AS A
WHERE 
EthnicCategory is not null
GROUP BY 
CASE
  WHEN EthnicCategory IN ('A','B','C') THEN 'White'
  WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
  WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
  WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
  WHEN EthnicCategory IN ('R','S','Z','99') THEN 'Other Ethnic Groups'
--   ELSE 'Unknown'
  END, 
CASE WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
ELSE CAST(DETENTION_COUNT AS string) END 
order by 1,2

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted   -- CA changed the ethnic descriptions here to match those in the MHA_CSV_Full table so that it's pulling out accurately in the Master notebook

SELECT
DISTINCT
'$year' as YEAR
,'People subject to repeat detentions' as MEASURE
,CASE 
  WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
  ELSE CAST(DETENTION_COUNT AS string) 
  END AS MEASURESUBCATEGORY
,'Ethnicity' as DEMOGRAPHIC
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
  END as DEMOGRAPHICBREAKDOWN
,'All submissions' as ORGANISATIONBREAKDOWN
,'MHSDS' AS DATASOURCE
,'All prov' as ORGCODE
,'All providers' as ORGNAME
,COUNT(Person_ID) AS  MHSDS_COUNT
FROM
(SELECT
      B.Person_ID,
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender,
      COUNT(DISTINCT B.UniqMHActEpisodeID) AS DETENTION_COUNT
      FROM
      $db_output.detentions AS B 
      LEFT JOIN $db_output.mhs001_latest C ON B.Person_ID = C.PERSON_ID
      WHERE
      MHA_Logic_Cat_full in ('A','B','C','D','P')
      GROUP BY 
      B.PERSON_ID, 
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender) AS A
--WHERE 
--ETHNICCATEGORY IS NOT NULL
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
  END,
CASE WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
ELSE CAST(DETENTION_COUNT AS string) END 
order by 1,2, 4, 3

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
DISTINCT
'$year' as YEAR
,'People subject to repeat detentions' as MEASURE
,CASE 
  WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
  ELSE CAST(DETENTION_COUNT AS string) 
  END AS MEASURESUBCATEGORY
,'Age' as DEMOGRAPHIC
,CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
ELSE 'Unknown'
END as DEMOGRAPHICBREAKDOWN
,'All submissions' as ORGANISATIONBREAKDOWN
,'MHSDS' AS DATASOURCE
,'All prov' as ORGCODE
,'All providers' as ORGNAME
,COUNT(Person_ID) AS  MHSDS_COUNT
FROM
(SELECT
      B.Person_ID,
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender,
      COUNT(DISTINCT B.UniqMHActEpisodeID) AS DETENTION_COUNT
      FROM
      $db_output.detentions AS B 
      LEFT JOIN $db_output.mhs001_latest C ON B.Person_ID = C.PERSON_ID
      WHERE
      MHA_Logic_Cat_full in ('A','B','C','D','P')
      GROUP BY 
      B.PERSON_ID, 
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender) AS A
GROUP BY 
CASE 
WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
ELSE 'Unknown'
END,
CASE WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
ELSE CAST(DETENTION_COUNT AS string) END 
order by 1,2

-- COMMAND ----------

INSERT INTO $db_output.mha_unformatted

SELECT
DISTINCT
'$year' as YEAR
,'People subject to repeat detentions' as MEASURE
,CASE 
  WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
  ELSE CAST(DETENTION_COUNT AS string) 
  END AS MEASURESUBCATEGORY
,'Gender' as DEMOGRAPHIC
,Der_Gender as DEMOGRAPHICBREAKDOWN
,'All submissions' as ORGANISATIONBREAKDOWN
,'MHSDS' AS DATASOURCE
,'All prov' as ORGCODE
,'All providers' as ORGNAME
,COUNT(Person_ID) AS  MHSDS_COUNT
FROM
(SELECT
      B.Person_ID,
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender,
      COUNT(DISTINCT B.UniqMHActEpisodeID) AS DETENTION_COUNT
      FROM
      $db_output.detentions AS B 
      LEFT JOIN $db_output.mhs001_latest C ON B.Person_ID = C.PERSON_ID
      WHERE
      MHA_Logic_Cat_full in ('A','B','C','D','P')
      GROUP BY 
      B.PERSON_ID, 
      C.AgeRepPeriodEnd,
      C.EthnicCategory,
      C.Der_Gender) AS A
GROUP BY Der_Gender,
CASE WHEN CAST(DETENTION_COUNT AS string) >= 7 THEN '7 and over'
ELSE CAST(DETENTION_COUNT AS string) END 
order by 4,3