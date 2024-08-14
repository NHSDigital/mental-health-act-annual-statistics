-- Databricks notebook source
 %run ../Functions

-- COMMAND ----------

 %md
 
 National - Counts and Quartiles

-- COMMAND ----------

 %sql
 OPTIMIZE $db_output.mha_los_quartiles;
 REFRESH TABLE $db_output.mha_los_quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
CATEGORY

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
CATEGORY,
Description

-- COMMAND ----------

 %md
 
 National - Gender Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Gender' as Demographic,
Der_Gender as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Gender' as Demographic,
Der_Gender as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'Gender' as Demographic,
Der_Gender as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Description,
Der_Gender

-- COMMAND ----------

 %md 
 
 Age - National Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Age' as Demographic,
Age as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Age

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Age' as Demographic,
Age as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Age

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'Age' as Demographic,
Age as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Description,
Age

-- COMMAND ----------

 %md
 
 National - Higher Ethnicity Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Description,
Higher_Ethnic_Category

-- COMMAND ----------

 %md
 
 National - Lower Ethnicity Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Lower Level Ethnicity' as Demographic,
Ethnic_Category_Description as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Ethnic_Category_Description

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Lower Level Ethnicity' as Demographic,
Ethnic_Category_Description as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Ethnic_Category_Description

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'Lower Level Ethnicity' as Demographic,
Ethnic_Category_Description as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Description,
Ethnic_Category_Description

-- COMMAND ----------

 %md
 
 National - IMD Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'IMD Decile' as Demographic,
IMD_Decile as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
IMD_Decile

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'IMD Decile' as Demographic,
IMD_Decile as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
IMD_Decile

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'England' as Geography,
'England' as OrgCode,
'England' as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'IMD Decile' as Demographic,
IMD_Decile as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
Category,
Description,
IMD_Decile

-- COMMAND ----------

 %md
 
 STP - Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY STP_Code, STP_Name

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
STP_Code, STP_Name, CATEGORY

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'All' as Demographic,
'All' as Demographic_Category,
COUNT(DISTINCT ID) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
STP_Code, STP_Name,
CATEGORY,
Description

-- COMMAND ----------

 %md
 
 STP - Higher Ethnicity Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
'All Detentions' as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
suppress(COUNT(DISTINCT ID)) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
STP_Code, STP_Name,Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
Category as MHA_Most_Severe_Category,
'All' as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
suppress(COUNT(DISTINCT ID)) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
STP_Code, STP_Name,
Category,
Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_los

SELECT 
'STP' as Geography,
STP_Code as OrgCode,
STP_Name as OrgName,
Category as MHA_Most_Severe_Category,
Description as MHA_Most_Severe_Section,
'Higher Level Ethnicity' as Demographic,
Higher_Ethnic_Category as Demographic_Category,
suppress(COUNT(DISTINCT ID)) AS METRIC_VALUE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.25)) as string)) AS LOWER_QUARTILE,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.5)) as string)) AS MEDIAN,
TRIM('[]', CAST(PERCENTILE(MHA_LOS,array(0.75)) as string)) AS UPPER_QUARTILE
FROM 
$db_output.mha_los_quartiles
GROUP BY 
STP_Code, STP_Name,
Category,
Description,
Higher_Ethnic_Category

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_geog_lookup; 
 CREATE TABLE $db_output.mha_los_geog_lookup as
 select
 g.Geography as Geography,
 g.OrgCode as OrgCode,
 g.OrgName as OrgName,
 r.CATEGORY,
 r.Description
 from (select distinct Geography, OrgCode, OrgName, "a" as tag FROM $db_output.mha_los) g
 cross join (select distinct CATEGORY, Description, "a" as tag FROM $db_output.excel_lod_mha_lookup) r ON g.tag = r.tag
 order by g.Geography,g.OrgCode,g.OrgName, r.CATEGORY, r.Description

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_lookup; 
 CREATE TABLE $db_output.mha_los_lookup as
 select
 r.Geography as Geography,
 r.OrgCode as OrgCode,
 r.OrgName as OrgName,
 r.CATEGORY as MHA_Most_Severe_Category,
 r.Description as MHA_Most_severe_section,
 a.Demographic as Demographic,
 a.Demographic_Category as Demographic_Category
 from (select distinct Geography, OrgCode, OrgName, CATEGORY, Description, "a" as tag FROM $db_output.mha_los_geog_lookup) r
 cross join (select distinct Demographic, Demographic_Category, "a" as tag FROM $db_output.mha_los) a ON r.tag = a.tag
 order by r.Geography,r.OrgCode,r.OrgName,r.CATEGORY, r.Description, a.Demographic, a.Demographic_Category

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_output; 
 CREATE TABLE $db_output.mha_los_output as
 SELECT 
 L.Geography,
 L.ORGCODE,
 L.ORGNAME,
 L.MHA_MOST_SEVERE_CATEGORY,
 L.MHA_Most_severe_Section,
 L.DEMOGRAPHIC,
 L.DEMOGRAPHIC_CATEGORY,
 COALESCE(CAST(A.METRIC_VALUE as int), 0) AS COUNT,
 COALESCE(CAST(A.LOWER_QUARTILE as float), 0) AS LOWER_QUARTILE,
 COALESCE(CAST(A.MEDIAN as float), 0) AS MEDIAN,
 COALESCE(CAST(A.UPPER_QUARTILE as float), 0) AS UPPER_QUARTILE
 FROM $db_output.mha_los_lookup L
 LEFT JOIN $db_output.mha_los A 
 on L.Geography = A.Geography and
 L.OrgCode = A.OrgCode and L.MHA_MOST_SEVERE_CATEGORY = A.MHA_MOST_SEVERE_CATEGORY AND L.MHA_Most_severe_Section = A.MHA_Most_severe_Section and L.Demographic = A.Demographic AND L.DEMOGRAPHIC_CATEGORY = A.DEMOGRAPHIC_CATEGORY