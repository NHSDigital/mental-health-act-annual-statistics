-- Databricks notebook source
 %md

 National - Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
CATEGORY

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
CATEGORY,
Description

-- COMMAND ----------

 %md

 National - Gender Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Der_Gender

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Description,
Der_Gender

-- COMMAND ----------

 %md 

 Age - National Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Age

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Age

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Description,
Age

-- COMMAND ----------

 %md

 National - Higher Ethnicity Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Higher_Ethnic_Category

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Description,
Higher_Ethnic_Category

-- COMMAND ----------

 %md

 National - Lower Ethnicity Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Ethnic_Category_Description

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Ethnic_Category_Description

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Description,
Ethnic_Category_Description

-- COMMAND ----------

 %md

 National - IMD Counts and Quartiles

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
IMD_Decile

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
IMD_Decile

-- COMMAND ----------

INSERT INTO $db_output.mha_cto_los1

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
$db_output.mha_los_quartiles_cto
GROUP BY 
Category,
Description,
IMD_Decile

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cto_los1_lookup; 
 CREATE TABLE $db_output.mha_cto_los1_lookup as
 select
 "England" as Geography,
 "England" as OrgCode,
 "England" as OrgName,
 r.CATEGORY as MHA_Most_Severe_Category,
 r.Description as MHA_Most_severe_section,
 a.Demographic as Demographic,
 a.Demographic_Category as Demographic_Category
 from (select distinct CATEGORY, Description, "a" as tag FROM $db_output.excel_lod_mha_cto_lookup) r
 cross join (select distinct Demographic, Demographic_Category, "a" as tag FROM $db_output.mha_cto_los1) a ON r.tag = a.tag
 order by r.CATEGORY, r.Description, a.Demographic, a.Demographic_Category

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_cto_output; 
 CREATE TABLE $db_output.mha_los_cto_output USING DELTA as
 SELECT 
 L.Geography,
 L.ORGCODE,
 L.ORGNAME,
 L.MHA_MOST_SEVERE_CATEGORY,
 L.MHA_Most_severe_Section,
 L.DEMOGRAPHIC,
 L.DEMOGRAPHIC_CATEGORY,
 COALESCE(CAST(A.METRIC_VALUE as int), "*") AS COUNT,
 COALESCE(CAST(A.LOWER_QUARTILE as float), "*") AS LOWER_QUARTILE,
 COALESCE(CAST(A.MEDIAN as float), "*") AS MEDIAN,
 COALESCE(CAST(A.UPPER_QUARTILE as float), "*") AS UPPER_QUARTILE
 FROM $db_output.mha_cto_los1_lookup L
 LEFT JOIN $db_output.mha_cto_los1 A 
 on L.OrgCode = A.OrgCode and L.MHA_MOST_SEVERE_CATEGORY = A.MHA_MOST_SEVERE_CATEGORY AND L.MHA_Most_severe_Section = A.MHA_Most_severe_Section and L.Demographic = A.Demographic AND L.DEMOGRAPHIC_CATEGORY = A.DEMOGRAPHIC_CATEGORY