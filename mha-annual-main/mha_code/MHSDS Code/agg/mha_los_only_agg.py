# Databricks notebook source
 %sql
 INSERT INTO $db_output.mha_los
 ---national all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, national all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY CATEGORY

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, national all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY CATEGORY, Description

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---gender all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM 
 $db_output.mha_los_quartiles
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, gender all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, gender all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM 
 $db_output.mha_los_quartiles
 GROUP BY Category, Description, Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---age band all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Age

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, age band all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Age

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, age band all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Description, Age

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---higher ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Higher_Ethnic_Category

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, higher ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Higher_Ethnic_Category

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, higher ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Description, Higher_Ethnic_Category

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---lower ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Ethnic_Category_Description

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, lower ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Ethnic_Category_Description

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, lower ethnicity all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Description, Ethnic_Category_Description

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---deprivation all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, deprivation all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los
 ---mha category, mha description, deprivation all detentions length of stay in days (n, lower quartile, median and upper quartile)
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
 ---All MHA_LOS values for grouping are put in an array then percentile value calculated for lower quartile, median and upper quartile
 ---Final value is returned in a list so trim removes the square brackets
 FROM $db_output.mha_los_quartiles
 GROUP BY Category, Description, IMD_Decile

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_lookup; 
 CREATE TABLE $db_output.mha_los_lookup as
 ---creates a lookup table which creates a row for every possible Geography, OrgCode, MHA Category, MHA Description and Demographic combination
 select
 "England" as Geography,
 "England" as OrgCode,
 "England" as OrgName,
 r.CATEGORY as MHA_Most_Severe_Category,
 r.Description as MHA_Most_severe_section,
 a.Demographic as Demographic,
 a.Demographic_Category as Demographic_Category
 from (select distinct CATEGORY, Description, "a" as tag FROM $db_output.excel_lod_mha_lookup) r
 cross join (select distinct Demographic, Demographic_Category, "a" as tag FROM $db_output.mha_los) a ON r.tag = a.tag
 order by r.CATEGORY, r.Description, a.Demographic, a.Demographic_Category

# COMMAND ----------

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
 COALESCE(CAST(A.METRIC_VALUE as int), 0) AS COUNT, ---Geography, OrgCode, MHA Category, MHA Description and Demographic combination doesn't exist in submitted data coalesce to 0
 COALESCE(CAST(A.LOWER_QUARTILE as float), 0) AS LOWER_QUARTILE,
 COALESCE(CAST(A.MEDIAN as float), 0) AS MEDIAN,
 COALESCE(CAST(A.UPPER_QUARTILE as float), 0) AS UPPER_QUARTILE
 FROM $db_output.mha_los_lookup L
 LEFT JOIN $db_output.mha_los A 
 on L.OrgCode = A.OrgCode and L.MHA_MOST_SEVERE_CATEGORY = A.MHA_MOST_SEVERE_CATEGORY AND L.MHA_Most_severe_Section = A.MHA_Most_severe_Section and L.Demographic = A.Demographic AND L.DEMOGRAPHIC_CATEGORY = A.DEMOGRAPHIC_CATEGORY