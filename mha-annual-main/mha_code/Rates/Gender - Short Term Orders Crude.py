# Databricks notebook source
 %sql
 create or replace temporary view gender_pop as
 select Der_Gender, sum(Population) as population
 from $db_output.ons_pop_v2_age_gender
 group by Der_Gender

# COMMAND ----------

 %sql
 create or replace temporary view gender_stos as
 select 
 Der_Gender,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.Short_Term_Orders
 WHERE
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'
 group by Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.gender_stos_rates;
 CREATE TABLE IF NOT EXISTS $db_output.gender_stos_rates AS
 -- create or replace global temporary view gender_stos_rates as
 select
 "Gender" as demographic_breakdown,
 "All" as organisation_breakdown,
 a.Der_Gender as primary_level,
 case when a.Der_Gender = "1" then "Male"
      when a.Der_Gender = "2" then "Female"
      when a.Der_Gender = "3" then "Non-binary"
      when a.Der_Gender = "4" then "Other (not listed)"
      when a.Der_Gender = "9" then "Indeterminate"
      end as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 "Section 136" as sub_measure,
 a.count,
 coalesce(b.population, 0) as population,
 coalesce(round((a.count / b.population) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from gender_stos a
 left join gender_pop b on a.Der_Gender = b.Der_Gender
 where a.Der_Gender in ('1', '2', '3', '4', '9')
 group by 
 a.Der_Gender, 
 case when a.Der_Gender = "1" then "Male"
      when a.Der_Gender = "2" then "Female"
      when a.Der_Gender = "3" then "Non-binary"
      when a.Der_Gender = "4" then "Other (not listed)"
      when a.Der_Gender = "9" then "Indeterminate"
      end,
 a.count, b.population

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_gender_stos_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_gender_stos_rates AS
 -- create or replace global temporary view all_gender_stos_rates as
 select
 "Gender" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 "Section 136" as sub_measure,
 sum(a.count) as count,
 sum(b.population) as population,
 round((sum(a.count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from gender_stos a
 left join gender_pop b on a.Der_Gender = b.Der_Gender
 where a.Der_Gender in ('1', '2', '3', '4', '9')