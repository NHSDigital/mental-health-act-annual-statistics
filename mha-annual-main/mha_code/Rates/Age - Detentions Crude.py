# Databricks notebook source
 %sql
 create or replace temporary view age_pop as
 select 
 CASE
 	WHEN Age_group = '0 to 4' THEN '15 and under'
 	WHEN Age_group = '5 to 7' THEN '15 and under'
 	WHEN Age_group = '8 to 9' THEN '15 and under'
 	WHEN Age_group = '10 to 14' THEN '15 and under'
 	WHEN Age_group = '15' THEN '15 and under'
 	WHEN Age_group = '16 to 17' THEN '16 to 17'
 	WHEN Age_group = '18 to 19' THEN '18 to 34'
 	WHEN Age_group = '20 to 24' THEN '18 to 34'
 	WHEN Age_group = '25 to 29' THEN '18 to 34'
 	WHEN Age_group = '30 to 34' THEN '18 to 34'
 	WHEN Age_group = '35 to 39' THEN '35 to 49'
 	WHEN Age_group = '40 to 44' THEN '35 to 49'
 	WHEN Age_group = '45 to 49' THEN '35 to 49'
 	WHEN Age_group = '50 to 54' THEN '50 to 64'
 	WHEN Age_group = '55 to 59' THEN '50 to 64'
 	WHEN Age_group = '60 to 64' THEN '50 to 64'
 	WHEN Age_group = '65 to 69' THEN '65 and over'
 	WHEN Age_group = '70 to 74' THEN '65 and over'
 	WHEN Age_group = '75 to 79' THEN '65 and over'
 	WHEN Age_group = '80 to 84' THEN '65 and over'
 	WHEN Age_group = '85 and over' THEN '65 and over'
 	end AS Der_Age_Group,
 sum(Population) as population
 from $db_output.ons_pop_v2_age_gender
 group by 
 CASE
 	WHEN Age_group = '0 to 4' THEN '15 and under'
 	WHEN Age_group = '5 to 7' THEN '15 and under'
 	WHEN Age_group = '8 to 9' THEN '15 and under'
 	WHEN Age_group = '10 to 14' THEN '15 and under'
 	WHEN Age_group = '15' THEN '15 and under'
 	WHEN Age_group = '16 to 17' THEN '16 to 17'
 	WHEN Age_group = '18 to 19' THEN '18 to 34'
 	WHEN Age_group = '20 to 24' THEN '18 to 34'
 	WHEN Age_group = '25 to 29' THEN '18 to 34'
 	WHEN Age_group = '30 to 34' THEN '18 to 34'
 	WHEN Age_group = '35 to 39' THEN '35 to 49'
 	WHEN Age_group = '40 to 44' THEN '35 to 49'
 	WHEN Age_group = '45 to 49' THEN '35 to 49'
 	WHEN Age_group = '50 to 54' THEN '50 to 64'
 	WHEN Age_group = '55 to 59' THEN '50 to 64'
 	WHEN Age_group = '60 to 64' THEN '50 to 64'
 	WHEN Age_group = '65 to 69' THEN '65 and over'
 	WHEN Age_group = '70 to 74' THEN '65 and over'
 	WHEN Age_group = '75 to 79' THEN '65 and over'
 	WHEN Age_group = '80 to 84' THEN '65 and over'
 	WHEN Age_group = '85 and over' THEN '65 and over'
 	end

# COMMAND ----------

 %sql
 create or replace temp view adult_cyp_pop as

 select
 CASE
 	WHEN Age_group = '0 to 4' THEN '17 and under'
 	WHEN Age_group = '5 to 7' THEN '17 and under'
 	WHEN Age_group = '8 to 9' THEN '17 and under'
 	WHEN Age_group = '10 to 14' THEN '17 and under'
 	WHEN Age_group = '15' THEN '17 and under'
 	WHEN Age_group = '16 to 17' THEN '17 and under'
 	WHEN Age_group = '18 to 19' THEN '18 and over'
 	WHEN Age_group = '20 to 24' THEN '18 and over'
 	WHEN Age_group = '25 to 29' THEN '18 and over'
 	WHEN Age_group = '30 to 34' THEN '18 and over'
 	WHEN Age_group = '35 to 39' THEN '18 and over'
 	WHEN Age_group = '40 to 44' THEN '18 and over'
 	WHEN Age_group = '45 to 49' THEN '18 and over'
 	WHEN Age_group = '50 to 54' THEN '18 and over'
 	WHEN Age_group = '55 to 59' THEN '18 and over'
 	WHEN Age_group = '60 to 64' THEN '18 and over'
 	WHEN Age_group = '65 to 69' THEN '18 and over'
 	WHEN Age_group = '70 to 74' THEN '18 and over'
 	WHEN Age_group = '75 to 79' THEN '18 and over'
 	WHEN Age_group = '80 to 84' THEN '18 and over'
 	WHEN Age_group = '85 and over' THEN '18 and over'
 	end AS Der_Adult_CYP_Group,
 sum(Population) as population
 from $db_output.ons_pop_v2_age_gender
 group by 
 CASE
 	WHEN Age_group = '0 to 4' THEN '17 and under'
 	WHEN Age_group = '5 to 7' THEN '17 and under'
 	WHEN Age_group = '8 to 9' THEN '17 and under'
 	WHEN Age_group = '10 to 14' THEN '17 and under'
 	WHEN Age_group = '15' THEN '17 and under'
 	WHEN Age_group = '16 to 17' THEN '17 and under'
 	WHEN Age_group = '18 to 19' THEN '18 and over'
 	WHEN Age_group = '20 to 24' THEN '18 and over'
 	WHEN Age_group = '25 to 29' THEN '18 and over'
 	WHEN Age_group = '30 to 34' THEN '18 and over'
 	WHEN Age_group = '35 to 39' THEN '18 and over'
 	WHEN Age_group = '40 to 44' THEN '18 and over'
 	WHEN Age_group = '45 to 49' THEN '18 and over'
 	WHEN Age_group = '50 to 54' THEN '18 and over'
 	WHEN Age_group = '55 to 59' THEN '18 and over'
 	WHEN Age_group = '60 to 64' THEN '18 and over'
 	WHEN Age_group = '65 to 69' THEN '18 and over'
 	WHEN Age_group = '70 to 74' THEN '18 and over'
 	WHEN Age_group = '75 to 79' THEN '18 and over'
 	WHEN Age_group = '80 to 84' THEN '18 and over'
 	WHEN Age_group = '85 and over' THEN '18 and over'
 	end 

# COMMAND ----------

 %sql
 create or replace temporary view age_detentions as
 select 
 case 
     when AgeRepPeriodEnd between 0 and 15 then '15 and under'
     when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
     when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
     when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
     when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
     when AgeRepPeriodEnd >= 65 then '65 and over'
     else 'Unknown'
     end as Der_Age_Group,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 case 
     when AgeRepPeriodEnd between 0 and 15 then '15 and under'
     when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
     when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
     when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
     when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
     when AgeRepPeriodEnd >= 65 then '65 and over'
     else 'Unknown'
     end

# COMMAND ----------

 %sql
 create or replace temporary view adult_cyp_detentions as
 select 
 case when AgeRepPeriodEnd < 18 then '17 and under'
      when AgeRepPeriodEnd >= 18 then '18 and over'
      else 'Unknown'
      end as Der_Adult_CYP_Group,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 case when AgeRepPeriodEnd < 18 then '17 and under'
      when AgeRepPeriodEnd >= 18 then '18 and over'
      else 'Unknown'
      end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.age_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.age_detentions_rates AS
 -- create or replace global temporary view age_detentions_rates as
 select
 "Age" as demographic_breakdown,
 "All" as organisation_breakdown,
 a.Der_Age_Group as primary_level,
 a.Der_Age_Group as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 a.count,
 coalesce(b.population, 0) as population,
 coalesce(round((a.count / b.population) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from age_detentions a
 left join age_pop b on a.Der_Age_Group = b.Der_Age_Group
 where a.Der_Age_Group <> "Unknown"
 group by a.Der_Age_Group, a.count, b.population

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.adult_cyp_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.adult_cyp_detentions_rates AS
 -- create or replace global temporary view adult_cyp_detentions_rates as
 select
 "Age" as demographic_breakdown,
 "All" as organisation_breakdown,
 a.Der_Adult_CYP_Group as primary_level,
 a.Der_Adult_CYP_Group as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 a.count,
 coalesce(b.population, 0) as population,
 coalesce(round((a.count / b.population) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from adult_cyp_detentions a
 left join adult_cyp_pop b on a.Der_Adult_CYP_Group = b.Der_Adult_CYP_Group
 where a.Der_Adult_CYP_Group <> "Unknown"
 group by a.Der_Adult_CYP_Group, a.count, b.population

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_age_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_age_detentions_rates AS
 -- create or replace global temporary view all_age_detentions_rates as
 select
 "Age" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 sum(a.count) as count,
 sum(b.population) as population,
 round((sum(a.count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from age_detentions a
 left join age_pop b on a.Der_Age_Group = b.Der_Age_Group
 where a.Der_Age_Group <> "Unknown"