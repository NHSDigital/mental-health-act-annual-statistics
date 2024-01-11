# Databricks notebook source
 %sql
 create or replace temporary view imd_detentions as
 select 
 CASE WHEN IMD_DECILE is null THEN "Not stated/Not known/Invalid" ELSE IMD_DECILE END AS Der_IMD_Decile,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by CASE WHEN IMD_DECILE is null THEN "Not stated/Not known/Invalid" ELSE IMD_DECILE END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.imd_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.imd_detentions_rates AS
 -- create or replace global temporary view imd_detentions_rates as
 select
 "IMD" as demographic_breakdown,
 "All" as organisation_breakdown,
 a.Der_IMD_Decile as primary_level,
 a.Der_IMD_Decile as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 a.count as count,
 coalesce(b.Count, 0) as population,
 coalesce(round((a.count / b.Count) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from imd_detentions a
 left join $db_output.imd_pop b on a.Der_IMD_Decile = b.IMD_Decile
 group by a.Der_IMD_Decile, a.count, b.Count

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_imd_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_imd_detentions_rates AS
 -- create or replace global temporary view all_imd_detentions_rates as
 select
 "IMD" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 sum(a.count) as count,
 sum(b.Count) as population,
 round((sum(a.count) / sum(b.Count)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from imd_detentions a
 left join $db_output.imd_pop b on a.Der_IMD_Decile = b.IMD_Decile