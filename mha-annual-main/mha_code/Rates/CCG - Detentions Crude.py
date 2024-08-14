# Databricks notebook source
 %run ../Functions

# COMMAND ----------

 %sql
 create or replace temporary view ccg_detentions as
 select 
 CASE WHEN IC_REC_CCG is null THEN "UNKNOWN" ELSE IC_REC_CCG END AS CCG_CODE,
 CASE WHEN CCG_NAME is null THEN "UNKNOWN" ELSE CCG_NAME END AS CCG_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN IC_REC_CCG is null THEN "UNKNOWN" ELSE IC_REC_CCG END,
 CASE WHEN CCG_NAME is null THEN "UNKNOWN" ELSE CCG_NAME END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ccg_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.ccg_detentions_rates AS
 -- create or replace global temporary view ccg_detentions_rates as
 select
 "All" as demographic_breakdown,
 "CCG" as organisation_breakdown,
 a.CCG_CODE as primary_level,
 a.CCG_NAME as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 a.count as count,
 coalesce(b.POP, 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from ccg_detentions a
 left join $db_output.mha_ccg_pop b on a.CCG_CODE = b.CCG_CODE
 group by a.CCG_CODE, a.CCG_NAME, a.count, b.POP

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_ccg_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_ccg_detentions_rates AS
 -- create or replace global temporary view all_ccg_detentions_rates as
 select
 "All" as demographic_breakdown,
 "CCG" as organisation_breakdown,
 "All prov" as primary_level,
 "All prov" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 sum(a.unsup_count) as count,
 sum(b.POP) as population,
 round((sum(a.unsup_count) / sum(b.POP)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from ccg_detentions a
 left join $db_output.mha_ccg_pop b on a.CCG_CODE = b.CCG_CODE