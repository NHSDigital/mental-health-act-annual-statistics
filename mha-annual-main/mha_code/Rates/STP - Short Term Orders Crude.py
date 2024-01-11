# Databricks notebook source
 %run ../Functions

# COMMAND ----------

 %sql
 create or replace temporary view stp_short_term_orders as
 select 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END AS STP_CODE,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END AS STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 FROM $db_output.short_term_orders A
 WHERE MHA_Logic_Cat_full in ('E','F')
 group by 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_short_term_orders_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_short_term_orders_rates AS
 -- create or replace global temporary view stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 a.STP_CODE as primary_level,
 a.STP_NAME as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 "All" as sub_measure,
 a.count as count,
 coalesce(b.POP, 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_short_term_orders a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE
 group by a.STP_CODE, a.STP_NAME, a.count, b.POP

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_stp_short_term_orders_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_stp_short_term_orders_rates AS
 -- create or replace global temporary view all_stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 "All prov" as primary_level,
 "All prov" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 "All" as sub_measure,
 sum(a.unsup_count) as count,
 sum(b.POP) as population,
 round((sum(a.unsup_count) / sum(b.POP)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_short_term_orders a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_short_term_orders as
 select 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 CASE
     WHEN EthnicCategory IN ('A','B','C') THEN 'White'
     WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
     WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
     WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
     WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
     ELSE 'Unknown'
 END as Ethnicity,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 FROM $db_output.short_term_orders A
 WHERE MHA_Logic_Cat_full in ('E','F')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end,
 CASE
     WHEN EthnicCategory IN ('A','B','C') THEN 'White'
     WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
     WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
     WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
     WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
     ELSE 'Unknown'
 END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_eth_short_term_orders_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_short_term_orders_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "Short term orders" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_short_term_orders a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP