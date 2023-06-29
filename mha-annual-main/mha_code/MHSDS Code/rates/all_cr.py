# Databricks notebook source
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

 %sql
 create or replace temporary view total_pop as
 ---get total population with a tag for joining
 select 
 'a' as tag,
 sum(Population) as population
 from $db_output.ons_pop_v2_age_gender

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_detentions as
 ---get count of all mhsds detentions
 select 
 "All detentions" as Measure,
 "All" as measureSubcategory,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ecds_detentions as
 ---combine mhsds detentions and ecds detentions 
 select 'a' as tag,
 m.count + e.Count as total_count
 from mhsds_detentions m
 left join $db_output.ECDS_agg e on m.Measure = e.Measure and m.measureSubcategory = e.measureSubcategory and e.OrganisationBreakdown = "All submissions"

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_stos as
 ---get count of mhsds short term orders with tag for joining
 select 
 "Short term orders" as Measure,
 "Section 136" as measureSubcategory,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.Short_Term_Orders
 WHERE
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ecds_stos as
 ---combine mhsds short term orders and ecds short term orders
 select 'a' as tag,
 m.count + e.Count as total_count
 from mhsds_stos m
 left join $db_output.ECDS_agg e on m.Measure = e.Measure and m.measureSubcategory = e.measureSubcategory and e.OrganisationBreakdown = "All submissions"

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ctos as
 ---get count of mhsds community treatment orders
 select 'a' as tag,
 COUNT(DISTINCT CTO_UniqMHActEpisodeID) as total_count
 from $db_output.CTO

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_discharges as
 ---get count of mhsds discharges following detention
 select 'a' as tag,
 COUNT(DISTINCT UniqHospProvSpellID) as total_count
 from $db_output.detentions
 WHERE
 DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'
 AND EndDateMHActLegalStatusClass <= DischDateHospProvSpell
 AND EndDateMHActLegalStatusClass >= StartDateHospProvSpell
 AND (MHA_LOGIC_CAT_FULL IN ('A','B','C','D','P'))

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_ecds_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_ecds_detentions_rates AS
 ---combine mhsds and ecds detentions and population count and calculate crude rate at national level
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_ecds_detentions a
 left join total_pop b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_ecds_stos_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_ecds_stos_rates AS
 ---combine mhsds and ecds short term orders and population count and calculate crude rate at national level
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_ecds_stos a
 left join total_pop b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_mhsds_ctos_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_mhsds_ctos_rates AS
 ---combine mhsds community treatment orders and population count and calculate crude rate at national level
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Uses of CTOs" as count_of,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_ctos a
 left join total_pop b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_mhsds_discharges_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_mhsds_discharges_rates AS
 ---combine mhsds discharges following detention and population count and calculate crude rate at national level
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Discharges following detention" as count_of,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_discharges a
 left join total_pop b on a.tag = b.tag