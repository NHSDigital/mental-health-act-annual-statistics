# Databricks notebook source
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Ethnicity Crude/Standardised Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_eth_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_eth_rates
 
 SELECT * FROM $db_output.eth_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.eth_stos_rates
 UNION ALL
 SELECT * FROM $db_output.eth_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.eth_discharges_rates

# COMMAND ----------

# DBTITLE 1,Gender Crude Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_gender_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_gender_rates
 
 SELECT * FROM $db_output.gender_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_gender_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.gender_stos_rates
 UNION ALL
 SELECT * FROM $db_output.all_gender_stos_rates
 UNION ALL
 SELECT * FROM $db_output.gender_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.all_gender_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.gender_discharges_rates
 UNION ALL
 SELECT * FROM $db_output.all_gender_discharges_rates

# COMMAND ----------

# DBTITLE 1,Age Crude Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_age_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_age_rates
 
 SELECT * FROM $db_output.age_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.adult_cyp_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_age_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.age_stos_rates
 UNION ALL
 SELECT * FROM $db_output.adult_cyp_stos_rates
 UNION ALL
 SELECT * FROM $db_output.all_age_stos_rates
 UNION ALL
 SELECT * FROM $db_output.age_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.adult_cyp_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.all_age_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.age_discharges_rates
 UNION ALL
 SELECT * FROM $db_output.adult_cyp_discharges_rates
 UNION ALL
 SELECT * FROM $db_output.all_age_discharges_rates

# COMMAND ----------

# DBTITLE 1,IMD Crude Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_imd_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_imd_rates
 
 SELECT * FROM $db_output.imd_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_imd_detentions_rates

# COMMAND ----------

# DBTITLE 1,CCG Crude Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_ccg_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_ccg_rates
 
 SELECT * FROM $db_output.ccg_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_ccg_detentions_rates

# COMMAND ----------

# DBTITLE 1,STP Crude Rates
 %sql
 ---COuld we get rid and do an INSERT instead in Rates notebooks
 DROP TABLE IF EXISTS $db_output.mha_stp_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_stp_rates
 
 SELECT * FROM $db_output.stp_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_stp_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.stp_imd_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.stp_age_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.stp_gender_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.stp_detentions_types_rates
 UNION ALL
 SELECT * FROM $db_output.all_stp_detentions_types_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_following_admission_to_hospital_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_following_admission_to_hospital_sections_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_following_revocation_of_cto_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_sections_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_on_admission_to_hospital_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_detentions_on_admission_to_hospital_parts_rates
 UNION ALL
 SELECT * FROM $db_output.stp_short_term_orders_rates
 UNION ALL
 SELECT * FROM $db_output.all_stp_short_term_orders_rates
 UNION ALL
 SELECT * FROM $db_output.stp_eth_short_term_orders_rates

# COMMAND ----------

# DBTITLE 1,MHSDS + ECDS All demographics Crude Rates
 %sql
 DROP TABLE IF EXISTS $db_output.mha_ecds_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_ecds_rates
 
 SELECT * FROM $db_output.all_ecds_detentions_rates
 UNION ALL
 SELECT * FROM $db_output.all_ecds_stos_rates
 UNION ALL
 SELECT * FROM $db_output.all_mhsds_ctos_rates
 UNION ALL
 SELECT * FROM $db_output.all_mhsds_discharges_rates
 UNION ALL
 SELECT * FROM $db_output.all_detentions_types_rates

# COMMAND ----------

# DBTITLE 1,Raw output
 %sql
 DROP TABLE IF EXISTS $db_output.mha_raw_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_raw_rates
 SELECT * FROM $db_output.mha_eth_rates
 UNION ALL
 SELECT * FROM $db_output.mha_gender_rates
 UNION ALL
 SELECT * FROM $db_output.mha_age_rates
 UNION ALL
 SELECT * FROM $db_output.mha_imd_rates
 UNION ALL
 SELECT * FROM $db_output.mha_ccg_rates
 UNION ALL
 SELECT * FROM $db_output.mha_stp_rates
 UNION ALL
 SELECT * FROM $db_output.mha_ecds_rates

# COMMAND ----------

# DBTITLE 1,Output Lookup 
 %sql
 DROP TABLE IF EXISTS $db_output.mha_lookup_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_lookup_rates
 select 
 distinct
 a.demographic_breakdown,
 a.organisation_breakdown,
 a.primary_level,
 a.primary_level_desc,
 a.secondary_level,
 a.secondary_level_desc,
 b.count_of,
 b.sub_measure
 from (select distinct demographic_breakdown, organisation_breakdown, primary_level, primary_level_desc, secondary_level, secondary_level_desc, 'a' as tag from $db_output.mha_raw_rates) a
 cross join (select distinct count_of, sub_measure, 'a' as tag from $db_output.mha_raw_rates) b on a.tag = b.tag
 order by a.demographic_breakdown, a.organisation_breakdown, b.count_of, a.primary_level, a.secondary_level

# COMMAND ----------

# DBTITLE 1,Final Output
 %sql
 DROP TABLE IF EXISTS $db_output.mha_final_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_final_rates
 SELECT 
 a.demographic_breakdown,
 a.organisation_breakdown,
 a.primary_level,
 a.primary_level_desc,
 a.secondary_level,
 a.secondary_level_desc,
 a.count_of,
 a.sub_measure,
 COALESCE(b.count, 0) as count,
 COALESCE(b.population, 0) as population,
 COALESCE(b.crude_rate_per_100000, 0) as CR,
 COALESCE(b.standardised_rate_per_100000, 0) as SR,
 COALESCE(b.CI, 0) as CI
 FROM $db_output.mha_lookup_rates a
 LEFT JOIN $db_output.mha_raw_rates b ON a.demographic_breakdown = b.demographic_breakdown AND a.organisation_breakdown = b.organisation_breakdown AND a.primary_level = b.primary_level AND a.secondary_level = b.secondary_level AND a.count_of = b.count_of AND a.sub_measure = b.sub_measure

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.mha_final_rates