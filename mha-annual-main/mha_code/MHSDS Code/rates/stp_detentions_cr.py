# Databricks notebook source
#mhsds count suppression function
#rates at sub-national level are based on the suppressed count
def suppress(x, base=5):   
    if x < 5:
        return '*'
    else:
        return int(base * round(float(x)/base))
spark.udf.register("suppress", suppress)

# COMMAND ----------

 %sql
 create or replace temporary view stp_detentions as
 ---get detentions count at stp/icb level
 select 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END AS STP_CODE,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END AS STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count, ---sub-national total no suppression
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count ---sub-national level suppression
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb level
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 a.STP_CODE as primary_level,
 a.STP_NAME as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 a.count as count,
 coalesce(b.POP, 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_detentions a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE
 group by a.STP_CODE, a.STP_NAME, a.count, b.POP

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_stp_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_stp_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb total
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 "All prov" as primary_level,
 "All prov" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 sum(a.unsup_count) as count,
 sum(b.POP) as population,
 round((sum(a.unsup_count) / sum(b.POP)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_detentions a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE

# COMMAND ----------

 %sql
 create or replace temporary view stp_imd_detentions as
 ---combine detentions and population count and calculate crude rate at stp/icb and deprivation level
 select 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END AS STP_CODE,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END AS STP_NAME,
 CASE WHEN IMD_DECILE is null THEN "Not stated/Not known/Invalid" ELSE IMD_DECILE END AS Der_IMD_Decile,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count ---suppress count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END,
 CASE WHEN IMD_DECILE is null THEN "Not stated/Not known/Invalid" ELSE IMD_DECILE END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_imd_lookup; 
 CREATE TABLE $db_output.stp_imd_lookup AS
 ---creates lookup table which gets every combination of stp/icb and deprivation value. I.e. in count/population some stps/icbs may not have all deprivation values
 select 
 distinct
 a.STP_CODE,
 a.STP_NAME,
 b.Der_IMD_Decile
 from (select distinct STP_CODE, STP_NAME, 'a' as tag from stp_imd_detentions) a
 cross join (select distinct Der_IMD_Decile, 'a' as tag from stp_imd_detentions) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_imd_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_imd_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb and deprivation level
 select
 "IMD" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Der_IMD_Decile as secondary_level,
 l.Der_IMD_Decile as secondary_level_desc,
 "All detentions" as count_of,
 coalesce(a.count, "*") as count, ---if stp/icb and deprivation combination doesn't exist in mha data then coalesce to *
 coalesce(b.Count, 0) as population, ---if stp/icb and deprivation combination doesn't exist in population data then coalesce to 0
 coalesce(round((a.count / b.Count) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_imd_lookup l
 left join stp_imd_detentions a on l.STP_CODE = a.STP_CODE and l.Der_IMD_Decile = a.Der_IMD_Decile
 left join $db_output.stp_imd_pop b on l.STP_CODE = b.STP and l.Der_IMD_Decile = b.IMD_Decile
 group by l.STP_CODE, l.STP_NAME, l.Der_IMD_Decile, a.count, b.Count

# COMMAND ----------

 %sql
 create or replace temporary view stp_age_detentions as
 ---combine detentions and population count and calculate crude rate at stp/icb and age band level
 select
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 case 
     when AgeRepPeriodEnd between 0 and 15 then '15 and under'
     when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
     when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
     when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
     when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
     when AgeRepPeriodEnd >= 65 then '65 and over'
     else 'Unknown'
     end as Der_Age_Group,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count ---suppress count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end,
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
 DROP TABLE IF EXISTS $db_output.stp_age_lookup; 
 CREATE TABLE $db_output.stp_age_lookup AS
 ---creates lookup table which gets every combination of stp/icb and age band value. I.e. in count/population some stps/icbs may not have all age band values
 select 
 distinct
 a.STP_CODE,
 a.STP_NAME,
 b.Der_Age_Group
 from (select distinct STP_CODE, STP_NAME, 'a' as tag from stp_age_detentions) a
 cross join (select distinct Der_Age_Group, 'a' as tag from stp_age_detentions) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_age_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_age_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb and age band level
 select
 "Age" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Der_Age_Group as secondary_level,
 l.Der_Age_Group as secondary_level_desc,
 "All detentions" as count_of,
 coalesce(a.count, "*") as count, ---if stp/icb and age band combination doesn't exist in mha data then coalesce to *
 round(coalesce(b.POP, 0), 0) as population, ---if stp/icb and age band combination doesn't exist in population data then coalesce to 0
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_age_lookup l
 left join stp_age_detentions a on l.STP_CODE = a.STP_CODE and l.Der_Age_Group = a.Der_Age_Group
 left join $db_output.stp_age_pop b on l.STP_CODE = b.STP_CODE and l.Der_Age_Group = b.Der_Age_Group
 where l.Der_Age_Group <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Der_Age_Group, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_gender_detentions as
 ---combine detentions and population count and calculate crude rate at stp/icb and gender level
 select 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 Der_Gender,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count ---suppress count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end, 
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_gender_lookup; 
 CREATE TABLE $db_output.stp_gender_lookup AS
 ---creates lookup table which gets every combination of stp/icb and gender value. I.e. in count/population some stps/icbs may not have all gender values
 select 
 distinct
 a.STP_CODE,
 a.STP_NAME,
 b.Der_Gender
 from (select distinct STP_CODE, STP_NAME, 'a' as tag from stp_gender_detentions) a
 cross join (select distinct Der_Gender, 'a' as tag from stp_gender_detentions) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_gender_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_gender_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb and gender level
 select
 "Gender" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Der_Gender as secondary_level,
 case when l.Der_Gender = "1" then "Male"
      when l.Der_Gender = "2" then "Female"
      when l.Der_Gender = "3" then "Non-binary"
      when l.Der_Gender = "4" then "Other (not listed)"
      when l.Der_Gender = "9" then "Indeterminate"
      else "Unknown"
      end as secondary_level_desc,
 "All detentions" as count_of,
 coalesce(a.count, "*") as count, ---if stp/icb and gender combination doesn't exist in mha data then coalesce to *
 round(coalesce(b.POP, 0), 0) as population, ---if stp/icb and gender combination doesn't exist (or is not Male/Female) in population data then coalesce to 0
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_gender_lookup l
 left join stp_gender_detentions a on l.STP_CODE = a.STP_CODE and l.Der_Gender = a.Der_Gender
 left join $db_output.stp_gender_pop b on l.STP_CODE = b.STP_CODE and l.Der_Gender = b.Sex
 where l.Der_Gender is not null
 group by l.STP_CODE, l.STP_NAME, l.Der_Gender, 
 case when l.Der_Gender = "1" then "Male"
      when l.Der_Gender = "2" then "Female"
      when l.Der_Gender = "3" then "Non-binary"
      when l.Der_Gender = "4" then "Other (not listed)"
      when l.Der_Gender = "9" then "Indeterminate"
      else "Unknown"
      end,
 a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions as
 ---combine detentions and population count and calculate crude rate at stp/icb and higher ethnicity level
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
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count ---suppress count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
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
 DROP TABLE IF EXISTS $db_output.stp_eth_lookup; 
 CREATE TABLE $db_output.stp_eth_lookup AS
 ---creates lookup table which gets every combination of stp/icb and gender value. I.e. in count/population some stps/icbs may not have all higher ethnicity values
 select 
 distinct
 STP_CODE,
 STP_NAME,
 Ethnicity
 from (select distinct STP_CODE, STP_NAME, 'a' as tag from stp_eth_detentions) a
 cross join (select distinct Ethnicity, 'a' as tag from stp_eth_detentions) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_rates AS
 ---combine detentions and population count and calculate crude rate at stp/icb and higher ethnicity level
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "All detentions" as count_of,
 coalesce(a.count, "*") as count, ---if stp/icb and higher ethnicity combination doesn't exist in mha data then coalesce to *
 round(coalesce(b.POP, 0), 0) as population, ---if stp/icb and higher ethnicity combination doesn't exist in population data then coalesce to 0
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP