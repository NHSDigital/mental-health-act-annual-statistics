# Databricks notebook source
 %run ../Functions

# COMMAND ----------

 %sql
 create or replace temporary view stp_detentions as
 select 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END AS STP_CODE,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END AS STP_NAME,
 
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')
 group by 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_detentions_rates AS
 -- create or replace global temporary view stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 a.STP_CODE as primary_level,
 a.STP_NAME as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
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
 -- create or replace global temporary view all_stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
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
 from stp_detentions a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE

# COMMAND ----------

 %sql
 ---this is not very efficient, can we use the unsuppressed aggregate output instead?
 create or replace temporary view stp_detentions_types as
 select 
 "Detentions following admission to hospital" as count_of,
 "All" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions following admission to hospital" as count_of,
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B')
 group by 
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions following revocation of CTO" as count_of,
 "All" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('D')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions following use of Place of Safety Order" as count_of,
 "All" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions following use of Place of Safety Order" as count_of,
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
 group by 
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A', 'P')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All detentions under Part 2" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All detentions under Part 3" as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('P') 
 and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14')
 group by 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END as sub_measure,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
 from $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('A','P')
 AND LegalStatusCode in ('02', '03')
 group by 
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END,
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_detentions_types_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_detentions_types_rates AS
 -- create or replace global temporary view stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 a.STP_CODE as primary_level,
 a.STP_NAME as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 a.count_of as count_of,
 a.sub_measure as sub_measure,
 a.count as count,
 coalesce(b.POP, 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_detentions_types a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE
 group by a.STP_CODE, a.STP_NAME, a.count, a.count_of, a.sub_measure, b.POP

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_stp_detentions_types_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_stp_detentions_types_rates AS
 -- create or replace global temporary view all_stp_detentions_rates as
 select
 "All" as demographic_breakdown,
 "STP" as organisation_breakdown,
 "All prov" as primary_level,
 "All prov" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 a.count_of as count_of,
 a.sub_measure as sub_measure,
 sum(a.unsup_count) as count,
 sum(b.POP) as population,
 round((sum(a.unsup_count) / sum(b.POP)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from stp_detentions_types a
 left join $db_output.mha_stp_pop b on a.STP_CODE = b.STP_CODE
 group by a.count_of, a.sub_measure

# COMMAND ----------

 %sql
 create or replace temporary view stp_imd_detentions as
 select 
 CASE WHEN STP_CODE is null THEN "UNKNOWN" ELSE STP_CODE END AS STP_CODE,
 CASE WHEN STP_NAME is null THEN "UNKNOWN" ELSE STP_NAME END AS STP_NAME,
 CASE WHEN IMD_DECILE is null THEN "Not stated/Not known/Invalid" ELSE IMD_DECILE END AS Der_IMD_Decile,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
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
 -- create or replace global temporary view stp_imd_detentions_rates as
 select
 "IMD" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Der_IMD_Decile as secondary_level,
 l.Der_IMD_Decile as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 coalesce(b.Count, 0) as population,
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
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
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
 -- create or replace global temporary view stp_age_detentions_rates as
 select
 "Age" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Der_Age_Group as secondary_level,
 l.Der_Age_Group as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
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
 select 
 CASE WHEN STP_CODE is null then "UNKNOWN" else STP_CODE end as STP_CODE,
 CASE WHEN STP_NAME is null then "UNKNOWN" else STP_NAME end as STP_NAME,
 Der_Gender,
 suppress(COUNT(DISTINCT UniqMHActEpisodeID)) as count
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
 -- create or replace global temporary view stp_gender_detentions_rates as
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
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
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
 select 
 distinct
 STP_CODE,
 STP_NAME,
 Ethnicity
 from (select distinct STP_CODE, STP_NAME, 'a' as tag from stp_eth_detentions) a
 cross join (select distinct Ethnicity, 'a' as tag from stp_eth_detentions) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.sub_measure_lookup; 
 CREATE TABLE IF NOT EXISTS $db_output.sub_measure_lookup
 (
 measure string,
 sub_measure string
 );
 TRUNCATE TABLE $db_output.sub_measure_lookup;
 INSERT INTO $db_output.sub_measure_lookup VALUES
 ("Detentions following admission to hospital", "Section 2"),
 ("Detentions following admission to hospital", "Section 3"),
 ("Detentions following use of Place of Safety Order", "Section 135/136 to 2"),
 ("Detentions following use of Place of Safety Order", "Section 135/136 to 3"),
 ("Detentions on admission to hospital", "All detentions under Part 2"),
 ("Detentions on admission to hospital", "All detentions under Part 3"),
 ("Detentions on admission to hospital", "Section 2"),
 ("Detentions on admission to hospital", "Section 3")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_eth_sub_measure_lookup; 
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_sub_measure_lookup AS
 SELECT 
 s.measure,
 s.sub_measure,
 l.STP_CODE,
 l.STP_NAME,
 l.Ethnicity
 from (select distinct measure, sub_measure, "a" as tag from $db_output.sub_measure_lookup) s
 cross join (select distinct STP_CODE, STP_NAME, Ethnicity, "a" as tag from $db_output.stp_eth_lookup) l on s.tag = l.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_following_admission_to_hospital as
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
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B')
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_following_admission_to_hospital_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_following_admission_to_hospital_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "Detentions following admission to hospital" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions_following_admission_to_hospital a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_following_admission_to_hospital_sections as
 select 
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END as sub_measure,
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
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('B')
 group by 
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END,
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_following_admission_to_hospital_sections_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_following_admission_to_hospital_sections_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 l.measure as count_of,
 l.sub_measure as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_sub_measure_lookup l
 left join stp_eth_detentions_following_admission_to_hospital_sections a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity and l.sub_measure = a.sub_measure and l.measure = "Detentions following admission to hospital"
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown" and l.measure = "Detentions following admission to hospital"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, l.measure, l.sub_measure, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_following_revocation_of_cto as
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('D')
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_following_revocation_of_cto_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_following_revocation_of_cto_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "Detentions following revocation of CTO" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions_following_revocation_of_cto a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_following_use_of_place_of_safety_order as
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "Detentions following use of Place of Safety Order" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions_following_use_of_place_of_safety_order a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_following_use_of_place_of_safety_order_sections as
 select 
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END as sub_measure,
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
 group by 
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END,
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_sections_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_following_use_of_place_of_safety_order_sections_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 l.measure as count_of,
 l.sub_measure as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_sub_measure_lookup l
 left join stp_eth_detentions_following_use_of_place_of_safety_order_sections a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity 
 and l.sub_measure = a.sub_measure and l.measure = "Detentions following use of Place of Safety Order"
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown" and l.measure = "Detentions following use of Place of Safety Order"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, l.measure, l.sub_measure, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_on_admission_to_hospital as
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A','P')
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_on_admission_to_hospital_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_on_admission_to_hospital_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 "Detentions on admission to hospital" as count_of,
 "All" as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_lookup l
 left join stp_eth_detentions_on_admission_to_hospital a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, a.count, b.POP

# COMMAND ----------

 %sql
 create or replace temporary view stp_eth_detentions_on_admission_to_hospital_parts as
 select 
 "All detentions under Part 2" as sub_measure,
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A')
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
 UNION ALL
 select 
 "All detentions under Part 3" as sub_measure,
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
 from $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('P') 
 and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14')
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
 UNION ALL
 select 
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END as sub_measure,
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
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A')
 group by 
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END,
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
 DROP TABLE IF EXISTS $db_output.stp_eth_detentions_on_admission_to_hospital_parts_rates;
 CREATE TABLE IF NOT EXISTS $db_output.stp_eth_detentions_on_admission_to_hospital_parts_rates AS
 -- create or replace global temporary view stp_eth_detentions_rates as
 select
 "Ethnicity" as demographic_breakdown,
 "STP" as organisation_breakdown,
 l.STP_CODE as primary_level,
 l.STP_NAME as primary_level_desc,
 l.Ethnicity as secondary_level,
 l.Ethnicity as secondary_level_desc,
 l.measure as count_of,
 l.sub_measure as sub_measure,
 coalesce(a.count, "*") as count,
 round(coalesce(b.POP, 0), 0) as population,
 coalesce(round((a.count / b.POP) * 100000, 1), 0) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from $db_output.stp_eth_sub_measure_lookup l
 left join stp_eth_detentions_on_admission_to_hospital_parts a on l.STP_CODE = a.STP_CODE and l.Ethnicity = a.Ethnicity 
 and l.sub_measure = a.sub_measure and l.measure = "Detentions on admission to hospital"
 left join $db_output.stp_eth_pop b on l.STP_CODE = b.STP_CODE and l.Ethnicity = b.Ethnic_group
 where l.Ethnicity <> "Unknown" and l.measure = "Detentions on admission to hospital"
 group by l.STP_CODE, l.STP_NAME, l.Ethnicity, l.measure, l.sub_measure, a.count, b.POP