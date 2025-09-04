# Databricks notebook source
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

 %sql
 create or replace temporary view total_pop as
 select 
 'a' as tag,
 sum(Population) as population
 from $db_output.ons_pop_v2_age_gender

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_detentions as
 select 
 "All detentions" as Measure,
 "All" as measureSubcategory,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.detentions
 where MHA_Logic_Cat_full in ('A','B','C','D','P')

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ecds_detentions as
 select 'a' as tag,
 m.count + e.Count as total_count
 from mhsds_detentions m
 left join $db_output.ECDS_agg e on m.Measure = e.Measure and m.measureSubcategory = e.measureSubcategory and e.OrganisationBreakdown = "All submissions"

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_stos as
 select 
 "Short term orders" as Measure,
 "Section 136" as measureSubcategory,
 COUNT(DISTINCT UniqMHActEpisodeID) as count
 from $db_output.Short_Term_Orders
 WHERE
 MHA_Logic_Cat_full in ('E') and LegalStatusCode = '20'

 ---AM:SEP2025: Changes to include ECDS value for STO
 UNION ALL

 SELECT
 "Short term orders" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 FROM $db_output.short_term_orders A
 WHERE MHA_Logic_Cat_full in ('E','F')

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ecds_stos as
 select 'a' as tag,
 m.measureSubcategory as sub_measure,          ---AM:SEP2025: Changes to include ECDS value for STO
 m.count + e.Count as total_count
 from mhsds_stos m
 left join $db_output.ECDS_agg e on m.Measure = e.Measure and m.measureSubcategory = e.measureSubcategory and e.OrganisationBreakdown = "All submissions"

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_ctos as
 select 'a' as tag,
 COUNT(DISTINCT CTO_UniqMHActEpisodeID) as total_count
 from $db_output.CTO

# COMMAND ----------

 %sql
 create or replace temporary view mhsds_discharges as
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
 -- create or replace global temporary view all_ecds_detentions_rates as
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "All detentions" as count_of,
 "All" as sub_measure,
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
 -- create or replace global temporary view all_ecds_stos_rates as
 /*select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 "Section 136" as sub_measure,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_ecds_stos a
 left join total_pop b on a.tag = b.tag
 */
 ---AM:SEP2025: Changes to include ECDS value for STO
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Short term orders" as count_of,
 a.sub_measure as sub_measure,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from (select distinct  sub_measure, total_count, tag from  mhsds_ecds_stos) a
 left join total_pop b on a.tag = b.tag
 group by a.sub_measure

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_mhsds_ctos_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_mhsds_ctos_rates AS
 -- create or replace global temporary view all_mhsds_ctos_rates as
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Uses of CTOs" as count_of,
 "All" as sub_measure,
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
 -- create or replace global temporary view all_mhsds_discharges_rates as
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 "Discharges following detention" as count_of,
 "All" as sub_measure,
 sum(a.total_count) as count,
 sum(b.population) as population,
 round((sum(a.total_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from mhsds_discharges a
 left join total_pop b on a.tag = b.tag

# COMMAND ----------

 %sql
 create or replace temporary view detentions_types as
 select 
 "Detentions following admission to hospital" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 FROM $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('B')
 UNION ALL
 select 
 "Detentions following admission to hospital" as count_of,
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 FROM $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('B')
 group by 
 CASE WHEN LegalStatusCode = '02' THEN 'Section 2'
       WHEN LegalStatusCode = '03' THEN 'Section 3' END
 UNION ALL
 select 
 "Detentions following revocation of CTO" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('D')
 UNION ALL
 select 
 "Detentions following use of Place of Safety Order" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
 UNION ALL
 select 
 "Detentions following use of Place of Safety Order" as count_of,
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('C')
 group by 
 CASE 
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass between DATE_ADD(PrevMHAEndDate,-2) and PrevMHAEndDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '02' THEN 'Section 135/136 to 2'
 	WHEN StartDateMHActLegalStatusClass = PrevMHAStartDate AND PrevLegalStatus in ('19','20') and LegalStatusCode = '03' THEN 'Section 135/136 to 3'
 	END
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A', 'P')
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All detentions under Part 2" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE MHA_Logic_Cat_full in ('A')
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 "All detentions under Part 3" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('P') 
 and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14')
 UNION ALL
 select 
 "Detentions on admission to hospital" as count_of,
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 from $db_output.detentions
 WHERE 
 MHA_Logic_Cat_full in ('A','P')
 AND LegalStatusCode in ('02', '03')
 group by 
 CASE 
 	WHEN LegalStatusCode = '02' then 'Section 2'
 	WHEN LegalStatusCode = '03' then 'Section 3'
 	END
 ---AM:SEP2025: Changes to include ECDS value for STO    
 /*    
 UNION ALL
 SELECT
 "Short term orders" as count_of,
 "All" as sub_measure,
 COUNT(DISTINCT UniqMHActEpisodeID) as unsup_count
 FROM $db_output.short_term_orders A
 WHERE MHA_Logic_Cat_full in ('E','F')
 */

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.all_detentions_types_rates;
 CREATE TABLE IF NOT EXISTS $db_output.all_detentions_types_rates AS
 -- create or replace global temporary view all_ecds_detentions_rates as
 select
 "All" as demographic_breakdown,
 "All" as organisation_breakdown,
 "All" as primary_level,
 "All" as primary_level_desc,
 "NONE" as secondary_level,
 "NONE" as secondary_level_desc,
 a.count_of as count_of,
 a.sub_measure as sub_measure,
 sum(a.unsup_count) as count,
 sum(b.population) as population,
 round((sum(a.unsup_count) / sum(b.population)) * 100000, 1) as crude_rate_per_100000,
 0 as standardised_rate_per_100000,
 0 as CI
 from (select distinct count_of, sub_measure, unsup_count, "a" as tag from detentions_types) a
 left join total_pop b on a.tag = b.tag
 group by a.count_of, a.sub_measure