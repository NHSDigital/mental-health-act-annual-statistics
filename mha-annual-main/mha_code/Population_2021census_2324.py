# Databricks notebook source
# DBTITLE 1,census 2021 data for ethnicity (from dss_corporate.ons_2021_census)
 %sql
 create or replace temporary view census_2021_national_derived as
 select 
 ethnic_group_code,
 sex_code,
 case when age_code between 0 and 4 then '0 to 4'
      when age_code between 5 and 7 then '5 to 7'
      when age_code between 8 and 9 then '8 to 9'
      when age_code between 10 and 14 then '10 to 14'
      when age_code = 15 then '15'
      when age_code between 16 and 17 then '16 to 17'
      when age_code between 18 and 19 then '18 to 19'
      when age_code between 20 and 24 then '20 to 24'
      when age_code between 25 and 29 then '25 to 29'
      when age_code between 30 and 34 then '30 to 34'
      when age_code between 35 and 39 then '35 to 39'
      when age_code between 40 and 44 then '40 to 44'
      when age_code between 45 and 49 then '45 to 49'
      when age_code between 50 and 54 then '50 to 54'
      when age_code between 55 and 59 then '55 to 59'
      when age_code between 60 and 64 then '60 to 64'
      when age_code between 65 and 69 then '65 to 69'
      when age_code between 70 and 74 then '70 to 74'
      when age_code between 75 and 79 then '75 to 79'
      when age_code between 80 and 84 then '80 to 84'
      else '85 and over' end as Age_Group,
      sum(observation) as observation
 from dss_corporate.ons_2021_census
 where area_type_group_code = "E92" ---England grouping only
 and ons_date = (select max(ons_date) from dss_corporate.ons_2021_census where area_type_group_code = "E92") ---most recent data
 group by ethnic_group_code,
 sex_code,
 case when age_code between 0 and 4 then '0 to 4'
      when age_code between 5 and 7 then '5 to 7'
      when age_code between 8 and 9 then '8 to 9'
      when age_code between 10 and 14 then '10 to 14'
      when age_code = 15 then '15'
      when age_code between 16 and 17 then '16 to 17'
      when age_code between 18 and 19 then '18 to 19'
      when age_code between 20 and 24 then '20 to 24'
      when age_code between 25 and 29 then '25 to 29'
      when age_code between 30 and 34 then '30 to 34'
      when age_code between 35 and 39 then '35 to 39'
      when age_code between 40 and 44 then '40 to 44'
      when age_code between 45 and 49 then '45 to 49'
      when age_code between 50 and 54 then '50 to 54'
      when age_code between 55 and 59 then '55 to 59'
      when age_code between 60 and 64 then '60 to 64'
      when age_code between 65 and 69 then '65 to 69'
      when age_code between 70 and 74 then '70 to 74'
      when age_code between 75 and 79 then '75 to 79'
      when age_code between 80 and 84 then '80 to 84'
      else '85 and over' end

# COMMAND ----------

# DBTITLE 1,Create mid-year estimates table from dss_corporate.ons_population_v2 using mid-2023 estimates
 %sql

 ---2023_24_PubChanges

 create or replace temporary view ons_pop_v2_derived as
 select *,
 case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end as Der_Gender,
 case when AGE_LOWER between 0 and 4 then '0 to 4'
      when AGE_LOWER between 5 and 7 then '5 to 7'
      when AGE_LOWER between 8 and 9 then '8 to 9'
      when AGE_LOWER between 10 and 14 then '10 to 14'
      when AGE_LOWER = 15 then '15'
      when AGE_LOWER between 16 and 17 then '16 to 17'
      when AGE_LOWER between 18 and 19 then '18 to 19'
      when AGE_LOWER between 20 and 24 then '20 to 24'
      when AGE_LOWER between 25 and 29 then '25 to 29'
      when AGE_LOWER between 30 and 34 then '30 to 34'
      when AGE_LOWER between 35 and 39 then '35 to 39'
      when AGE_LOWER between 40 and 44 then '40 to 44'
      when AGE_LOWER between 45 and 49 then '45 to 49'
      when AGE_LOWER between 50 and 54 then '50 to 54'
      when AGE_LOWER between 55 and 59 then '55 to 59'
      when AGE_LOWER between 60 and 64 then '60 to 64'
      when AGE_LOWER between 65 and 69 then '65 to 69'
      when AGE_LOWER between 70 and 74 then '70 to 74'
      when AGE_LOWER between 75 and 79 then '75 to 79'
      when AGE_LOWER between 80 and 84 then '80 to 84'
      else '85 and over' end as Age_Group
 from dss_corporate.ons_population_v2
 where year_of_count = '2023' 
 and GEOGRAPHIC_GROUP_CODE = 'E92'


# COMMAND ----------

# DBTITLE 1,Age and Gender National Population (using mid-2023 estimate)
 %sql
 drop table if exists $db_output.ons_pop_v2_age_gender;
 create table if not exists $db_output.ons_pop_v2_age_gender as
 /*select
 sex_code as Der_Gender,
 Age_Group,
 SUM(observation) as Population
 from census_2021_national_derived
 group by Der_Gender, Age_Group */

 ---2023_24_PubChanges

 select
 Der_Gender,
 Age_Group,
 SUM(POPULATION_COUNT) as Population
 from ons_pop_v2_derived
 group by Der_Gender, Age_Group

# COMMAND ----------

# DBTITLE 1,Population Health
 %sql
 DROP TABLE IF EXISTS $db_output.pop_health;
 CREATE TABLE IF NOT EXISTS $db_output.pop_health AS
 select 
 CASE WHEN ethnic_group_code = 13 THEN 'British'
 WHEN ethnic_group_code = 14 THEN 'Irish'
 WHEN ethnic_group_code = 15 THEN 'Gypsy'
 WHEN ethnic_group_code = 16 THEN 'Roma'
 WHEN ethnic_group_code = 17 THEN 'Any Other White background'
 WHEN ethnic_group_code = 11 THEN 'White and Black Caribbean'
 WHEN ethnic_group_code = 10 THEN 'White and Black African'
 WHEN ethnic_group_code = 9 THEN 'White and Asian'
 WHEN ethnic_group_code = 12 THEN 'Any Other Mixed background'
 WHEN ethnic_group_code = 3 THEN 'Indian'
 WHEN ethnic_group_code = 4 THEN 'Pakistani'
 WHEN ethnic_group_code = 1 THEN 'Bangladeshi'
 WHEN ethnic_group_code = 5 THEN 'Any Other Asian background'
 WHEN ethnic_group_code = 7 THEN 'Caribbean'
 WHEN ethnic_group_code = 6 THEN 'African'
 WHEN ethnic_group_code = 8 THEN 'Any Other Black background'
 WHEN ethnic_group_code = 2 THEN 'Chinese'
 WHEN ethnic_group_code = 18 THEN 'Arab'
 WHEN ethnic_group_code = 19 THEN 'Any Other Ethnic Group'
 ELSE 'Unknown' END as ethnic_group_formatted, ---get everything after : in description (i.e. lower ethnic group)
 a.description as Ethnic_group,
 ethnic_group_code,
 CASE WHEN ethnic_group_code = 13 THEN 'A'
 WHEN ethnic_group_code = 14 THEN 'B'
 WHEN ethnic_group_code = 17 THEN 'C'
 WHEN ethnic_group_code = 11 THEN 'D'
 WHEN ethnic_group_code = 10 THEN 'E'
 WHEN ethnic_group_code = 9 THEN 'F'
 WHEN ethnic_group_code = 12 THEN 'G'
 WHEN ethnic_group_code = 3 THEN 'H'
 WHEN ethnic_group_code = 4 THEN 'J'
 WHEN ethnic_group_code = 1 THEN 'K'
 WHEN ethnic_group_code = 5 THEN 'L'
 WHEN ethnic_group_code = 7 THEN 'M'
 WHEN ethnic_group_code = 6 THEN 'N'
 WHEN ethnic_group_code = 8 THEN 'P'
 WHEN ethnic_group_code = 2 THEN 'R' 
 WHEN ethnic_group_code = 19 THEN 'S'
 ELSE 'Unknown' END AS LowerEthnicityCode,
 CASE WHEN ethnic_group_code IN (13, 14, 17, 15, 16) THEN 'White'
      WHEN ethnic_group_code IN (11, 10, 9, 12) THEN 'Mixed'
      WHEN ethnic_group_code IN (3, 4, 1, 5) THEN 'Asian or Asian British'
      WHEN ethnic_group_code IN (7, 6, 8) THEN 'Black or Black British'
      WHEN ethnic_group_code IN (2, 19, 18) THEN 'Other Ethnic Groups'
      ELSE 'Unknown' END AS UpperEthnicity,
 sex_code as Der_Gender,
 Age_Group,
 observation as Population
 from census_2021_national_derived c
 left join dss_corporate.ONS_2021_census_lookup a on c.ethnic_group_code = a.code and a.field = "ethnic_group_code"
 where ethnic_group_code != -8 ---exclude does not apply ethnicity
 order by ethnic_group_formatted, der_gender desc, Age_Group

# COMMAND ----------

# DBTITLE 1,LSOA (& IMD) using mid-2022 estimate
 %sql
 ---2023_24_PubChanges

 create or replace temp view lsoa_census_2021_imd_decile as
 select
 c.GEOGRAPHIC_SUBGROUP_CODE,
 CASE
 WHEN r.DECI_IMD = 10 THEN '01 Least deprived'
 WHEN r.DECI_IMD = 9 THEN '02 Less deprived'
 WHEN r.DECI_IMD = 8 THEN '03 Less deprived'
 WHEN r.DECI_IMD = 7 THEN '04 Less deprived'
 WHEN r.DECI_IMD = 6 THEN '05 Less deprived'
 WHEN r.DECI_IMD = 5 THEN '06 More deprived'
 WHEN r.DECI_IMD = 4 THEN '07 More deprived'
 WHEN r.DECI_IMD = 3 THEN '08 More deprived'
 WHEN r.DECI_IMD = 2 THEN '09 More deprived'
 WHEN r.DECI_IMD = 1 THEN '10 Most deprived'
 ELSE 'Not stated/Not known/Invalid'
 END AS IMD_Decile,
 c.POPULATION_COUNT as observation
 FROM dss_corporate.ons_population_v2 c
 LEFT JOIN DSS_CORPORATE.ENGLISH_INDICES_OF_DEP_V02 r on c.GEOGRAPHIC_SUBGROUP_CODE = r.LSOA_CODE_2011 AND c.GEOGRAPHIC_GROUP_CODE = "E01" AND r.IMD_YEAR = '2019'
 WHERE c.GEOGRAPHIC_GROUP_CODE= "E01"
 and year_of_count = '2022'

# COMMAND ----------

# DBTITLE 1,National IMD Population
 %sql
 DROP TABLE IF EXISTS $db_output.imd_pop;
 CREATE TABLE IF NOT EXISTS $db_output.imd_pop AS
 SELECT IMD_Decile,
 SUM(observation) as Count
 from lsoa_census_2021_imd_decile
 group by IMD_Decile

# COMMAND ----------

# DBTITLE 1,Sub ICB (ethnicity pop) using census 2021
 %sql
 ---2023_24_PubChanges
 create or replace temporary view census_2021_sub_icb_derived_ethnicity as
 select 
 od.DH_GEOGRAPHY_CODE as ods_sub_icb_code,
 c.ethnic_group_code,
 a.description as ethnic_description,
 c.sex_code,
 c.age_code,
 c.observation
 from dss_corporate.ons_2021_census c
 left join (SELECT *
 FROM dss_corporate.ONS_CHD_GEO_EQUIVALENTS AS od
 WHERE DATE_OF_OPERATION = (SELECT MAX(DATE_OF_OPERATION) FROM dss_corporate.ONS_CHD_GEO_EQUIVALENTS AS od2 WHERE od.GEOGRAPHY_CODE = od2.GEOGRAPHY_CODE)) od
 on c.area_type_code = od.GEOGRAPHY_CODE and area_type_group_code = "E38"
 left join dss_corporate.ONS_2021_census_lookup a on c.ethnic_group_code = a.code and a.field = "ethnic_group_code"
 where area_type_group_code = "E38" ---Sub ICB grouping only
 and ethnic_group_code != -8 ---exclude does not apply ethnicity
 and ons_date = (select max(ons_date) from dss_corporate.ons_2021_census where area_type_group_code = "E38") ---most recent data

# COMMAND ----------

# DBTITLE 1,Sub ICB Pop using mid-2022 estimate
 %sql
 ---2023_24_PubChanges
 create or replace temporary view census_2021_sub_icb_derived as
 select 
 COALESCE(o.DH_GEOGRAPHY_CODE, od.DH_GEOGRAPHY_CODE) as ods_sub_icb_code,
 case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end as sex_code,
 c.AGE_LOWER as age_code,
 c.POPULATION_COUNT as observation
 from dss_corporate.ons_population_v2 c
 left join (SELECT * FROM dss_corporate.ONS_CHD_GEO_EQUIVALENTS AS od
 WHERE DATE_OF_OPERATION = (SELECT MAX(DATE_OF_OPERATION) FROM dss_corporate.ONS_CHD_GEO_EQUIVALENTS AS od2 WHERE od.GEOGRAPHY_CODE = od2.GEOGRAPHY_CODE)) od
 on c.GEOGRAPHIC_SUBGROUP_CODE = od.GEOGRAPHY_CODE and GEOGRAPHIC_GROUP_CODE = "E38"
 where GEOGRAPHIC_GROUP_CODE = "E38" ---Sub ICB grouping only
 and year_of_count = '2022'

# COMMAND ----------

# DBTITLE 1,Sub ICB (CCG) Pop Data
 %sql
 drop table if exists $db_output.mha_ccg_pop;   
 create table if not exists $db_output.mha_ccg_pop as

 select 
 ods_sub_icb_code as CCG_CODE,
 sum(observation) as POP
 from census_2021_sub_icb_derived
 group by ods_sub_icb_code

# COMMAND ----------

# DBTITLE 1,ICB (STP) Pop data
 %sql
 drop table if exists $db_output.mha_stp_pop;
 create table if not exists $db_output.mha_stp_pop as
 select
 stp.STP_CODE,
 stp.STP_NAME,
 sum(ccg.POP) as POP
 from $db_output.mha_ccg_pop ccg
 inner join $db_output.mha_stp_mapping stp on ccg.CCG_CODE = stp.CCG_CODE
 group by stp.STP_CODE, stp.STP_NAME

# COMMAND ----------

# DBTITLE 1,SubICB (CCG) Demographic Pop data  (using census 2021 for ethnicity)
 %sql
 drop table if exists $db_output.mha_ccg_ethpop;   
 create table if not exists $db_output.mha_ccg_ethpop as

 select 
 ods_sub_icb_code as CCG21_CODE,
 CASE WHEN ethnic_group_code in (1, 2, 3, 4, 5) then "Asian or Asian British"
      WHEN ethnic_group_code in (6, 7, 8) then "Black or Black British"
      WHEN ethnic_group_code in (9, 10, 11, 12) then "Mixed"
      WHEN ethnic_group_code in (13, 14, 15, 16, 17) then "White"
      WHEN ethnic_group_code in (18, 19) then "Other Ethnic Groups"
      ELSE "Unknown" END as Ethnic_group,
 substring_index(ethnic_description, ': ', -1) as Ethnicity, ---get everything after : in description (i.e. lower ethnic group) 
 sex_code as Sex,
 age_code as Age,
 sum(observation) as POP
 from census_2021_sub_icb_derived_ethnicity
 group by ods_sub_icb_code,
 CASE WHEN ethnic_group_code in (1, 2, 3, 4, 5) then "Asian or Asian British"
      WHEN ethnic_group_code in (6, 7, 8) then "Black or Black British"
      WHEN ethnic_group_code in (9, 10, 11, 12) then "Mixed"
      WHEN ethnic_group_code in (13, 14, 15, 16, 17) then "White"
      WHEN ethnic_group_code in (18, 19) then "Other Ethnic Groups"
      ELSE "Unknown" END,
 substring_index(ethnic_description, ': ', -1),
 sex_code, age_code

# COMMAND ----------

# DBTITLE 1,STP Ethnicity Pop data
 %sql
 drop table if exists $db_output.mha_stp_ethpop;
 create table if not exists $db_output.mha_stp_ethpop as
 select
 stp.STP_CODE,
 stp.STP_NAME,
 ccg.Ethnic_group,
 ccg.Ethnicity,
 ccg.Sex, 
 ccg.Age, 
 sum(ccg.POP) as POP
 from $db_output.mha_ccg_ethpop ccg
 inner join $db_output.mha_stp_mapping stp on ccg.CCG21_CODE = stp.CCG_CODE
 group by stp.STP_CODE, stp.STP_NAME, ccg.Ethnic_group, ccg.Ethnicity, ccg.Sex, ccg.Age

# COMMAND ----------

# DBTITLE 1,STP Age & Gender Pop data (using mid-2022 estimate)
 %sql
 drop table if exists $db_output.mha_stp_age_gender_pop;
 create table if not exists $db_output.mha_stp_age_gender_pop as
 select
 stp.STP_CODE,
 stp.STP_NAME,
 ccg.sex_code as Sex, 
 ccg.age_code as Age, 
 sum(ccg.observation) as POP
 from census_2021_sub_icb_derived ccg
 inner join $db_output.mha_stp_mapping stp on ccg.ods_sub_icb_code = stp.CCG_CODE
 group by stp.STP_CODE, stp.STP_NAME,  ccg.sex_code, ccg.age_code


# COMMAND ----------

# DBTITLE 1,STP; Age Pop data
 %sql
 drop table if exists $db_output.stp_age_pop;
 create table if not exists $db_output.stp_age_pop as
 select
 STP_CODE,
 STP_NAME,
 case 
     when Age between 0 and 15 then '15 and under'
     when Age between 16 and 17 then '16 to 17'
     when Age between 18 and 34 then '18 to 34'
     when Age between 35 and 49 then '35 to 49'
     when Age between 50 and 64 then '50 to 64'
     when Age >= 65 then '65 and over'
     end as Der_Age_Group,
 sum(POP) as POP
 from $db_output.mha_stp_age_gender_pop
 group by STP_CODE, STP_NAME,
 case 
     when Age between 0 and 15 then '15 and under'
     when Age between 16 and 17 then '16 to 17'
     when Age between 18 and 34 then '18 to 34'
     when Age between 35 and 49 then '35 to 49'
     when Age between 50 and 64 then '50 to 64'
     when Age >= 65 then '65 and over'
     end

# COMMAND ----------

# DBTITLE 1,STP; Gender Pop data
 %sql
 drop table if exists $db_output.stp_gender_pop;
 create table if not exists $db_output.stp_gender_pop as
 select
 STP_CODE,
 STP_NAME,
 Sex,
 sum(POP) as POP
 from $db_output.mha_stp_age_gender_pop
 group by STP_CODE, STP_NAME, Sex

# COMMAND ----------

# DBTITLE 1,STP; Higher Ethnicity Pop data
 %sql
 drop table if exists $db_output.stp_eth_pop;
 create table if not exists $db_output.stp_eth_pop as
 select
 STP_CODE,
 STP_NAME,
 Ethnic_group,
 sum(POP) as POP
 from $db_output.mha_stp_ethpop
 group by STP_CODE, STP_NAME, Ethnic_group

# COMMAND ----------

 %sql
 create or replace temporary view lsoa21_ons_ccg as
 select distinct LSOA11, CCG from dss_corporate.postcode 
 where 
 LEFT(LSOA11, 3) = "E01"
 and (RECORD_END_DATE >= '$rp_enddate' OR RECORD_END_DATE IS NULL)	
 and RECORD_START_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 create or replace temporary view lsoa21_stp_imd_mapped as
 select 
 a.GEOGRAPHIC_SUBGROUP_CODE,
 t.CCG,
 s.STP_CODE,
 a.IMD_Decile,
 a.observation
 from lsoa_census_2021_imd_decile a
 left join lsoa21_ons_ccg t on a.GEOGRAPHIC_SUBGROUP_CODE = t.LSOA11
 left join $db_output.mha_stp_mapping s on t.CCG = s.CCG_CODE

# COMMAND ----------

# DBTITLE 1,STP; IMD Decile Pop data
 %sql
 DROP TABLE IF EXISTS $db_output.stp_imd_pop;
 CREATE TABLE IF NOT EXISTS $db_output.stp_imd_pop
 select STP_CODE as STP, IMD_Decile, SUM(observation) as Count
 from lsoa21_stp_imd_mapped
 group by STP_Code, IMD_Decile
