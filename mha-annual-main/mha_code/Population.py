# Databricks notebook source
db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

import pandas as pd
import io

# COMMAND ----------

# DBTITLE 1,Create mid-year estimates table from $reference_data.ons_population_v2 <-- change year_of_count depending on pub year
 %sql
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
 from $reference_data.ons_population_v2
 where year_of_count = '2020' ---needs to change depending on the year being ran
 and GEOGRAPHIC_GROUP_CODE = 'E38'
 and trim(RECORD_TYPE) = 'E'

# COMMAND ----------

# DBTITLE 1,Age and Gender National Population
 %sql
 drop table if exists $db_output.ons_pop_v2_age_gender;
 create table if not exists $db_output.ons_pop_v2_age_gender as
 select
 Der_Gender,
 Age_Group,
 SUM(POPULATION_COUNT) as Population
 from ons_pop_v2_derived
 group by Der_Gender, Age_Group

# COMMAND ----------

# DBTITLE 1,Create Standardisation Population Table
 %sql
 -- TRUNCATE TABLE $db_output.pop_health;
 -- REFRESH TABLE $db_output.pop_health;
 DROP TABLE IF EXISTS $db_output.pop_health;

 --------------- This is the new table format _____________________________
 CREATE TABLE IF NOT EXISTS $db_output.pop_health
 (
        ethnic_group_formatted    STRING,
        Ethnic_group              STRING,
        Der_Gender                STRING,
        Age_group                 STRING,
        Population                INT
               )
  using delta

# COMMAND ----------

 %sql
 truncate table $db_output.pop_health

# COMMAND ----------

# DBTITLE 1,Census2011 estimates for ethnic group by age and sex
 %md
 sourced from: https://www.nomisweb.co.uk/census/2011/LC2101EW/view/2092957699?rows=c_ethpuk11&cols=c_sex

# COMMAND ----------

# DBTITLE 1,Population Health
###changing temp. Gender field to be 1 or 2 rather than MALE/FEMALE
Population_Data = [
  ['Bangladeshi','Asian/Asian British: Bangladeshi','2','0 to 4','24692'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','5 to 7','14904'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','8 to 9','9569'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','10 to 14','21449'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','15','4126'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','16 to 17','7471'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','18 to 19','6987'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','20 to 24','20475'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','25 to 29','23400'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','30 to 34','21934'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','35 to 39','17274'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','40 to 44','10414'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','45 to 49','6457'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','50 to 54','5056'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','55 to 59','5533'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','60 to 64','3905'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','65 to 69','2635'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','70 to 74','2693'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','75 to 79','1477'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','80 to 84','555'],
['Bangladeshi','Asian/Asian British: Bangladeshi','2','85 and over','257'],
['Chinese','Other ethnic group: Chinese','2','0 to 4','8640'],
['Chinese','Other ethnic group: Chinese','2','5 to 7','4035'],
['Chinese','Other ethnic group: Chinese','2','8 to 9','2346'],
['Chinese','Other ethnic group: Chinese','2','10 to 14','7041'],
['Chinese','Other ethnic group: Chinese','2','15','1964'],
['Chinese','Other ethnic group: Chinese','2','16 to 17','5333'],
['Chinese','Other ethnic group: Chinese','2','18 to 19','9228'],
['Chinese','Other ethnic group: Chinese','2','20 to 24','40025'],
['Chinese','Other ethnic group: Chinese','2','25 to 29','23839'],
['Chinese','Other ethnic group: Chinese','2','30 to 34','19916'],
['Chinese','Other ethnic group: Chinese','2','35 to 39','15772'],
['Chinese','Other ethnic group: Chinese','2','40 to 44','13284'],
['Chinese','Other ethnic group: Chinese','2','45 to 49','12342'],
['Chinese','Other ethnic group: Chinese','2','50 to 54','10538'],
['Chinese','Other ethnic group: Chinese','2','55 to 59','9082'],
['Chinese','Other ethnic group: Chinese','2','60 to 64','6670'],
['Chinese','Other ethnic group: Chinese','2','65 to 69','3321'],
['Chinese','Other ethnic group: Chinese','2','70 to 74','2808'],
['Chinese','Other ethnic group: Chinese','2','75 to 79','2037'],
['Chinese','Other ethnic group: Chinese','2','80 to 84','1227'],
['Chinese','Other ethnic group: Chinese','2','85 and over','931'],
['Indian','Asian/Asian British: Indian','2','0 to 4','47712'],
['Indian','Asian/Asian British: Indian','2','5 to 7','24526'],
['Indian','Asian/Asian British: Indian','2','8 to 9','14523'],
['Indian','Asian/Asian British: Indian','2','10 to 14','36151'],
['Indian','Asian/Asian British: Indian','2','15','7373'],
['Indian','Asian/Asian British: Indian','2','16 to 17','15038'],
['Indian','Asian/Asian British: Indian','2','18 to 19','17350'],
['Indian','Asian/Asian British: Indian','2','20 to 24','53318'],
['Indian','Asian/Asian British: Indian','2','25 to 29','75048'],
['Indian','Asian/Asian British: Indian','2','30 to 34','76059'],
['Indian','Asian/Asian British: Indian','2','35 to 39','59072'],
['Indian','Asian/Asian British: Indian','2','40 to 44','49926'],
['Indian','Asian/Asian British: Indian','2','45 to 49','43753'],
['Indian','Asian/Asian British: Indian','2','50 to 54','41844'],
['Indian','Asian/Asian British: Indian','2','55 to 59','36151'],
['Indian','Asian/Asian British: Indian','2','60 to 64','27778'],
['Indian','Asian/Asian British: Indian','2','65 to 69','19491'],
['Indian','Asian/Asian British: Indian','2','70 to 74','16757'],
['Indian','Asian/Asian British: Indian','2','75 to 79','11782'],
['Indian','Asian/Asian British: Indian','2','80 to 84','7004'],
['Indian','Asian/Asian British: Indian','2','85 and over','4745'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','0 to 4','31723'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','5 to 7','17483'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','8 to 9','10085'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','10 to 14','25473'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','15','4964'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','16 to 17','10138'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','18 to 19','10764'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','20 to 24','33331'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','25 to 29','40849'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','30 to 34','47615'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','35 to 39','49740'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','40 to 44','38381'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','45 to 49','26945'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','50 to 54','21166'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','55 to 59','16898'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','60 to 64','13848'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','65 to 69','8075'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','70 to 74','5578'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','75 to 79','3575'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','80 to 84','2086'],
['Any Other Asian Background','Asian/Asian British: Other Asian','2','85 and over','1350'],
['Pakistani','Asian/Asian British: Pakistani','2','0 to 4','63090'],
['Pakistani','Asian/Asian British: Pakistani','2','5 to 7','36053'],
['Pakistani','Asian/Asian British: Pakistani','2','8 to 9','22399'],
['Pakistani','Asian/Asian British: Pakistani','2','10 to 14','49072'],
['Pakistani','Asian/Asian British: Pakistani','2','15','9092'],
['Pakistani','Asian/Asian British: Pakistani','2','16 to 17','17988'],
['Pakistani','Asian/Asian British: Pakistani','2','18 to 19','17307'],
['Pakistani','Asian/Asian British: Pakistani','2','20 to 24','46711'],
['Pakistani','Asian/Asian British: Pakistani','2','25 to 29','56889'],
['Pakistani','Asian/Asian British: Pakistani','2','30 to 34','54965'],
['Pakistani','Asian/Asian British: Pakistani','2','35 to 39','41815'],
['Pakistani','Asian/Asian British: Pakistani','2','40 to 44','34300'],
['Pakistani','Asian/Asian British: Pakistani','2','45 to 49','21386'],
['Pakistani','Asian/Asian British: Pakistani','2','50 to 54','20406'],
['Pakistani','Asian/Asian British: Pakistani','2','55 to 59','16489'],
['Pakistani','Asian/Asian British: Pakistani','2','60 to 64','10822'],
['Pakistani','Asian/Asian British: Pakistani','2','65 to 69','7356'],
['Pakistani','Asian/Asian British: Pakistani','2','70 to 74','7035'],
['Pakistani','Asian/Asian British: Pakistani','2','75 to 79','5264'],
['Pakistani','Asian/Asian British: Pakistani','2','80 to 84','2708'],
['Pakistani','Asian/Asian British: Pakistani','2','85 and over','1342'],
['Asian or Asian British','Asian/Asian British: Total','2','0 to 4','175857'],
['Asian or Asian British','Asian/Asian British: Total','2','5 to 7','97001'],
['Asian or Asian British','Asian/Asian British: Total','2','8 to 9','58922'],
['Asian or Asian British','Asian/Asian British: Total','2','10 to 14','139186'],
['Asian or Asian British','Asian/Asian British: Total','2','15','27519'],
['Asian or Asian British','Asian/Asian British: Total','2','16 to 17','55968'],
['Asian or Asian British','Asian/Asian British: Total','2','18 to 19','61636'],
['Asian or Asian British','Asian/Asian British: Total','2','20 to 24','193860'],
['Asian or Asian British','Asian/Asian British: Total','2','25 to 29','220025'],
['Asian or Asian British','Asian/Asian British: Total','2','30 to 34','220489'],
['Asian or Asian British','Asian/Asian British: Total','2','35 to 39','183673'],
['Asian or Asian British','Asian/Asian British: Total','2','40 to 44','146305'],
['Asian or Asian British','Asian/Asian British: Total','2','45 to 49','110883'],
['Asian or Asian British','Asian/Asian British: Total','2','50 to 54','99010'],
['Asian or Asian British','Asian/Asian British: Total','2','55 to 59','84153'],
['Asian or Asian British','Asian/Asian British: Total','2','60 to 64','63023'],
['Asian or Asian British','Asian/Asian British: Total','2','65 to 69','40878'],
['Asian or Asian British','Asian/Asian British: Total','2','70 to 74','34871'],
['Asian or Asian British','Asian/Asian British: Total','2','75 to 79','24135'],
['Asian or Asian British','Asian/Asian British: Total','2','80 to 84','13580'],
['Asian or Asian British','Asian/Asian British: Total','2','85 and over','8625'],
['African','Black/African/Caribbean/Black British: African','2','0 to 4','53213'],
['African','Black/African/Caribbean/Black British: African','2','5 to 7','29045'],
['African','Black/African/Caribbean/Black British: African','2','8 to 9','15942'],
['African','Black/African/Caribbean/Black British: African','2','10 to 14','40181'],
['African','Black/African/Caribbean/Black British: African','2','15','8232'],
['African','Black/African/Caribbean/Black British: African','2','16 to 17','16099'],
['African','Black/African/Caribbean/Black British: African','2','18 to 19','16689'],
['African','Black/African/Caribbean/Black British: African','2','20 to 24','41140'],
['African','Black/African/Caribbean/Black British: African','2','25 to 29','46030'],
['African','Black/African/Caribbean/Black British: African','2','30 to 34','53970'],
['African','Black/African/Caribbean/Black British: African','2','35 to 39','50058'],
['African','Black/African/Caribbean/Black British: African','2','40 to 44','46289'],
['African','Black/African/Caribbean/Black British: African','2','45 to 49','33934'],
['African','Black/African/Caribbean/Black British: African','2','50 to 54','21290'],
['African','Black/African/Caribbean/Black British: African','2','55 to 59','11577'],
['African','Black/African/Caribbean/Black British: African','2','60 to 64','7455'],
['African','Black/African/Caribbean/Black British: African','2','65 to 69','5537'],
['African','Black/African/Caribbean/Black British: African','2','70 to 74','4229'],
['African','Black/African/Caribbean/Black British: African','2','75 to 79','2051'],
['African','Black/African/Caribbean/Black British: African','2','80 to 84','947'],
['African','Black/African/Caribbean/Black British: African','2','85 and over','477'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','0 to 4','14523'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','5 to 7','9038'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','8 to 9','6085'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','10 to 14','17293'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','15','3842'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','16 to 17','7850'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','18 to 19','8120'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','20 to 24','21001'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','25 to 29','21037'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','30 to 34','19863'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','35 to 39','21189'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','40 to 44','30784'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','45 to 49','37184'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','50 to 54','27000'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','55 to 59','16480'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','60 to 64','10506'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','65 to 69','11578'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','70 to 74','13120'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','75 to 79','10348'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','80 to 84','6052'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','2','85 and over','3382'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','0 to 4','18760'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','5 to 7','10412'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','8 to 9','6191'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','10 to 14','14212'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','15','2590'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','16 to 17','4890'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','18 to 19','4134'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','20 to 24','9214'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','25 to 29','9739'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','30 to 34','9154'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','35 to 39','9219'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','40 to 44','11634'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','45 to 49','11779'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','50 to 54','6224'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','55 to 59','2890'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','60 to 64','1865'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','65 to 69','1592'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','70 to 74','1466'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','75 to 79','1006'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','80 to 84','515'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','2','85 and over','283'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','0 to 4','86496'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','5 to 7','48495'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','8 to 9','28218'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','10 to 14','71686'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','15','14664'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','16 to 17','28839'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','18 to 19','28943'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','20 to 24','71355'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','25 to 29','76806'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','30 to 34','82987'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','35 to 39','80466'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','40 to 44','88707'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','45 to 49','82897'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','50 to 54','54514'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','55 to 59','30947'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','60 to 64','19826'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','65 to 69','18707'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','70 to 74','18815'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','75 to 79','13405'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','80 to 84','7514'],
['Black or Black British','Black/African/Caribbean/Black British: Total','2','85 and over','4142'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','0 to 4','24328'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','5 to 7','10807'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','8 to 9','5762'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','10 to 14','13927'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','15','2574'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','16 to 17','5146'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','18 to 19','5075'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','20 to 24','13254'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','25 to 29','13877'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','30 to 34','12029'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','35 to 39','9221'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','40 to 44','7692'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','45 to 49','6334'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','50 to 54','4489'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','55 to 59','3015'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','60 to 64','2460'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','65 to 69','1832'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','70 to 74','1296'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','75 to 79','968'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','80 to 84','656'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','2','85 and over','498'],
['Mixed','Mixed/multiple ethnic group: Total','2','0 to 4','105875'],
['Mixed','Mixed/multiple ethnic group: Total','2','5 to 7','50318'],
['Mixed','Mixed/multiple ethnic group: Total','2','8 to 9','27830'],
['Mixed','Mixed/multiple ethnic group: Total','2','10 to 14','67841'],
['Mixed','Mixed/multiple ethnic group: Total','2','15','12789'],
['Mixed','Mixed/multiple ethnic group: Total','2','16 to 17','25173'],
['Mixed','Mixed/multiple ethnic group: Total','2','18 to 19','25062'],
['Mixed','Mixed/multiple ethnic group: Total','2','20 to 24','57040'],
['Mixed','Mixed/multiple ethnic group: Total','2','25 to 29','48200'],
['Mixed','Mixed/multiple ethnic group: Total','2','30 to 34','38894'],
['Mixed','Mixed/multiple ethnic group: Total','2','35 to 39','31924'],
['Mixed','Mixed/multiple ethnic group: Total','2','40 to 44','29460'],
['Mixed','Mixed/multiple ethnic group: Total','2','45 to 49','24968'],
['Mixed','Mixed/multiple ethnic group: Total','2','50 to 54','16165'],
['Mixed','Mixed/multiple ethnic group: Total','2','55 to 59','10005'],
['Mixed','Mixed/multiple ethnic group: Total','2','60 to 64','7719'],
['Mixed','Mixed/multiple ethnic group: Total','2','65 to 69','5762'],
['Mixed','Mixed/multiple ethnic group: Total','2','70 to 74','4311'],
['Mixed','Mixed/multiple ethnic group: Total','2','75 to 79','3420'],
['Mixed','Mixed/multiple ethnic group: Total','2','80 to 84','2372'],
['Mixed','Mixed/multiple ethnic group: Total','2','85 and over','2087'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','0 to 4','31936'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','5 to 7','15074'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','8 to 9','8169'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','10 to 14','18449'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','15','3352'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','16 to 17','6660'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','18 to 19','6344'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','20 to 24','14556'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','25 to 29','11909'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','30 to 34','9701'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','35 to 39','8050'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','40 to 44','7597'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','45 to 49','6351'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','50 to 54','3967'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','55 to 59','2715'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','60 to 64','2206'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','65 to 69','1526'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','70 to 74','1065'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','75 to 79','894'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','80 to 84','603'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','2','85 and over','609'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','0 to 4','17389'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','5 to 7','7706'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','8 to 9','3839'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','10 to 14','8791'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','15','1492'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','16 to 17','2765'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','18 to 19','2584'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','20 to 24','6188'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','25 to 29','5789'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','30 to 34','5578'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','35 to 39','4702'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','40 to 44','4028'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','45 to 49','3321'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','50 to 54','2423'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','55 to 59','1591'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','60 to 64','946'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','65 to 69','652'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','70 to 74','399'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','75 to 79','254'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','80 to 84','190'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','2','85 and over','128'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','0 to 4','32222'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','5 to 7','16731'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','8 to 9','10060'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','10 to 14','26674'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','15','5371'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','16 to 17','10602'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','18 to 19','11059'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','20 to 24','23042'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','25 to 29','16625'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','30 to 34','11586'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','35 to 39','9951'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','40 to 44','10143'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','45 to 49','8962'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','50 to 54','5286'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','55 to 59','2684'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','60 to 64','2107'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','65 to 69','1752'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','70 to 74','1551'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','75 to 79','1304'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','80 to 84','923'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','2','85 and over','852'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','0 to 4','11753'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','5 to 7','6076'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','8 to 9','3802'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','10 to 14','9379'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','15','1927'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','16 to 17','4039'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','18 to 19','4355'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','20 to 24','12883'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','25 to 29','16314'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','30 to 34','16293'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','35 to 39','14549'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','40 to 44','12333'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','45 to 49','9927'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','50 to 54','8040'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','55 to 59','6383'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','60 to 64','4764'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','65 to 69','3238'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','70 to 74','2466'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','75 to 79','1710'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','80 to 84','1208'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','2','85 and over','902'],
['Arab','Other ethnic group: Arab','2','0 to 4','11917'],
['Arab','Other ethnic group: Arab','2','5 to 7','6264'],
['Arab','Other ethnic group: Arab','2','8 to 9','3586'],
['Arab','Other ethnic group: Arab','2','10 to 14','7196'],
['Arab','Other ethnic group: Arab','2','15','1248'],
['Arab','Other ethnic group: Arab','2','16 to 17','2582'],
['Arab','Other ethnic group: Arab','2','18 to 19','3111'],
['Arab','Other ethnic group: Arab','2','20 to 24','8517'],
['Arab','Other ethnic group: Arab','2','25 to 29','10693'],
['Arab','Other ethnic group: Arab','2','30 to 34','9616'],
['Arab','Other ethnic group: Arab','2','35 to 39','7606'],
['Arab','Other ethnic group: Arab','2','40 to 44','5848'],
['Arab','Other ethnic group: Arab','2','45 to 49','4273'],
['Arab','Other ethnic group: Arab','2','50 to 54','3230'],
['Arab','Other ethnic group: Arab','2','55 to 59','2253'],
['Arab','Other ethnic group: Arab','2','60 to 64','1673'],
['Arab','Other ethnic group: Arab','2','65 to 69','1075'],
['Arab','Other ethnic group: Arab','2','70 to 74','796'],
['Arab','Other ethnic group: Arab','2','75 to 79','629'],
['Arab','Other ethnic group: Arab','2','80 to 84','326'],
['Arab','Other ethnic group: Arab','2','85 and over','209'],
['Other Ethnic Groups','Other ethnic group: Total','2','0 to 4','23670'],
['Other Ethnic Groups','Other ethnic group: Total','2','5 to 7','12340'],
['Other Ethnic Groups','Other ethnic group: Total','2','8 to 9','7388'],
['Other Ethnic Groups','Other ethnic group: Total','2','10 to 14','16575'],
['Other Ethnic Groups','Other ethnic group: Total','2','15','3175'],
['Other Ethnic Groups','Other ethnic group: Total','2','16 to 17','6621'],
['Other Ethnic Groups','Other ethnic group: Total','2','18 to 19','7466'],
['Other Ethnic Groups','Other ethnic group: Total','2','20 to 24','21400'],
['Other Ethnic Groups','Other ethnic group: Total','2','25 to 29','27007'],
['Other Ethnic Groups','Other ethnic group: Total','2','30 to 34','25909'],
['Other Ethnic Groups','Other ethnic group: Total','2','35 to 39','22155'],
['Other Ethnic Groups','Other ethnic group: Total','2','40 to 44','18181'],
['Other Ethnic Groups','Other ethnic group: Total','2','45 to 49','14200'],
['Other Ethnic Groups','Other ethnic group: Total','2','50 to 54','11270'],
['Other Ethnic Groups','Other ethnic group: Total','2','55 to 59','8636'],
['Other Ethnic Groups','Other ethnic group: Total','2','60 to 64','6437'],
['Other Ethnic Groups','Other ethnic group: Total','2','65 to 69','4313'],
['Other Ethnic Groups','Other ethnic group: Total','2','70 to 74','3262'],
['Other Ethnic Groups','Other ethnic group: Total','2','75 to 79','2339'],
['Other Ethnic Groups','Other ethnic group: Total','2','80 to 84','1534'],
['Other Ethnic Groups','Other ethnic group: Total','2','85 and over','1111'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','0 to 4','1143856'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','5 to 7','648624'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','8 to 9','414482'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','10 to 14','1156256'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','15','247240'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','16 to 17','502460'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','18 to 19','528247'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','20 to 24','1311726'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','25 to 29','1230196'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','30 to 34','1174753'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','35 to 39','1313709'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','40 to 44','1562891'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','45 to 49','1632855'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','50 to 54','1453345'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','55 to 59','1315223'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','60 to 64','1456325'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','65 to 69','1170625'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','70 to 74','971097'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','75 to 79','834504'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','80 to 84','685381'],
['British','White: English/Welsh/Scottish/Northern Irish/British','2','85 and over','755362'],
['Gypsy','White: Gypsy or Irish Traveller','2','0 to 4','2831'],
['Gypsy','White: Gypsy or Irish Traveller','2','5 to 7','1611'],
['Gypsy','White: Gypsy or Irish Traveller','2','8 to 9','959'],
['Gypsy','White: Gypsy or Irish Traveller','2','10 to 14','2682'],
['Gypsy','White: Gypsy or Irish Traveller','2','15','518'],
['Gypsy','White: Gypsy or Irish Traveller','2','16 to 17','1041'],
['Gypsy','White: Gypsy or Irish Traveller','2','18 to 19','853'],
['Gypsy','White: Gypsy or Irish Traveller','2','20 to 24','2257'],
['Gypsy','White: Gypsy or Irish Traveller','2','25 to 29','2248'],
['Gypsy','White: Gypsy or Irish Traveller','2','30 to 34','1975'],
['Gypsy','White: Gypsy or Irish Traveller','2','35 to 39','2025'],
['Gypsy','White: Gypsy or Irish Traveller','2','40 to 44','1992'],
['Gypsy','White: Gypsy or Irish Traveller','2','45 to 49','1788'],
['Gypsy','White: Gypsy or Irish Traveller','2','50 to 54','1401'],
['Gypsy','White: Gypsy or Irish Traveller','2','55 to 59','1009'],
['Gypsy','White: Gypsy or Irish Traveller','2','60 to 64','845'],
['Gypsy','White: Gypsy or Irish Traveller','2','65 to 69','577'],
['Gypsy','White: Gypsy or Irish Traveller','2','70 to 74','441'],
['Gypsy','White: Gypsy or Irish Traveller','2','75 to 79','322'],
['Gypsy','White: Gypsy or Irish Traveller','2','80 to 84','176'],
['Gypsy','White: Gypsy or Irish Traveller','2','85 and over','140'],
['Irish','White: Irish','2','0 to 4','4269'],
['Irish','White: Irish','2','5 to 7','2518'],
['Irish','White: Irish','2','8 to 9','1792'],
['Irish','White: Irish','2','10 to 14','4815'],
['Irish','White: Irish','2','15','1042'],
['Irish','White: Irish','2','16 to 17','2013'],
['Irish','White: Irish','2','18 to 19','2962'],
['Irish','White: Irish','2','20 to 24','11166'],
['Irish','White: Irish','2','25 to 29','13957'],
['Irish','White: Irish','2','30 to 34','14736'],
['Irish','White: Irish','2','35 to 39','15829'],
['Irish','White: Irish','2','40 to 44','19073'],
['Irish','White: Irish','2','45 to 49','19746'],
['Irish','White: Irish','2','50 to 54','19119'],
['Irish','White: Irish','2','55 to 59','21169'],
['Irish','White: Irish','2','60 to 64','24111'],
['Irish','White: Irish','2','65 to 69','23950'],
['Irish','White: Irish','2','70 to 74','22420'],
['Irish','White: Irish','2','75 to 79','19055'],
['Irish','White: Irish','2','80 to 84','14059'],
['Irish','White: Irish','2','85 and over','11607'],
['Any Other White Background','White: Other White','2','0 to 4','77424'],
['Any Other White Background','White: Other White','2','5 to 7','32185'],
['Any Other White Background','White: Other White','2','8 to 9','18678'],
['Any Other White Background','White: Other White','2','10 to 14','44877'],
['Any Other White Background','White: Other White','2','15','8657'],
['Any Other White Background','White: Other White','2','16 to 17','17358'],
['Any Other White Background','White: Other White','2','18 to 19','23401'],
['Any Other White Background','White: Other White','2','20 to 24','116113'],
['Any Other White Background','White: Other White','2','25 to 29','211906'],
['Any Other White Background','White: Other White','2','30 to 34','194575'],
['Any Other White Background','White: Other White','2','35 to 39','133753'],
['Any Other White Background','White: Other White','2','40 to 44','95884'],
['Any Other White Background','White: Other White','2','45 to 49','72720'],
['Any Other White Background','White: Other White','2','50 to 54','57542'],
['Any Other White Background','White: Other White','2','55 to 59','44105'],
['Any Other White Background','White: Other White','2','60 to 64','36851'],
['Any Other White Background','White: Other White','2','65 to 69','25377'],
['Any Other White Background','White: Other White','2','70 to 74','20959'],
['Any Other White Background','White: Other White','2','75 to 79','16462'],
['Any Other White Background','White: Other White','2','80 to 84','14507'],
['Any Other White Background','White: Other White','2','85 and over','13486'],
['White','White: Total','2','0 to 4','1228380'],
['White','White: Total','2','5 to 7','684938'],
['White','White: Total','2','8 to 9','435911'],
['White','White: Total','2','10 to 14','1208630'],
['White','White: Total','2','15','257457'],
['White','White: Total','2','16 to 17','522872'],
['White','White: Total','2','18 to 19','555463'],
['White','White: Total','2','20 to 24','1441262'],
['White','White: Total','2','25 to 29','1458307'],
['White','White: Total','2','30 to 34','1386039'],
['White','White: Total','2','35 to 39','1465316'],
['White','White: Total','2','40 to 44','1679840'],
['White','White: Total','2','45 to 49','1727109'],
['White','White: Total','2','50 to 54','1531407'],
['White','White: Total','2','55 to 59','1381506'],
['White','White: Total','2','60 to 64','1518132'],
['White','White: Total','2','65 to 69','1220529'],
['White','White: Total','2','70 to 74','1014917'],
['White','White: Total','2','75 to 79','870343'],
['White','White: Total','2','80 to 84','714123'],
['White','White: Total','2','85 and over','780595'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','0 to 4','25059'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','5 to 7','15284'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','8 to 9','9836'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','10 to 14','22461'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','15','4367'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','16 to 17','7791'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','18 to 19','7782'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','20 to 24','21716'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','25 to 29','22438'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','30 to 34','22331'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','35 to 39','20183'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','40 to 44','14966'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','45 to 49','9083'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','50 to 54','7592'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','55 to 59','4336'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','60 to 64','1383'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','65 to 69','1904'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','70 to 74','3404'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','75 to 79','2093'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','85 and over','249'],
['Bangladeshi','Asian/Asian British: Bangladeshi','1','80 to 84','993'],
['Chinese','Other ethnic group: Chinese','1','0 to 4','9122'],
['Chinese','Other ethnic group: Chinese','1','5 to 7','3775'],
['Chinese','Other ethnic group: Chinese','1','8 to 9','2251'],
['Chinese','Other ethnic group: Chinese','1','10 to 14','7175'],
['Chinese','Other ethnic group: Chinese','1','15','2051'],
['Chinese','Other ethnic group: Chinese','1','16 to 17','5375'],
['Chinese','Other ethnic group: Chinese','1','18 to 19','8196'],
['Chinese','Other ethnic group: Chinese','1','20 to 24','37603'],
['Chinese','Other ethnic group: Chinese','1','25 to 29','22759'],
['Chinese','Other ethnic group: Chinese','1','30 to 34','17545'],
['Chinese','Other ethnic group: Chinese','1','35 to 39','13509'],
['Chinese','Other ethnic group: Chinese','1','40 to 44','10476'],
['Chinese','Other ethnic group: Chinese','1','45 to 49','9706'],
['Chinese','Other ethnic group: Chinese','1','50 to 54','8776'],
['Chinese','Other ethnic group: Chinese','1','55 to 59','6927'],
['Chinese','Other ethnic group: Chinese','1','60 to 64','4927'],
['Chinese','Other ethnic group: Chinese','1','65 to 69','2919'],
['Chinese','Other ethnic group: Chinese','1','70 to 74','2717'],
['Chinese','Other ethnic group: Chinese','1','75 to 79','1864'],
['Chinese','Other ethnic group: Chinese','1','85 and over','512'],
['Chinese','Other ethnic group: Chinese','1','80 to 84','939'],
['Indian','Asian/Asian British: Indian','1','0 to 4','49939'],
['Indian','Asian/Asian British: Indian','1','5 to 7','25974'],
['Indian','Asian/Asian British: Indian','1','8 to 9','15377'],
['Indian','Asian/Asian British: Indian','1','10 to 14','38407'],
['Indian','Asian/Asian British: Indian','1','15','8043'],
['Indian','Asian/Asian British: Indian','1','16 to 17','15887'],
['Indian','Asian/Asian British: Indian','1','18 to 19','18488'],
['Indian','Asian/Asian British: Indian','1','20 to 24','63622'],
['Indian','Asian/Asian British: Indian','1','25 to 29','80753'],
['Indian','Asian/Asian British: Indian','1','30 to 34','80212'],
['Indian','Asian/Asian British: Indian','1','35 to 39','64234'],
['Indian','Asian/Asian British: Indian','1','40 to 44','50965'],
['Indian','Asian/Asian British: Indian','1','45 to 49','40925'],
['Indian','Asian/Asian British: Indian','1','50 to 54','40205'],
['Indian','Asian/Asian British: Indian','1','55 to 59','36932'],
['Indian','Asian/Asian British: Indian','1','60 to 64','25990'],
['Indian','Asian/Asian British: Indian','1','65 to 69','17403'],
['Indian','Asian/Asian British: Indian','1','70 to 74','16305'],
['Indian','Asian/Asian British: Indian','1','75 to 79','11589'],
['Indian','Asian/Asian British: Indian','1','85 and over','3239'],
['Indian','Asian/Asian British: Indian','1','80 to 84','5812'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','0 to 4','33644'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','5 to 7','18447'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','8 to 9','10546'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','10 to 14','27399'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','15','5701'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','16 to 17','12300'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','18 to 19','12815'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','20 to 24','36275'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','25 to 29','39121'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','30 to 34','42960'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','35 to 39','44196'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','40 to 44','32950'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','45 to 49','24116'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','50 to 54','17915'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','55 to 59','13714'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','60 to 64','10211'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','65 to 69','6601'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','70 to 74','4805'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','75 to 79','3171'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','85 and over','846'],
['Any Other Asian Background','Asian/Asian British: Other Asian','1','80 to 84','1602'],
['Pakistani','Asian/Asian British: Pakistani','1','0 to 4','65164'],
['Pakistani','Asian/Asian British: Pakistani','1','5 to 7','37052'],
['Pakistani','Asian/Asian British: Pakistani','1','8 to 9','23481'],
['Pakistani','Asian/Asian British: Pakistani','1','10 to 14','51512'],
['Pakistani','Asian/Asian British: Pakistani','1','15','9602'],
['Pakistani','Asian/Asian British: Pakistani','1','16 to 17','18818'],
['Pakistani','Asian/Asian British: Pakistani','1','18 to 19','19011'],
['Pakistani','Asian/Asian British: Pakistani','1','20 to 24','50845'],
['Pakistani','Asian/Asian British: Pakistani','1','25 to 29','58127'],
['Pakistani','Asian/Asian British: Pakistani','1','30 to 34','58334'],
['Pakistani','Asian/Asian British: Pakistani','1','35 to 39','45361'],
['Pakistani','Asian/Asian British: Pakistani','1','40 to 44','36822'],
['Pakistani','Asian/Asian British: Pakistani','1','45 to 49','22181'],
['Pakistani','Asian/Asian British: Pakistani','1','50 to 54','20599'],
['Pakistani','Asian/Asian British: Pakistani','1','55 to 59','18344'],
['Pakistani','Asian/Asian British: Pakistani','1','60 to 64','9321'],
['Pakistani','Asian/Asian British: Pakistani','1','65 to 69','6796'],
['Pakistani','Asian/Asian British: Pakistani','1','70 to 74','8487'],
['Pakistani','Asian/Asian British: Pakistani','1','75 to 79','5779'],
['Pakistani','Asian/Asian British: Pakistani','1','85 and over','1331'],
['Pakistani','Asian/Asian British: Pakistani','1','80 to 84','2826'],
['Asian or Asian British','Asian/Asian British: Total','1','0 to 4','182928'],
['Asian or Asian British','Asian/Asian British: Total','1','5 to 7','100532'],
['Asian or Asian British','Asian/Asian British: Total','1','8 to 9','61491'],
['Asian or Asian British','Asian/Asian British: Total','1','10 to 14','146954'],
['Asian or Asian British','Asian/Asian British: Total','1','15','29764'],
['Asian or Asian British','Asian/Asian British: Total','1','16 to 17','60171'],
['Asian or Asian British','Asian/Asian British: Total','1','18 to 19','66292'],
['Asian or Asian British','Asian/Asian British: Total','1','20 to 24','210061'],
['Asian or Asian British','Asian/Asian British: Total','1','25 to 29','223198'],
['Asian or Asian British','Asian/Asian British: Total','1','30 to 34','221382'],
['Asian or Asian British','Asian/Asian British: Total','1','35 to 39','187483'],
['Asian or Asian British','Asian/Asian British: Total','1','40 to 44','146179'],
['Asian or Asian British','Asian/Asian British: Total','1','45 to 49','106011'],
['Asian or Asian British','Asian/Asian British: Total','1','50 to 54','95087'],
['Asian or Asian British','Asian/Asian British: Total','1','55 to 59','80253'],
['Asian or Asian British','Asian/Asian British: Total','1','60 to 64','51832'],
['Asian or Asian British','Asian/Asian British: Total','1','65 to 69','35623'],
['Asian or Asian British','Asian/Asian British: Total','1','70 to 74','35718'],
['Asian or Asian British','Asian/Asian British: Total','1','75 to 79','24496'],
['Asian or Asian British','Asian/Asian British: Total','1','85 and over','6177'],
['Asian or Asian British','Asian/Asian British: Total','1','80 to 84','12172'],
['African','Black/African/Caribbean/Black British: African','1','0 to 4','54450'],
['African','Black/African/Caribbean/Black British: African','1','5 to 7','28940'],
['African','Black/African/Caribbean/Black British: African','1','8 to 9','16334'],
['African','Black/African/Caribbean/Black British: African','1','10 to 14','40603'],
['African','Black/African/Caribbean/Black British: African','1','15','8335'],
['African','Black/African/Caribbean/Black British: African','1','16 to 17','15794'],
['African','Black/African/Caribbean/Black British: African','1','18 to 19','15647'],
['African','Black/African/Caribbean/Black British: African','1','20 to 24','37637'],
['African','Black/African/Caribbean/Black British: African','1','25 to 29','39486'],
['African','Black/African/Caribbean/Black British: African','1','30 to 34','46933'],
['African','Black/African/Caribbean/Black British: African','1','35 to 39','45068'],
['African','Black/African/Caribbean/Black British: African','1','40 to 44','42653'],
['African','Black/African/Caribbean/Black British: African','1','45 to 49','31794'],
['African','Black/African/Caribbean/Black British: African','1','50 to 54','21313'],
['African','Black/African/Caribbean/Black British: African','1','55 to 59','11424'],
['African','Black/African/Caribbean/Black British: African','1','60 to 64','6445'],
['African','Black/African/Caribbean/Black British: African','1','65 to 69','3746'],
['African','Black/African/Caribbean/Black British: African','1','70 to 74','3571'],
['African','Black/African/Caribbean/Black British: African','1','75 to 79','2001'],
['African','Black/African/Caribbean/Black British: African','1','85 and over','415'],
['African','Black/African/Caribbean/Black British: African','1','80 to 84','767'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','0 to 4','14904'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','5 to 7','9640'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','8 to 9','6324'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','10 to 14','17639'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','15','3995'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','16 to 17','7884'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','18 to 19','7861'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','20 to 24','18867'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','25 to 29','17295'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','30 to 34','16366'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','35 to 39','17318'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','40 to 44','25603'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','45 to 49','31384'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','50 to 54','22142'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','55 to 59','12728'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','60 to 64','7031'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','65 to 69','8233'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','70 to 74','11787'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','75 to 79','9566'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','85 and over','2726'],
['Caribbean','Black/African/Caribbean/Black British: Caribbean','1','80 to 84','5448'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','0 to 4','19496'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','5 to 7','10643'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','8 to 9','6509'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','10 to 14','14511'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','15','2699'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','16 to 17','5235'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','18 to 19','4349'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','20 to 24','10297'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','25 to 29','11333'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','30 to 34','9884'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','35 to 39','8611'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','40 to 44','11666'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','45 to 49','11824'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','50 to 54','5738'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','55 to 59','2274'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','60 to 64','1287'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','65 to 69','1102'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','70 to 74','1138'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','75 to 79','886'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','85 and over','219'],
['Any Other Black Background','Black/African/Caribbean/Black British: Other Black','1','80 to 84','387'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','0 to 4','88850'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','5 to 7','49223'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','8 to 9','29167'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','10 to 14','72753'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','15','15029'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','16 to 17','28913'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','18 to 19','27857'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','20 to 24','66801'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','25 to 29','68114'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','30 to 34','73183'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','35 to 39','70997'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','40 to 44','79922'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','45 to 49','75002'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','50 to 54','49193'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','55 to 59','26426'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','60 to 64','14763'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','65 to 69','13081'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','70 to 74','16496'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','75 to 79','12453'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','85 and over','3360'],
['Black or Black British','Black/African/Caribbean/Black British: Total','1','80 to 84','6602'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','0 to 4','24902'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','5 to 7','11491'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','8 to 9','6298'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','10 to 14','14485'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','15','2666'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','16 to 17','5053'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','18 to 19','4615'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','20 to 24','11692'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','25 to 29','12687'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','30 to 34','11099'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','35 to 39','8223'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','40 to 44','6839'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','45 to 49','5513'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','50 to 54','3633'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','55 to 59','2537'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','60 to 64','1894'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','65 to 69','1603'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','70 to 74','1048'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','75 to 79','728'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','85 and over','322'],
['Any Other Mixed Background','Mixed/multiple ethnic group: Other Mixed','1','80 to 84','437'],
['Mixed','Mixed/multiple ethnic group: Total','1','0 to 4','109372'],
['Mixed','Mixed/multiple ethnic group: Total','1','5 to 7','51729'],
['Mixed','Mixed/multiple ethnic group: Total','1','8 to 9','29288'],
['Mixed','Mixed/multiple ethnic group: Total','1','10 to 14','70207'],
['Mixed','Mixed/multiple ethnic group: Total','1','15','13501'],
['Mixed','Mixed/multiple ethnic group: Total','1','16 to 17','26094'],
['Mixed','Mixed/multiple ethnic group: Total','1','18 to 19','24312'],
['Mixed','Mixed/multiple ethnic group: Total','1','20 to 24','55977'],
['Mixed','Mixed/multiple ethnic group: Total','1','25 to 29','48259'],
['Mixed','Mixed/multiple ethnic group: Total','1','30 to 34','38616'],
['Mixed','Mixed/multiple ethnic group: Total','1','35 to 39','29864'],
['Mixed','Mixed/multiple ethnic group: Total','1','40 to 44','28063'],
['Mixed','Mixed/multiple ethnic group: Total','1','45 to 49','23688'],
['Mixed','Mixed/multiple ethnic group: Total','1','50 to 54','15095'],
['Mixed','Mixed/multiple ethnic group: Total','1','55 to 59','9148'],
['Mixed','Mixed/multiple ethnic group: Total','1','60 to 64','6554'],
['Mixed','Mixed/multiple ethnic group: Total','1','65 to 69','5283'],
['Mixed','Mixed/multiple ethnic group: Total','1','70 to 74','4098'],
['Mixed','Mixed/multiple ethnic group: Total','1','75 to 79','3106'],
['Mixed','Mixed/multiple ethnic group: Total','1','85 and over','1383'],
['Mixed','Mixed/multiple ethnic group: Total','1','80 to 84','2027'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','0 to 4','33434'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','5 to 7','15628'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','8 to 9','8452'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','10 to 14','19432'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','15','3710'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','16 to 17','6954'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','18 to 19','6780'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','20 to 24','16282'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','25 to 29','13834'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','30 to 34','10848'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','35 to 39','8227'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','40 to 44','7913'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','45 to 49','6404'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','50 to 54','4062'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','55 to 59','2657'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','60 to 64','1959'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','65 to 69','1478'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','70 to 74','1112'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','75 to 79','866'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','85 and over','412'],
['White and Asian','Mixed/multiple ethnic group: White and Asian','1','80 to 84','531'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','0 to 4','18051'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','5 to 7','7737'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','8 to 9','4156'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','10 to 14','8863'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','15','1556'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','16 to 17','2939'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','18 to 19','2630'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','20 to 24','6109'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','25 to 29','5986'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','30 to 34','5584'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','35 to 39','4508'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','40 to 44','3884'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','45 to 49','3201'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','50 to 54','2236'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','55 to 59','1353'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','60 to 64','723'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','65 to 69','548'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','70 to 74','325'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','75 to 79','211'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','85 and over','68'],
['White and Black African','Mixed/multiple ethnic group: White and Black African','1','80 to 84','127'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','0 to 4','32985'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','5 to 7','16873'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','8 to 9','10382'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','10 to 14','27427'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','15','5569'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','16 to 17','11148'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','18 to 19','10287'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','20 to 24','21894'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','25 to 29','15752'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','30 to 34','11085'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','35 to 39','8906'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','40 to 44','9427'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','45 to 49','8570'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','50 to 54','5164'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','55 to 59','2601'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','60 to 64','1978'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','65 to 69','1654'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','70 to 74','1613'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','75 to 79','1301'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','85 and over','581'],
['White and Black Caribbean','Mixed/multiple ethnic group: White and Black Caribbean','1','80 to 84','932'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','0 to 4','12054'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','5 to 7','6464'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','8 to 9','3770'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','10 to 14','9945'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','15','2137'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','16 to 17','4818'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','18 to 19','4836'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','20 to 24','14823'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','25 to 29','21041'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','30 to 34','23547'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','35 to 39','18225'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','40 to 44','13629'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','45 to 49','10955'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','50 to 54','8813'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','55 to 59','6959'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','60 to 64','4801'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','65 to 69','3030'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','70 to 74','2223'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','75 to 79','1441'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','85 and over','622'],
['Any other ethnic group','Other ethnic group: Any other ethnic group','1','80 to 84','959'],
['Arab','Other ethnic group: Arab','1','0 to 4','12825'],
['Arab','Other ethnic group: Arab','1','5 to 7','6861'],
['Arab','Other ethnic group: Arab','1','8 to 9','3606'],
['Arab','Other ethnic group: Arab','1','10 to 14','8060'],
['Arab','Other ethnic group: Arab','1','15','1449'],
['Arab','Other ethnic group: Arab','1','16 to 17','3246'],
['Arab','Other ethnic group: Arab','1','18 to 19','4036'],
['Arab','Other ethnic group: Arab','1','20 to 24','13314'],
['Arab','Other ethnic group: Arab','1','25 to 29','14439'],
['Arab','Other ethnic group: Arab','1','30 to 34','13940'],
['Arab','Other ethnic group: Arab','1','35 to 39','12408'],
['Arab','Other ethnic group: Arab','1','40 to 44','10118'],
['Arab','Other ethnic group: Arab','1','45 to 49','7511'],
['Arab','Other ethnic group: Arab','1','50 to 54','5303'],
['Arab','Other ethnic group: Arab','1','55 to 59','3883'],
['Arab','Other ethnic group: Arab','1','60 to 64','2846'],
['Arab','Other ethnic group: Arab','1','65 to 69','1776'],
['Arab','Other ethnic group: Arab','1','70 to 74','1189'],
['Arab','Other ethnic group: Arab','1','75 to 79','808'],
['Arab','Other ethnic group: Arab','1','85 and over','245'],
['Arab','Other ethnic group: Arab','1','80 to 84','474'],
['Other Ethnic Groups','Other ethnic group: Total','1','0 to 4','24879'],
['Other Ethnic Groups','Other ethnic group: Total','1','5 to 7','13325'],
['Other Ethnic Groups','Other ethnic group: Total','1','8 to 9','7376'],
['Other Ethnic Groups','Other ethnic group: Total','1','10 to 14','18005'],
['Other Ethnic Groups','Other ethnic group: Total','1','15','3586'],
['Other Ethnic Groups','Other ethnic group: Total','1','16 to 17','8064'],
['Other Ethnic Groups','Other ethnic group: Total','1','18 to 19','8872'],
['Other Ethnic Groups','Other ethnic group: Total','1','20 to 24','28137'],
['Other Ethnic Groups','Other ethnic group: Total','1','25 to 29','35480'],
['Other Ethnic Groups','Other ethnic group: Total','1','30 to 34','37487'],
['Other Ethnic Groups','Other ethnic group: Total','1','35 to 39','30633'],
['Other Ethnic Groups','Other ethnic group: Total','1','40 to 44','23747'],
['Other Ethnic Groups','Other ethnic group: Total','1','45 to 49','18466'],
['Other Ethnic Groups','Other ethnic group: Total','1','50 to 54','14116'],
['Other Ethnic Groups','Other ethnic group: Total','1','55 to 59','10842'],
['Other Ethnic Groups','Other ethnic group: Total','1','60 to 64','7647'],
['Other Ethnic Groups','Other ethnic group: Total','1','65 to 69','4806'],
['Other Ethnic Groups','Other ethnic group: Total','1','70 to 74','3412'],
['Other Ethnic Groups','Other ethnic group: Total','1','75 to 79','2249'],
['Other Ethnic Groups','Other ethnic group: Total','1','85 and over','867'],
['Other Ethnic Groups','Other ethnic group: Total','1','80 to 84','1433'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','0 to 4','1202836'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','5 to 7','681855'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','8 to 9','437174'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','10 to 14','1214781'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','15','262408'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','16 to 17','530358'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','18 to 19','544664'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','20 to 24','1341931'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','25 to 29','1247066'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','30 to 34','1177936'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','35 to 39','1299230'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','40 to 44','1534481'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','45 to 49','1606878'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','50 to 54','1443266'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','55 to 59','1299094'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','60 to 64','1425831'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','65 to 69','1117957'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','70 to 74','873728'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','75 to 79','688740'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','85 and over','358591'],
['British','White: English/Welsh/Scottish/Northern Irish/British','1','80 to 84','481274'],
['Gypsy','White: Gypsy or Irish Traveller','1','0 to 4','2894'],
['Gypsy','White: Gypsy or Irish Traveller','1','5 to 7','1608'],
['Gypsy','White: Gypsy or Irish Traveller','1','8 to 9','1047'],
['Gypsy','White: Gypsy or Irish Traveller','1','10 to 14','2749'],
['Gypsy','White: Gypsy or Irish Traveller','1','15','571'],
['Gypsy','White: Gypsy or Irish Traveller','1','16 to 17','1104'],
['Gypsy','White: Gypsy or Irish Traveller','1','18 to 19','897'],
['Gypsy','White: Gypsy or Irish Traveller','1','20 to 24','2207'],
['Gypsy','White: Gypsy or Irish Traveller','1','25 to 29','1941'],
['Gypsy','White: Gypsy or Irish Traveller','1','30 to 34','1858'],
['Gypsy','White: Gypsy or Irish Traveller','1','35 to 39','1754'],
['Gypsy','White: Gypsy or Irish Traveller','1','40 to 44','1836'],
['Gypsy','White: Gypsy or Irish Traveller','1','45 to 49','1759'],
['Gypsy','White: Gypsy or Irish Traveller','1','50 to 54','1410'],
['Gypsy','White: Gypsy or Irish Traveller','1','55 to 59','1065'],
['Gypsy','White: Gypsy or Irish Traveller','1','60 to 64','913'],
['Gypsy','White: Gypsy or Irish Traveller','1','65 to 69','638'],
['Gypsy','White: Gypsy or Irish Traveller','1','70 to 74','464'],
['Gypsy','White: Gypsy or Irish Traveller','1','75 to 79','272'],
['Gypsy','White: Gypsy or Irish Traveller','1','85 and over','90'],
['Gypsy','White: Gypsy or Irish Traveller','1','80 to 84','127'],
['Irish','White: Irish','1','0 to 4','4607'],
['Irish','White: Irish','1','5 to 7','2590'],
['Irish','White: Irish','1','8 to 9','1840'],
['Irish','White: Irish','1','10 to 14','5038'],
['Irish','White: Irish','1','15','1117'],
['Irish','White: Irish','1','16 to 17','2248'],
['Irish','White: Irish','1','18 to 19','2893'],
['Irish','White: Irish','1','20 to 24','10432'],
['Irish','White: Irish','1','25 to 29','15387'],
['Irish','White: Irish','1','30 to 34','15813'],
['Irish','White: Irish','1','35 to 39','15877'],
['Irish','White: Irish','1','40 to 44','19544'],
['Irish','White: Irish','1','45 to 49','21446'],
['Irish','White: Irish','1','50 to 54','19328'],
['Irish','White: Irish','1','55 to 59','19691'],
['Irish','White: Irish','1','60 to 64','21965'],
['Irish','White: Irish','1','65 to 69','21551'],
['Irish','White: Irish','1','70 to 74','18935'],
['Irish','White: Irish','1','75 to 79','14222'],
['Irish','White: Irish','1','85 and over','4911'],
['Irish','White: Irish','1','80 to 84','8158'],
['Any Other White Background','White: Other White','1','0 to 4','81805'],
['Any Other White Background','White: Other White','1','5 to 7','33656'],
['Any Other White Background','White: Other White','1','8 to 9','19370'],
['Any Other White Background','White: Other White','1','10 to 14','46524'],
['Any Other White Background','White: Other White','1','15','9246'],
['Any Other White Background','White: Other White','1','16 to 17','17699'],
['Any Other White Background','White: Other White','1','18 to 19','20958'],
['Any Other White Background','White: Other White','1','20 to 24','94858'],
['Any Other White Background','White: Other White','1','25 to 29','181091'],
['Any Other White Background','White: Other White','1','30 to 34','188628'],
['Any Other White Background','White: Other White','1','35 to 39','129744'],
['Any Other White Background','White: Other White','1','40 to 44','89669'],
['Any Other White Background','White: Other White','1','45 to 49','66508'],
['Any Other White Background','White: Other White','1','50 to 54','50234'],
['Any Other White Background','White: Other White','1','55 to 59','35226'],
['Any Other White Background','White: Other White','1','60 to 64','27635'],
['Any Other White Background','White: Other White','1','65 to 69','19026'],
['Any Other White Background','White: Other White','1','70 to 74','15102'],
['Any Other White Background','White: Other White','1','75 to 79','10165'],
['Any Other White Background','White: Other White','1','85 and over','8189'],
['Any Other White Background','White: Other White','1','80 to 84','7857'],
['White','White: Total','1','0 to 4','1292142'],
['White','White: Total','1','5 to 7','719709'],
['White','White: Total','1','8 to 9','459431'],
['White','White: Total','1','10 to 14','1269092'],
['White','White: Total','1','15','273342'],
['White','White: Total','1','16 to 17','551409'],
['White','White: Total','1','18 to 19','569412'],
['White','White: Total','1','20 to 24','1449428'],
['White','White: Total','1','25 to 29','1445485'],
['White','White: Total','1','30 to 34','1384235'],
['White','White: Total','1','35 to 39','1446605'],
['White','White: Total','1','40 to 44','1645530'],
['White','White: Total','1','45 to 49','1696591'],
['White','White: Total','1','50 to 54','1514238'],
['White','White: Total','1','55 to 59','1355076'],
['White','White: Total','1','60 to 64','1476344'],
['White','White: Total','1','65 to 69','1159172'],
['White','White: Total','1','70 to 74','908229'],
['White','White: Total','1','75 to 79','713399'],
['White','White: Total','1','85 and over','371781'],
['White','White: Total','1','80 to 84','497416']
  
]

# Create the pandas DataFrame 
pdf_Population_Data = pd.DataFrame(Population_Data, columns = ['ethnic_group_formatted', 'Ethnic_group', 'Gender', 'Age_group', 'Population']) 

# COMMAND ----------

# # Create the pandas DataFrame 
# pdf_Population_Data = pd.DataFrame(MHSDS_Contact_List, columns = ['ethnic_group_formatted', 'Ethnic_group', 'Gender', 'Age_group', 'Population']) 

# COMMAND ----------

# DBTITLE 1,Upload Data to the Database
if not pdf_Population_Data.empty:
  sdf_Population_Data = spark.createDataFrame(pdf_Population_Data)
  sdf_Population_Data.write.insertInto(f"{db_output}.pop_health")
  print('complete - Population Data Uploaded')
else:
  print('no data')
print('---')   

# COMMAND ----------

# DBTITLE 1,IMD Population Data - LSOA2011 -> IMD2019 (CCG19 Mapped to CCG21 and then transformed to STP21)
 %md
 transformed from this datasource:
 https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates

 IMD Decile data for each LSOA11 code was obtained from $reference_data

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.imd_pop;

 --------------- This is the new table format _____________________________
 CREATE TABLE IF NOT EXISTS $db_output.imd_pop
 (      
        IMD_Decile                STRING,
        Count                     INT
 )
  using delta

# COMMAND ----------

# DBTITLE 1,IMD 2019 Pop data
data = """
IMD_Decile,Count
01 Least deprived,5413926
02 Less deprived,5487156
03 Less deprived,5554096
04 Less deprived,5563598
05 Less deprived,5733511
06 More deprived,5692582
07 More deprived,5766656
08 More deprived,5809906
09 More deprived,5677439
10 Most deprived,5588091
"""
df = pd.read_csv(io.StringIO(data), header=0, delimiter=',').astype(str)
spark.createDataFrame(df).write.insertInto(f"{db_output}.imd_pop")

# COMMAND ----------

# DBTITLE 1,CCG 2020 Pop data
 %sql
 create or replace temp view reference_list_rn as
 select 
 --       predecessor ID
      geo.DH_GEOGRAPHY_CODE
 --       successor ID
       ,SuccessorDetails.TargetOrganisationID
      --predecessor ID
       ,SuccessorDetails.OrganisationID
       ,row_number() over (partition by geo.DH_GEOGRAPHY_CODE order by geo.year_of_change desc) as RN
 ,sum(pop_v2.population_count) as all_ages
 ,sum(case when (pop_v2.age_lower > 17 and pop_v2.age_lower < 65) then pop_v2.population_count else 0 end) as Aged_18_to_64
 ,sum(case when (pop_v2.age_lower > 64) then pop_v2.population_count else 0 end) as Aged_65_OVER
 ,sum(case when (pop_v2.age_lower < 18) then pop_v2.population_count else 0 end) as Aged_0_to_17
 from $reference_data.ONS_POPULATION_V2 pop_v2
 inner join  $reference_data.ONS_CHD_GEO_EQUIVALENTS geo
 on pop_v2.GEOGRAPHIC_SUBGROUP_CODE = geo.geography_code 
 Left join $reference_data.odsapisuccessordetails successorDetails 
 on geo.DH_GEOGRAPHY_CODE = successorDetails.Organisationid and successorDetails.Type = 'Successor' and successorDetails.startDate > '2021-03-31'--and geo.IS_CURRENT =1
 where pop_v2.GEOGRAPHIC_GROUP_CODE = 'E38'
 and geo.ENTITY_CODE = 'E38'
 -- and geo.IS_CURRENT =1
 and pop_v2.year_of_count = '2020'
 and trim(pop_v2.RECORD_TYPE) = 'E'
 -- and DH_GEOGRAPHY_CODE = '99M'
 group by geo.DH_GEOGRAPHY_CODE,successorDetails.TargetOrganisationID,successorDetails.OrganisationID,geo.year_of_change
 order by geo.DH_GEOGRAPHY_CODE

# COMMAND ----------

 %sql
 create or replace temp view reference_list_rn1 as
 select * from reference_list_rn where RN = 1

# COMMAND ----------

 %sql
 create or replace temp view no_successor as
 --when successorDetails.Organisationid is not null then we do have a successor organisation after 31 Mar 2021
 --likewise when successorDetails.OrganisationID is null then predecessor adn successor orgs are the same (ie no successor)
 select 
 --ref.OrganisationID as id, 
         org1.org_code as CCG_2020_CODE,
         org1.name as CCG_2020_name, 
         org1.org_code as CCG_2021_CODE,
         org1.name as CCG_2021_name,
         ref.All_Ages,
         ref.Aged_18_to_64,
         ref.Aged_65_OVER,
         ref.Aged_0_to_17
 from reference_list_rn1 ref
 inner join $reference_data.org_daily org1
 on ref.DH_GEOGRAPHY_CODE = org1.org_code
 where ref.OrganisationID is null
 and org1.Business_end_date is null
 order by CCG_2020_CODE 

# COMMAND ----------

 %sql
 ---description needed
 create or replace temp view with_successor_this_one_predecessor as
 select 
         ref.OrganisationID as id,
         org2.name as name, 
         org2.org_code as Org_CODE,
         ref.All_Ages,
         ref.Aged_18_to_64,
         ref.Aged_65_OVER,
         ref.Aged_0_to_17
 from reference_list_rn1 ref
 inner join $reference_data.org_daily org2
 on ref.DH_GEOGRAPHY_CODE = org2.org_code
 where ref.OrganisationID is not null
 and org2.Business_end_date is null

# COMMAND ----------

 %sql
 create or replace temp view with_successor_this_one_successor as
 select 
       ref.OrganisationID as id,
       org3.name ,
       org3.org_code
 from reference_list_rn1 ref
 inner join $reference_data.org_daily org3
 on ref.TargetOrganisationID = org3.org_code
 where ref.OrganisationID is not null
 and org3.Business_end_date is null 

# COMMAND ----------

 %sql
 create or replace temp view with_successor as
 select  
       org_predecessor.org_code as CCG_2020_CODE,
       org_predecessor.Name as CCG_2020_NAME,
       org_successor.org_code as CCG_2021_CODE,
       org_successor.Name as CCG_2021_NAME,
       org_predecessor.All_Ages,
       org_predecessor.Aged_18_to_64,
       org_predecessor.Aged_65_OVER,
       org_predecessor.Aged_0_to_17
 from with_successor_this_one_predecessor org_predecessor
 inner join with_successor_this_one_successor org_successor
 on org_predecessor.id = org_successor.id
 order by CCG_2020_CODE

# COMMAND ----------

# DBTITLE 1,CCG 2021 Pop Data
 %sql
 drop table if exists $db_output.mha_ccg_pop;   
 create table if not exists $db_output.mha_ccg_pop as

 select 
 o.CCG_2021_CODE as CCG_CODE,
 sum(All_ages) as POP
 from no_successor o
 group by o.CCG_2021_CODE
 union
 select 
 o.CCG_2021_CODE as CCG_CODE,
 sum(All_ages) as POP
 from with_successor o
 group by o.CCG_2021_CODE

# COMMAND ----------

# DBTITLE 1,STP 2021 Pop data
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

# DBTITLE 1,CCG 2019 Demographic Pop data
 %sql
 create or replace temporary view ods_ccg_ethpop as
 select distinct e.CCG19CD, o.DH_GEOGRAPHY_CODE as ODS_CCG_CODE, s.OrganisationID as PRED_ID, s.TargetOrganisationID as SUCC_ID, e.Ethnic_group, e.Ethnic_subgroup, e.Ethnicity, e.Ethnic_Code, e.Sex, e.Age, e.EthPOP_value 
 from $reference_data.ccg_ethpop e
 left join $reference_data.ONS_CHD_GEO_EQUIVALENTS o on e.CCG19CD = o.GEOGRAPHY_CODE
 left join $reference_data.odsapisuccessordetails s on o.DH_GEOGRAPHY_CODE = s.Organisationid and s.Type = 'Successor' --and s.startDate > '2022-03-31'
 where Ethnic_group <> "All"
 order by e.CCG19CD, e.Ethnic_group, e.Ethnic_subgroup, e.Ethnicity, e.Ethnic_Code, e.Sex, e.Age

# COMMAND ----------

 %sql
 create or replace temp view ethpop_no_successor as
 select  
         org1.org_code as CCG19_CODE,
         org1.org_code as CCG21_CODE,
         e.Ethnic_group, 
         e.Ethnic_subgroup, 
         e.Ethnicity, 
         e.Ethnic_Code, 
         e.Sex, 
         e.Age, 
         e.EthPOP_value
 from ods_ccg_ethpop e
 inner join $reference_data.org_daily org1
 on e.ODS_CCG_CODE = org1.org_code
 where e.PRED_ID is null
 and org1.Business_end_date is null

# COMMAND ----------

 %sql
 create or replace temp view ethpop_with_successor_this_one_predecessor as
 select 
         e.PRED_ID,
         org2.org_code as CCG19_CODE,
         e.Ethnic_group, 
         e.Ethnic_subgroup, 
         e.Ethnicity, 
         e.Ethnic_Code, 
         e.Sex, 
         e.Age, 
         e.EthPOP_value
 from ods_ccg_ethpop e
 inner join $reference_data.org_daily org2
 on e.ODS_CCG_CODE = org2.org_code
 where e.PRED_ID is not null
 and org2.Business_end_date is null

# COMMAND ----------

 %sql
 create or replace temp view ethpop_with_successor_this_one_successor as
 select 
      distinct
       e.PRED_ID,
       org3.org_code as CCG21_CODE
 from ods_ccg_ethpop e
 inner join $reference_data.org_daily org3
 on e.SUCC_ID = org3.org_code
 where e.PRED_ID is not null
 and org3.Business_end_date is null 

# COMMAND ----------

 %sql
 create or replace temp view ethpop_with_successor as
 select  
       org_predecessor.CCG19_CODE,
       org_successor.CCG21_CODE,
       org_predecessor.Ethnic_group, 
       org_predecessor.Ethnic_subgroup, 
       org_predecessor.Ethnicity, 
       org_predecessor.Ethnic_Code, 
       org_predecessor.Sex, 
       org_predecessor.Age, 
       org_predecessor.EthPOP_value
 from ethpop_with_successor_this_one_predecessor org_predecessor
 inner join ethpop_with_successor_this_one_successor org_successor
 on org_predecessor.PRED_ID = org_successor.PRED_ID

# COMMAND ----------

# DBTITLE 1,CCG 2019 and 2021 Demographic Pop data
 %sql
 drop table if exists $db_output.mha_ccg_ethpop;   
 create table if not exists $db_output.mha_ccg_ethpop as

 select 
 CCG19_CODE,
 CCG21_CODE,
 Ethnic_group, 
 Ethnic_subgroup, 
 Ethnicity, 
 Ethnic_Code, 
 Sex, 
 Age, 
 sum(EthPOP_value) as POP
 from ethpop_no_successor o
 group by CCG19_CODE, CCG21_CODE, Ethnic_group, Ethnic_subgroup, Ethnicity, Ethnic_Code, Sex, Age
 union
 select 
 CCG19_CODE,
 CCG21_CODE,
 Ethnic_group, 
 Ethnic_subgroup, 
 Ethnicity, 
 Ethnic_Code, 
 Sex, 
 Age, 
 sum(EthPOP_value) as POP
 from ethpop_with_successor o
 group by CCG19_CODE, CCG21_CODE, Ethnic_group, Ethnic_subgroup, Ethnicity, Ethnic_Code, Sex, Age

# COMMAND ----------

# DBTITLE 1,STP 2021 Demographic Pop data
 %sql
 drop table if exists $db_output.mha_stp_ethpop;
 create table if not exists $db_output.mha_stp_ethpop as
 select
 stp.STP_CODE,
 stp.STP_NAME,
 ccg.Ethnic_group, 
 ccg.Ethnic_subgroup, 
 ccg.Ethnicity, 
 ccg.Ethnic_Code, 
 ccg.Sex, 
 ccg.Age, 
 sum(ccg.POP) as POP
 from $db_output.mha_ccg_ethpop ccg
 inner join $db_output.mha_stp_mapping stp on ccg.CCG21_CODE = stp.CCG_CODE
 group by stp.STP_CODE, stp.STP_NAME, ccg.Ethnic_group, ccg.Ethnic_subgroup, ccg.Ethnicity, ccg.Ethnic_Code, ccg.Sex, ccg.Age

# COMMAND ----------

# DBTITLE 1,STP 2021 Age Pop data
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
 from $db_output.mha_stp_ethpop
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

# DBTITLE 1,STP 2021 Gender Pop data
 %sql
 drop table if exists $db_output.stp_gender_pop;
 create table if not exists $db_output.stp_gender_pop as
 select
 STP_CODE,
 STP_NAME,
 Sex,
 sum(POP) as POP
 from $db_output.mha_stp_ethpop
 group by STP_CODE, STP_NAME, Sex

# COMMAND ----------

# DBTITLE 1,STP 2021 Higher Ethnicity Pop data
 %sql
 drop table if exists $db_output.stp_eth_pop;
 create table if not exists $db_output.stp_eth_pop as
 select
 STP_CODE,
 STP_NAME,
 case when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end as Ethnic_group,
 sum(POP) as POP
 from $db_output.mha_stp_ethpop
 group by STP_CODE, STP_NAME, 
 case when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end

# COMMAND ----------

 %sql
 create or replace temporary view lsoa11_ons_ccg19 as
 select distinct LSOA11, CCG as CCG19 from $reference_data.postcode 
 where 
 LEFT(LSOA11, 1) = "E"
 and (RECORD_END_DATE >= '2020-03-31' OR RECORD_END_DATE IS NULL)	
 and RECORD_START_DATE <= '2020-03-31' 

# COMMAND ----------

 %sql
 create or replace temporary view lsoa11_ons_ccg21 as
 select distinct LSOA11, CCG as CCG21 from $reference_data.postcode 
 where 
 LEFT(LSOA11, 1) = "E"
 and (RECORD_END_DATE >= '2022-03-31' OR RECORD_END_DATE IS NULL)	
 and RECORD_START_DATE <= '2022-03-31'

# COMMAND ----------

 %sql
 create or replace temporary view lsoa11_stp_mapped as
 select n.LSOA11, n.CCG19, t.CCG21, s.STP_CODE as STP21,
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
                 END AS IMD_Decile
 from lsoa11_ons_ccg19 n
 left join lsoa11_ons_ccg21 t on n.LSOA11 = t.LSOA11
 left join $reference_data.english_indices_of_dep_v02 r 
                     on n.LSOA11 = r.LSOA_CODE_2011 
                     and r.imd_year = '2019'
 left join $db_output.mha_stp_mapping s on t.CCG21 = s.CCG_CODE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_imd_pop;

 --------------- This is the new table format _____________________________
 CREATE TABLE IF NOT EXISTS $db_output.stp_imd_pop
 (      
        STP                       STRING,         
        IMD_Decile                STRING,
        Count                     INT
 )
  using delta

# COMMAND ----------

data = """
STP,IMD_Decile,Count
QMJ,04 Less deprived,132906
QMJ,08 More deprived,266582
QMJ,06 More deprived,148413
QMJ,05 Less deprived,182774
QMJ,07 More deprived,198444
QMJ,09 More deprived,253373
QMJ,02 Less deprived,105658
QMJ,03 Less deprived,135831
QMJ,10 Most deprived,56028
QMJ,01 Least deprived,30797
QWE,04 Less deprived,208614
QWE,08 More deprived,145200
QWE,06 More deprived,146961
QWE,05 Less deprived,172356
QWE,07 More deprived,133082
QWE,09 More deprived,94157
QWE,02 Less deprived,245341
QWE,03 Less deprived,174087
QWE,10 Most deprived,10190
QWE,01 Least deprived,174822
QRV,04 Less deprived,204389
QRV,08 More deprived,302807
QRV,06 More deprived,320577
QRV,05 Less deprived,281535
QRV,07 More deprived,350054
QRV,09 More deprived,213252
QRV,02 Less deprived,129126
QRV,03 Less deprived,213882
QRV,10 Most deprived,45258
QRV,01 Least deprived,42839
QMF,04 Less deprived,103880
QMF,08 More deprived,544177
QMF,06 More deprived,219294
QMF,05 Less deprived,171733
QMF,07 More deprived,319056
QMF,09 More deprived,434935
QMF,02 Less deprived,71512
QMF,03 Less deprived,76174
QMF,10 Most deprived,52211
QMF,01 Least deprived,30411
QKK,04 Less deprived,138177
QKK,08 More deprived,357163
QKK,06 More deprived,226108
QKK,05 Less deprived,177307
QKK,07 More deprived,289960
QKK,09 More deprived,280471
QKK,02 Less deprived,132039
QKK,03 Less deprived,120800
QKK,10 Most deprived,20040
QKK,01 Least deprived,77206
QOP,04 Less deprived,185172
QOP,08 More deprived,345029
QOP,06 More deprived,211052
QOP,05 Less deprived,180271
QOP,07 More deprived,269284
QOP,09 More deprived,431077
QOP,02 Less deprived,183260
QOP,03 Less deprived,205596
QOP,10 Most deprived,710603
QOP,01 Least deprived,147912
QYG,04 Less deprived,201776
QYG,08 More deprived,210487
QYG,06 More deprived,187066
QYG,05 Less deprived,175364
QYG,07 More deprived,189637
QYG,09 More deprived,291138
QYG,02 Less deprived,205508
QYG,03 Less deprived,228612
QYG,10 Most deprived,581026
QYG,01 Least deprived,225943
QF7,04 Less deprived,130176
QF7,08 More deprived,170176
QF7,06 More deprived,125169
QF7,05 Less deprived,145715
QF7,07 More deprived,131679
QF7,09 More deprived,212269
QF7,02 Less deprived,97682
QF7,03 Less deprived,113224
QF7,10 Most deprived,337537
QF7,01 Least deprived,62852
QHM,04 Less deprived,233167
QHM,08 More deprived,371026
QHM,06 More deprived,268379
QHM,05 Less deprived,210846
QHM,07 More deprived,321927
QHM,09 More deprived,411601
QHM,02 Less deprived,238004
QHM,03 Less deprived,213519
QHM,10 Most deprived,544407
QHM,01 Least deprived,176436
QHL,04 Less deprived,51823
QHL,08 More deprived,119775
QHL,06 More deprived,98521
QHL,05 Less deprived,66556
QHL,07 More deprived,109773
QHL,09 More deprived,159778
QHL,02 Less deprived,44963
QHL,03 Less deprived,43662
QHL,10 Most deprived,412806
QHL,01 Least deprived,72910
QWU,04 Less deprived,113142
QWU,08 More deprived,99014
QWU,06 More deprived,99791
QWU,05 Less deprived,117415
QWU,07 More deprived,94686
QWU,09 More deprived,68018
QWU,02 Less deprived,91659
QWU,03 Less deprived,118102
QWU,10 Most deprived,66703
QWU,01 Least deprived,80924
QUA,04 Less deprived,69352
QUA,08 More deprived,154655
QUA,06 More deprived,103078
QUA,05 Less deprived,72278
QUA,07 More deprived,113120
QUA,09 More deprived,360243
QUA,02 Less deprived,53173
QUA,03 Less deprived,59276
QUA,10 Most deprived,352202
QUA,01 Least deprived,39127
QWO,04 Less deprived,210648
QWO,08 More deprived,269728
QWO,06 More deprived,212671
QWO,05 Less deprived,192319
QWO,07 More deprived,176423
QWO,09 More deprived,333716
QWO,02 Less deprived,146471
QWO,03 Less deprived,199144
QWO,10 Most deprived,527581
QWO,01 Least deprived,114893
QE1,04 Less deprived,174130
QE1,08 More deprived,165065
QE1,06 More deprived,129887
QE1,05 Less deprived,138725
QE1,07 More deprived,159110
QE1,09 More deprived,183228
QE1,02 Less deprived,150669
QE1,03 Less deprived,184162
QE1,10 Most deprived,310153
QE1,01 Least deprived,100470
QOQ,04 Less deprived,213407
QOQ,08 More deprived,118015
QOQ,06 More deprived,141597
QOQ,05 Less deprived,177336
QOQ,07 More deprived,125342
QOQ,09 More deprived,85702
QOQ,02 Less deprived,196742
QOQ,03 Less deprived,194994
QOQ,10 Most deprived,223709
QOQ,01 Least deprived,227492
QJ2,04 Less deprived,90537
QJ2,08 More deprived,102140
QJ2,06 More deprived,122021
QJ2,05 Less deprived,95364
QJ2,07 More deprived,85172
QJ2,09 More deprived,113010
QJ2,02 Less deprived,118722
QJ2,03 Less deprived,111397
QJ2,10 Most deprived,81696
QJ2,01 Least deprived,106367
QK1,04 Less deprived,122100
QK1,08 More deprived,118328
QK1,06 More deprived,94962
QK1,05 Less deprived,92536
QK1,07 More deprived,89856
QK1,09 More deprived,59804
QK1,02 Less deprived,142202
QK1,03 Less deprived,155236
QK1,10 Most deprived,74992
QK1,01 Least deprived,150290
QUY,04 Less deprived,121820
QUY,08 More deprived,76412
QUY,06 More deprived,80666
QUY,05 Less deprived,85185
QUY,07 More deprived,92199
QUY,09 More deprived,82721
QUY,02 Less deprived,92489
QUY,03 Less deprived,97524
QUY,10 Most deprived,83278
QUY,01 Least deprived,151228
QOX,04 Less deprived,113257
QOX,08 More deprived,40196
QOX,06 More deprived,87768
QOX,05 Less deprived,116335
QOX,07 More deprived,56353
QOX,09 More deprived,31956
QOX,02 Less deprived,156912
QOX,03 Less deprived,158695
QOX,10 Most deprived,24280
QOX,01 Least deprived,136165
QHG,04 Less deprived,116752
QHG,08 More deprived,87192
QHG,06 More deprived,101121
QHG,05 Less deprived,80496
QHG,07 More deprived,91807
QHG,09 More deprived,96399
QHG,02 Less deprived,134972
QHG,03 Less deprived,129600
QHG,10 Most deprived,28719
QHG,01 Least deprived,83816
QH8,04 Less deprived,138709
QH8,08 More deprived,116343
QH8,06 More deprived,135851
QH8,05 Less deprived,104870
QH8,07 More deprived,115058
QH8,09 More deprived,74569
QH8,02 Less deprived,154060
QH8,03 Less deprived,147429
QH8,10 Most deprived,46271
QH8,01 Least deprived,162188
QKS,04 Less deprived,235722
QKS,08 More deprived,167593
QKS,06 More deprived,240299
QKS,05 Less deprived,208403
QKS,07 More deprived,198411
QKS,09 More deprived,179241
QKS,02 Less deprived,168917
QKS,03 Less deprived,184864
QKS,10 Most deprived,119419
QKS,01 Least deprived,157242
QU9,04 Less deprived,202913
QU9,08 More deprived,64753
QU9,06 More deprived,96038
QU9,05 Less deprived,157011
QU9,07 More deprived,90583
QU9,09 More deprived,36757
QU9,02 Less deprived,296019
QU9,03 Less deprived,243188
QU9,10 Most deprived,8244
QU9,01 Least deprived,518640
QNQ,04 Less deprived,70201
QNQ,08 More deprived,46382
QNQ,06 More deprived,68452
QNQ,05 Less deprived,59828
QNQ,07 More deprived,70215
QNQ,09 More deprived,17213
QNQ,02 Less deprived,85505
QNQ,03 Less deprived,69213
QNQ,10 Most deprived,0
QNQ,01 Least deprived,257965
QNX,04 Less deprived,201638
QNX,08 More deprived,132145
QNX,06 More deprived,190771
QNX,05 Less deprived,261785
QNX,07 More deprived,147211
QNX,09 More deprived,91683
QNX,02 Less deprived,197053
QNX,03 Less deprived,211459
QNX,10 Most deprived,70832
QNX,01 Least deprived,200703
QRL,04 Less deprived,181963
QRL,08 More deprived,148595
QRL,06 More deprived,176647
QRL,05 Less deprived,176110
QRL,07 More deprived,197000
QRL,09 More deprived,128753
QRL,02 Less deprived,238476
QRL,03 Less deprived,207820
QRL,10 Most deprived,74766
QRL,01 Least deprived,294024
QUE,04 Less deprived,105114
QUE,08 More deprived,48320
QUE,06 More deprived,95773
QUE,05 Less deprived,109236
QUE,07 More deprived,56729
QUE,09 More deprived,77176
QUE,02 Less deprived,126891
QUE,03 Less deprived,117288
QUE,10 Most deprived,34651
QUE,01 Least deprived,121449
QT6,04 Less deprived,54367
QT6,08 More deprived,67104
QT6,06 More deprived,97095
QT6,05 Less deprived,77177
QT6,07 More deprived,165831
QT6,09 More deprived,44115
QT6,02 Less deprived,8418
QT6,03 Less deprived,30223
QT6,10 Most deprived,27472
QT6,01 Least deprived,0
QJK,04 Less deprived,139451
QJK,08 More deprived,118712
QJK,06 More deprived,167788
QJK,05 Less deprived,165402
QJK,07 More deprived,162730
QJK,09 More deprived,74490
QJK,02 Less deprived,123504
QJK,03 Less deprived,119853
QJK,10 Most deprived,74683
QJK,01 Least deprived,54126
QVV,04 Less deprived,116791
QVV,08 More deprived,42109
QVV,06 More deprived,98386
QVV,05 Less deprived,115015
QVV,07 More deprived,93326
QVV,09 More deprived,38549
QVV,02 Less deprived,84127
QVV,03 Less deprived,79807
QVV,10 Most deprived,26068
QVV,01 Least deprived,79661
QM7,04 Less deprived,158572
QM7,08 More deprived,82077
QM7,06 More deprived,172659
QM7,05 Less deprived,189767
QM7,07 More deprived,143650
QM7,09 More deprived,26110
QM7,02 Less deprived,217048
QM7,03 Less deprived,174234
QM7,10 Most deprived,1621
QM7,01 Least deprived,314049
QJG,04 Less deprived,117823
QJG,08 More deprived,79610
QJG,06 More deprived,119466
QJG,05 Less deprived,180886
QJG,07 More deprived,92984
QJG,09 More deprived,69067
QJG,02 Less deprived,104779
QJG,03 Less deprived,100567
QJG,10 Most deprived,47807
QJG,01 Least deprived,71195
QR1,04 Less deprived,86115
QR1,08 More deprived,28116
QR1,06 More deprived,54083
QR1,05 Less deprived,97017
QR1,07 More deprived,39604
QR1,09 More deprived,29023
QR1,02 Less deprived,104480
QR1,03 Less deprived,84532
QR1,10 Most deprived,19593
QR1,01 Least deprived,94507
QJM,04 Less deprived,87508
QJM,08 More deprived,78211
QJM,06 More deprived,69632
QJM,05 Less deprived,95347
QJM,07 More deprived,101118
QJM,09 More deprived,59025
QJM,02 Less deprived,92773
QJM,03 Less deprived,76164
QJM,10 Most deprived,50635
QJM,01 Least deprived,50811
QMM,04 Less deprived,96755
QMM,08 More deprived,89638
QMM,06 More deprived,164886
QMM,05 Less deprived,152372
QMM,07 More deprived,141710
QMM,09 More deprived,78072
QMM,02 Less deprived,72355
QMM,03 Less deprived,89999
QMM,10 Most deprived,84774
QMM,01 Least deprived,55632
QPM,04 Less deprived,74893
QPM,08 More deprived,89117
QPM,06 More deprived,54022
QPM,05 Less deprived,76474
QPM,07 More deprived,50226
QPM,09 More deprived,64340
QPM,02 Less deprived,90729
QPM,03 Less deprived,108796
QPM,10 Most deprived,41176
QPM,01 Least deprived,86446
QT1,04 Less deprived,79521
QT1,08 More deprived,95173
QT1,06 More deprived,90539
QT1,05 Less deprived,103137
QT1,07 More deprived,102053
QT1,09 More deprived,147945
QT1,02 Less deprived,95071
QT1,03 Less deprived,77968
QT1,10 Most deprived,140547
QT1,01 Least deprived,111711
QOC,04 Less deprived,68082
QOC,08 More deprived,30567
QOC,06 More deprived,73242
QOC,05 Less deprived,68450
QOC,07 More deprived,86789
QOC,09 More deprived,28057
QOC,02 Less deprived,32679
QOC,03 Less deprived,53710
QOC,10 Most deprived,31722
QOC,01 Least deprived,29692
QSL,04 Less deprived,83174
QSL,08 More deprived,35921
QSL,06 More deprived,97675
QSL,05 Less deprived,95131
QSL,07 More deprived,81096
QSL,09 More deprived,33577
QSL,02 Less deprived,33883
QSL,03 Less deprived,56726
QSL,10 Most deprived,14167
QSL,01 Least deprived,30875
QNC,04 Less deprived,123911
QNC,08 More deprived,110010
QNC,06 More deprived,96180
QNC,05 Less deprived,132187
QNC,07 More deprived,101025
QNC,09 More deprived,115816
QNC,02 Less deprived,132501
QNC,03 Less deprived,131788
QNC,10 Most deprived,100428
QNC,01 Least deprived,92089
QXU,04 Less deprived,116101
QXU,08 More deprived,19247
QXU,06 More deprived,68550
QXU,05 Less deprived,84293
QXU,07 More deprived,65506
QXU,09 More deprived,6130
QXU,02 Less deprived,206431
QXU,03 Less deprived,155246
QXU,10 Most deprived,0
QXU,01 Least deprived,327666
QGH,04 Less deprived,89049
QGH,08 More deprived,56996
QGH,06 More deprived,139446
QGH,05 Less deprived,91167
QGH,07 More deprived,66837
QGH,09 More deprived,60883
QGH,02 Less deprived,84353
QGH,03 Less deprived,99705
QGH,10 Most deprived,29796
QGH,01 Least deprived,70355
"""
df = pd.read_csv(io.StringIO(data), header=0, delimiter=',').astype(str)
spark.createDataFrame(df).write.insertInto(f"{db_output}.stp_imd_pop")