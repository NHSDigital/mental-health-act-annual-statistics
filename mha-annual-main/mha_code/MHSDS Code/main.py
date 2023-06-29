# Databricks notebook source
import pandas as pd

# COMMAND ----------

#import mhsds_functions to get logic for MHRunParameters dataclass

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

#initialise widgets
dbutils.widgets.text("db_output", "output_database") #output database
dbutils.widgets.text("db_source", "mh_database") #mhsds database
dbutils.widgets.text("status", "Final") #mhsds submission window
dbutils.widgets.text("rp_enddate", "2022-03-31") #end date of the financial year
dbutils.widgets.text("year", "2021/22") #financial year string

#get widgets as python variables
db_output = dbutils.widgets.get("db_output")
status = dbutils.widgets.get("status")
rp_enddate = dbutils.widgets.get("rp_enddate")
year = dbutils.widgets.get("year") # year widget is exclusive to mha annual publication

# COMMAND ----------

#initlise MHRunParameters
mha_annual = MHRunParameters(db_output, db_source, status, rp_enddate) #db_output, db_source and rp_enddate required to initialise MHRunParameters dataclass

#create custom parameter json for mha run
mha_annual_params = {
  "db_output": mha_annual.db_output, #output database
  "db_source": mha_annual.db_source, #mhsds database
  "rp_enddate": mha_annual.rp_enddate, #end date of the financial year
  "rp_startdate": mha_annual.rp_startdate_12m, #start month of the financial year
  "start_month_id": mha_annual.start_month_id, #uniqmonthid in mhs000header for start month of financial year
  "month_id": mha_annual.end_month_id, #uniqmonthid in mhs000header for end month of financial year
  "year": year #financial year string
}

# COMMAND ----------

#run "All Tables" notebook
#gets all required corporate reference data and filters the required mhsds base tables to be required financial year only
dbutils.notebook.run("all_tables", 0, mha_annual_params)

# COMMAND ----------

#run "Population" notebook
#gets all required population figures at different geographical and demographic breakdowns which are required for standardisation and crude rates methodology
dbutils.notebook.run("population", 0, mha_annual_params)

# COMMAND ----------

#run "Annual_Measures_Prep" and "Monthly_Measures_Prep" notebooks
#gets all final tables which will be used for aggregate counts for different measures and breakdowns
#these were split into 2 notebooks due massive notebook cell output
dbutils.notebook.run("prep/annual_measures_prep", 0, mha_annual_params)
dbutils.notebook.run("prep/monthly_measures_prep", 0, mha_annual_params)

#adds ecds data into pipeline (ecds data is processed outside of DAE in SQL Server)
dbutils.notebook.run("prep/ecds_prep", 0, mha_annual_params)

# COMMAND ----------

#aggregate figures for all detentions, detentions on and following admission, place of safety orders, following revocation of community treatment order 
#by different sections of the act, changes in legal status during detention, geographical and demographic breakdowns
dbutils.notebook.run("agg/table1_agg", 0, mha_annual_params)

#aggregate figures for short term orders by different sections of the act, geographical and demographic breakdowns
dbutils.notebook.run("agg/table2_agg", 0, mha_annual_params)

#aggregate figures for all uses, revocations, discharges, recalls of community treatment orders by different sections of the act, geographical and demographic breakdowns
dbutils.notebook.run("agg/table2_agg", 0, mha_annual_params)

#aggregate figures for uses of section 2 and section 3 by different sections of the act, geographical and demographic breakdowns
dbutils.notebook.run("agg/table2_agg", 0, mha_annual_params)

#aggregate figures for transfers on section by geographical breakdowns
dbutils.notebook.run("agg/transfers_agg", 0, mha_annual_params)

#aggregate figures for people subject to repeat detentions by detention count and demographic breakdowns
dbutils.notebook.run("agg/repeat_detentions_agg", 0, mha_annual_params)

#aggregate figures for discharges following detention by geographical and demographic breakdowns
dbutils.notebook.run("agg/discharges_following_detention_agg", 0, mha_annual_params)

#aggregate monthly people subject to the act figures (as of March 31st) by geographical and demographic breakdowns
dbutils.notebook.run("agg/end_of_fy_year_agg", 0, mha_annual_params)

#aggregate mental health act detentions length of stay figures by demographic breakdowns
dbutils.notebook.run("agg/mha_los_only_agg", 0, mha_annual_params)

#aggregate mental health act detentions and community treatment orders length of stay figures by demographic breakdowns
dbutils.notebook.run("agg/mha_cto_los_agg", 0, mha_annual_params)

#aggregate ecds (emergency care) detentions for all detentions, detentions on admission to hospital, detentions under part 2, 3, section 2, 3, 35, 36, 37, 37(41 restrictions), 45A, 47, 47(49 restrictions), 48, 48(49 restrictions) and other acts detentions on admissions to hospital, short term orders, place of safety orders, section 136, section 5, section 5(2), section 5(4) short term orders
dbutils.notebook.run("agg/ecds_agg", 0, mha_annual_params)

# COMMAND ----------

#aggregate all detentions standardised rates for upper/lower ethnicity
dbutils.notebook.run("rates/eth_detentions_sr", 0, mha_annual_params)

#aggregate short term orders standardised rates for upper/lower ethnicity
dbutils.notebook.run("rates/eth_stos_sr", 0, mha_annual_params)

#aggregate community treatment orders standardised rates for upper/lower ethnicity
dbutils.notebook.run("rates/eth_ctos_sr", 0, mha_annual_params)

#aggregate discharges standardised rates for upper/lower ethnicity
dbutils.notebook.run("rates/eth_discharges_sr", 0, mha_annual_params)

#aggregate all detentions crude rates for gender
dbutils.notebook.run("rates/gender_detentions_cr", 0, mha_annual_params)

#aggregate short term orders crude rates for gender
dbutils.notebook.run("rates/gender_stos_cr", 0, mha_annual_params)

#aggregate community treatment orders crude rates for gender
dbutils.notebook.run("rates/gender_ctos_cr", 0, mha_annual_params)

#aggregate discharges crude rates for gender
dbutils.notebook.run("rates/gender_deischarges_cr", 0, mha_annual_params)

#aggregate all detentions crude rates for age band
dbutils.notebook.run("rates/age_detentions_cr", 0, mha_annual_params)

#aggregate short term orders crude rates for age band
dbutils.notebook.run("rates/age_stos_cr", 0, mha_annual_params)

#aggregate community treatment orders crude rates for age band
dbutils.notebook.run("rates/age_ctos_cr", 0, mha_annual_params)

#aggregate discharges crude rates for age band
dbutils.notebook.run("rates/age_discharges_cr", 0, mha_annual_params)

#aggregate all detentions crude rates for deprivation
dbutils.notebook.run("rates/imd_detentions_cr", 0, mha_annual_params)

#aggregate all detentions crude rates for ccg/stp
#currently using 2020 ccg/stp Population data
dbutils.notebook.run("rates/ccg_detentions_cr", 0, mha_annual_params)
dbutils.notebook.run("rates/stp_detentions_cr", 0, mha_annual_params)

#adding ecds data to detentions and short term orders count to use for rates
dbutils.notebook.run("rates/all_cr", 0, mha_annual_params)

#combine all rates for 4 measures into single table
dbutils.notebook.run("rates/rates_combine", 0, mha_annual_params)

# COMMAND ----------

#combine aggregate counts and rates into a single output
dbutils.notebook.run("output", 0, mha_annual_params)

# COMMAND ----------

#aggregate data quality outputs used for Power BI and publication report
dbutils.notebook.run("dq", 0, mha_annual_params)

# COMMAND ----------

#check figures in aggregate output and prep tables 
dbutils.notebook.run("../tests/datatests/output_checking", 0, mha_annual_params)

# COMMAND ----------

 %md
 ### 2020-21 MHSDS Detentions: 51,759 ECDS Detentions: 1480 Total Detentions: 53,239 24Prov Detentions: 24,604
 ### 2021-22 MHSDS Detentions: 51,128 ECDS Detentions: 2209 Total Detentions: 53,337 24Prov Detentions: 23,207

# COMMAND ----------

 %sql
 ---detentions from 24 providers who submitted to kp90 in 2015-16 (needed in html publication text)
 select count(distinct UniqMHActEpisodeID) from $db_output.detentions
 where OrgIDProv IN ("NR5", "RTQ", "RVN", "RRP", "RXT", "TAD", "TAF", "RJ8", "RXM", "RMV", "RWK", "RV9", "RP7", "RMY", "RX4", "RHA", "TAH", "RRE", "RQY", "RW1", "RX2", "RX3", "RKL", "R1A", "RWV")
 and MHA_Logic_Cat_full in ("A","B","C","D","P")

# COMMAND ----------

 %sql
 ---suppressed main published csv
 SELECT 
 Year,
 Measure,
 measureSubcategory,
 Demographic,
 DemographicBreakdown,
 OrganisationBreakdown,
 Datasource,
 OrgID,
 OrgName,
 MHSDS_Count,
 Acute_Count,
 Total_Count,
 CrudeRate,
 StdRate,
 Confidence_interval            
 FROM
 $db_output.mha_suppressed
 order by Measure, measureSubcategory, Demographic, DemographicBreakdown, OrganisationBreakdown, OrgID

# COMMAND ----------

 %sql
 refresh table $db_output.mha_los_output;
 refresh table $db_output.mha_los_cto_output;
 ---suppressed length of stay published csv
 select "MHA only" as section,
 Geography,
 ORGCODE,
 ORGNAME,
 MHA_MOST_SEVERE_CATEGORY,
 MHA_Most_severe_Section,
 DEMOGRAPHIC,
 DEMOGRAPHIC_CATEGORY,
 COUNT,
 CASE WHEN COUNT < 5 THEN "*" ELSE LOWER_QUARTILE END AS LOWER_QUARTILE,
 CASE WHEN COUNT < 5 THEN "*" ELSE MEDIAN END AS MEDIAN,
 CASE WHEN COUNT < 5 THEN "*" ELSE UPPER_QUARTILE END AS UPPER_QUARTILE
 from $db_output.mha_los_output
 union all
 select "MHA and CTO" as section,
 Geography,
 ORGCODE,
 ORGNAME,
 MHA_MOST_SEVERE_CATEGORY,
 MHA_Most_severe_Section,
 DEMOGRAPHIC,
 DEMOGRAPHIC_CATEGORY,
 COUNT,
 CASE WHEN COUNT < 5 THEN "*" ELSE LOWER_QUARTILE END AS LOWER_QUARTILE,
 CASE WHEN COUNT < 5 THEN "*" ELSE MEDIAN END AS MEDIAN,
 CASE WHEN COUNT < 5 THEN "*" ELSE UPPER_QUARTILE END AS UPPER_QUARTILE
 from $db_output.mha_los_cto_output

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.mha_spell_number;
 REFRESH TABLE $db_output.mha_spell_number_cto;
 ---mha episodes csv (used for Excel Tables)
 SELECT 
 "MHA only" as section,
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN "10+"
   ELSE EPISODE_COUNT
   END AS EPISODE_COUNT,
 COUNT(*) as COUNT
 FROM 
 $db_output.mha_spell_number
 WHERE 
 END_DATE BETWEEN "$rp_startdate" and "$rp_enddate"
 AND START_DATE > "1900-01-01"
 GROUP BY 
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN "10+"
   ELSE EPISODE_COUNT
   END
 UNION ALL
 SELECT
 "MHA and CTO" as section,
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN "10+"
   ELSE EPISODE_COUNT
   END AS EPISODE_COUNT,
 COUNT(*) as COUNT
 FROM 
 $db_output.mha_spell_number_cto
 WHERE 
 END_DATE BETWEEN "$rp_startdate" and "$rp_enddate"
 AND START_DATE > "1900-01-01"
 GROUP BY 
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN "10+"
   ELSE EPISODE_COUNT
   END
 ORDER BY section, EPISODE_COUNT ASC

# COMMAND ----------

 %sql
 refresh table $db_output.mha_final_rates;
 ---mha rates csv (used for Excel Tables)
 select
 breakdown, primary_level, primary_level_desc, secondary_level, secondary_level_desc, count_of, count, population, CR, SR, CI
 from $db_output.mha_final_rates order by count_of, breakdown, primary_level, secondary_level

# COMMAND ----------

 %sql
 refresh table $db_output.mha_table_1e_final_rates;
 ---mha table 1e csv (used for Power BI and Excel Tables)
 select * from $db_output.mha_table_1e_final_rates where Ethnic6 is not null
 order by Age, Ethnic6, Der_Gender

# COMMAND ----------

 %sql
 refresh table $db_output.mha_table_1h;
 ---mha table 1h csv (used for Excel Tables)
 select * from $db_output.mha_table_1h

# COMMAND ----------

 %sql
 refresh table $db_output.mha_mhs08_additional;
 ---mha table 5 csv (used for Excel Tables)
 select * from $db_output.mha_mhs08_additional

# COMMAND ----------

 %sql
 refresh table $db_output.dq_mha_prov_type_subm_detentions_year;
 ---number of providers submitting and detentions by provider type for the financial year (used in html publication chart)
 select * from $db_output.dq_mha_prov_type_subm_detentions_year

# COMMAND ----------

 %sql
 refresh table $db_output.dq_mha_prov_type_subm_year;
 ---mha dq table 1a (used in html publication)
 select
 his.Org_Type as `Organisation Type`,
 his.T1a_2015_16 as `2015-16`,
 his.T1a_2016_17 as `2016-17`,
 his.T1a_2017_18 as `2017-18`,
 his.T1a_2018_19 as `2018-19`,
 his.T1a_2019_20 as `2019-20`,
 his.T1a_2020_21 as `2020-21`,
 y.providers as `2021-22`
 from $db_output.dq_table_1a_history his
 left join $db_output.dq_mha_prov_type_subm_year y on his.Org_Type = y.Org_Type

# COMMAND ----------

 %sql
 refresh table $db_output.dq_mha_prov_type_det_year;
 ---mha dq table 1b (used in html publication)
 select
 his.Org_Type as `Organisation Type`,
 his.T1b_2015_16 as `2015-16`,
 his.T1b_2016_17 as `2016-17`,
 his.T1b_2017_18 as `2017-18`,
 his.T1b_2018_19 as `2018-19`,
 his.T1b_2019_20 as `2019-20`,
 his.T1b_2020_21 as `2020-21`,
 y.detentions as `2021-22`
 from $db_output.dq_table_1b_history his
 left join $db_output.dq_mha_prov_type_det_year y on his.Org_Type = y.Org_Type

# COMMAND ----------

spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_prov_submissions_org_type")
#mha dq table 2 (used in html publication)
dq2 = spark.table(f"{mha_annual.db_output}.dq_prov_submissions_org_type")
dq2_p = dq2.toPandas()
dq2_p = dq2_p.astype({"UniqMonthID": "object", "ReportingPeriodStartDate": "datetime64[ns]", "Providers_submitting": "int32", "NHS_SUBMITTING": "int32", "IND_SUBMITTING": "int32"})
dq2_p["Month_Year"] =  dq2_p["ReportingPeriodStartDate"].dt.strftime("%b-%Y")
dq2_allprov_pivot = pd.pivot_table(dq2_p, values="Providers_submitting", columns="Month_Year")
dq2_allprov_pivot["Provider Type"] = "All"
dq2_nhsprov_pivot = pd.pivot_table(dq2_p, values="NHS_SUBMITTING", columns="Month_Year")
dq2_nhsprov_pivot["Provider Type"] = "NHS"
dq2_indprov_pivot = pd.pivot_table(dq2_p, values="IND_SUBMITTING", columns="Month_Year")
dq2_indprov_pivot["Provider Type"] = "ISP"
dq2_final = pd.concat([dq2_allprov_pivot, dq2_nhsprov_pivot, dq2_indprov_pivot])
dq2_final = dq2_final[["Provider Type", "Apr-2021", "May-2021", "Jun-2021", "Jul-2021", "Aug-2021", "Sep-2021", "Oct-2021", "Nov-2021", "Dec-2021", "Jan-2022", "Feb-2022", "Mar-2022"]]
display(dq2_final)

# COMMAND ----------

 %sql
 refresh table $db_output.dq_mha_submissions_12m;
 ---mha submissions over 12 months (used in html publication chart)
 select 
 OrgIDProvider,
 NAME as ProviderName,
 case when ORG_TYPE_CODE is not null then ORG_TYPE_CODE
      when (length(OrgIDProvider) = 3 and left(OrgIDProvider, 1) in ("R", "T")) then "NHS TRUST"
      else "INDEPENDENT HEALTH PROVIDER" end as OrgType,
 case when MHA_SUBMISSIONS = 12 then "All 12 months"
      else "Less than 12 months" end as Submissions
 from $db_output.dq_mha_submissions_12m 
 where MHA_SUBMISSIONS >= 1
 order by NAME

# COMMAND ----------

spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_table_3_prep3")
#mha dq table 3 (used in html publication)
dq3 = spark.table(f"{mha_annual.db_output}.dq_table_3_prep3")
dq3_p = dq3.toPandas()
dq3_p = dq3_p[dq3_p["level"] != "Unknown"]
dq3_pivot = pd.pivot_table(dq3_p, values="detentions", index=["breakdown","level"], columns="OrgType", fill_value=0)
dq3_pivot = dq3_pivot.fillna(0)
dq3_pivot = dq3_pivot.reset_index()
dq3_age_pivot = dq3_pivot[dq3_pivot["breakdown"] == "Age"]
dq3_age_pivot["All %"] = round((dq3_age_pivot["All"] / dq3_age_pivot["All"].sum()), 3)
dq3_age_pivot["Independent %"] = round((dq3_age_pivot["Independent"] / dq3_age_pivot["Independent"].sum()), 3)
dq3_age_pivot["NHS %"] = round((dq3_age_pivot["NHS"] / dq3_age_pivot["NHS"].sum()), 3)
dq3_gen_pivot = dq3_pivot[dq3_pivot["breakdown"] == "Gender"]
dq3_gen_pivot["All %"] = round((dq3_gen_pivot["All"] / dq3_gen_pivot["All"].sum()), 3)
dq3_gen_pivot["Independent %"] = round((dq3_gen_pivot["Independent"] / dq3_gen_pivot["Independent"].sum()), 3)
dq3_gen_pivot["NHS %"] = round((dq3_gen_pivot["NHS"] / dq3_gen_pivot["NHS"].sum()), 3)
dq3_eth_pivot = dq3_pivot[dq3_pivot["breakdown"] == "Ethnicity"]
dq3_eth_pivot["All %"] = round((dq3_eth_pivot["All"] / dq3_eth_pivot["All"].sum()), 3)
dq3_eth_pivot["Independent %"] = round((dq3_eth_pivot["Independent"] / dq3_eth_pivot["Independent"].sum()), 3)
dq3_eth_pivot["NHS %"] = round((dq3_eth_pivot["NHS"] / dq3_eth_pivot["NHS"].sum()), 3)
dq3_final_pivot = pd.concat([dq3_gen_pivot, dq3_age_pivot, dq3_eth_pivot])
display(dq3_final_pivot)

# COMMAND ----------

spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_mha_time_recording")
#mha dq table 5 (used in html publication)
dq5 = spark.table(f"{mha_annual.db_output}.dq_mha_time_recording")
dq5_p = dq5.toPandas()
dq5_p["MHA_Count_Perc"] = round((dq5_p["MHA_Count"] / dq5_p["MHA_Count"].sum()), 3)
dq5_p["Hosp_Count_Perc"] = round((dq5_p["Hosp_Count"] / dq5_p["Hosp_Count"].sum()), 3)
dq5_final = dq5_p[["time_recording", "MHA_Count_Perc", "Hosp_Count_Perc"]]
dq5_final.columns = ["Times recorded", "Mental Health Act episode %", "Hospital provider spell %"]
display(dq5_final)

# COMMAND ----------

 %sql
 refresh table $db_output.dq_table_6_year;
 ---mha dq table 6 (used in html publication)
 select 
 his.Detention_Type,
 his.T6_2017_18 as `2017-18`,
 his.T6_2018_19 as `2018-19`,
 his.T6_2019_20 as `2019-20`,
 his.T6_2020_21 as `2020-21`,
 y.count as `2021-22`
 from $db_output.dq_table_6_history his
 left join $db_output.dq_table_6_year y on his.Detention_Type = y.Detention_Type

# COMMAND ----------

 %sql
 refresh table $db_output.dq_mha_submissions_12m;
 ---mha dq table 7 (used in html publication)
 select 
 sub.OrgIDProvider as `Org code`,
 sub.NAME as `Organisation Name`,
 sub.MHA_SUBMISSIONS as `Number of monthly submissions`,
 case when kp90.Org_Code is null then "No" else "Yes" end as `Submitted to KP90 in 2015-16?`,
 case when sub.ORG_TYPE_CODE = "NHS TRUST" then "NHS Facilities" else "Independent" end as `Organisation Type`
 from $db_output.dq_mha_submissions_12m sub
 left join $db_output.dq_kp90_submitters kp90 on sub.OrgIDProvider = kp90.Org_Code
 where MHA_SUBMISSIONS between 1 and 11
 order by sub.OrgIDProvider

# COMMAND ----------

 %sql
 ---mha dq table 8 (used in html publication)
 select 
 ecds.Organisation_Code as `Org Code`, 
 ecds.Organisation_Name as `Organisation Name`,
 case when kp90.Org_Code is null then "No" else "Yes" end as `Submitted to KP90 in 2015-16?`
 from $db_output.ecds ecds
 left join $db_output.dq_kp90_submitters kp90 on ecds.Organisation_Code = kp90.Org_Code
 order by ecds.Organisation_Code

# COMMAND ----------

 %sql
 ---mha dq table 9 (used in html publication)
 select
 kp90.Org_Code as `Org Code`,
 kp90.Org_Name as `Org Name`,
 kp90.Detentions as `All detentions: 2015-16`,
 case when (length(kp90.Org_Code) = 3 and left(kp90.Org_Code, 1) in ("R", "T")) then "NHS Facilities" else "Independent" end as `Org Type`
 from $db_output.dq_kp90_submitters kp90
 left join $db_output.mha_raw mha on kp90.Org_Code = mha.OrgID
 where mha.OrgID is null
 order by case when (length(kp90.Org_Code) = 3 and left(kp90.Org_Code, 1) in ("R", "T")) then "NHS Facilities" else "Independent" end desc, kp90.Org_Name

# COMMAND ----------

 %sql
 ---mha dq table 10 (used in html publication)
 ---to calculate estimated change
 select 
 y.OrgIDProv as `Org Code`,
 od.NAME as `Org Name`,
 case when his.Org_Type = "MH" then "NHS Facilities" else his.Org_Type end as `Org Type`,
 his.T5_2015_16 as `People detained on 31st March 2016`,
 his.T5_2016_17 as `People detained on 31st March 2017`,
 his.T5_2017_18 as `People detained on 31st March 2018`,
 his.T5_2018_19 as `People detained on 31st March 2019`,
 his.T5_2019_20 as `People detained on 31st March 2020`,
 his.T5_2020_21 as `People detained on 31st March 2021`,
 y.mhs08 as `People detained on 31st March 2022`
 from $db_output.dq_table_10_history his
 left join $db_output.mha_dq_table10_year y on his.Org_Code = y.OrgIDProv
 left join global_temp.RD_ORG_DAILY_LATEST od on y.OrgIDProv = od.ORG_CODE
 where OrgIDProv IN ("NR5", "RTQ", "RVN", "RRP", "RXT", "TAD", "TAF", "RJ8", "RXM", "RMV", "RWK", "RV9", "RP7", "RMY", "RX4", "RHA", "TAH", "RRE", "RQY", "RW1", "RX2", "RX3", "RKL", "R1A", "RWV")
 order by case when his.Org_Type = "MH" then "NHS Facilities" else his.Org_Type end, od.NAME

# COMMAND ----------

 %sql
 refresh table $db_output.mha_final_rates;
 ---mha power bi rates csv (used for Power BI)
 select
 replace("$year", "/", "-") as year,
 breakdown, primary_level, primary_level_desc, secondary_level, secondary_level_desc, count_of, count, CR
 from $db_output.mha_final_rates 
 where breakdown != "CCG"
 order by count_of, breakdown, primary_level, secondary_level

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.mha_pbi_dq_table2;
 ---mha power bi dq table 2 csv (used for Power BI)
 SELECT * FROM $db_output.mha_pbi_dq_table2
 ORDER BY Org_Code

# COMMAND ----------

def suppress(x, base=5):   
    if x < 5:
        return ''
    else:
        return int(base * round(float(x)/base))
spark.udf.register("suppress", suppress)

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.mha_pbi_dq_table3;
 ---mha power bi dq table 3 csv (used for Power BI)
 SELECT 
 ORGANISATION_CODE as `Organisation Code`, 
 ORGANISATION_NAME as `Organisation Name`, 
 TABLE_NAME as `Table Name`, 
 VALUES_STRING as `Values`, 
 CASE WHEN ORGANISATION_CODE = "England" THEN Records ELSE suppress(Records) end as Records, ---suppress sub-national counts
 PERIOD_NAME as `Period Name`, 
 PERIOD_END_DATE as `Period End Date`, 
 CONCAT(ORGANISATION_NAME, ' - ', ORGANISATION_CODE) AS `Organisation ID`,
 replace("$year", "/", "-") as `Reporting Year`
 FROM $db_output.mha_pbi_dq_table3 
 WHERE TABLE_NAME != 'Any'
 ORDER BY PERIOD_END_DATE, ORGANISATION_NAME, TABLE_NAME