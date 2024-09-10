# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Get MHRunParameters Class 
 %run mhsds_code/functions/parameter_functions

# COMMAND ----------

#initialise widgets
dbutils.widgets.text("db_output", "output_database") #output database
dbutils.widgets.text("db_source", "mh_database") #mhsds database
dbutils.widgets.text("rp_enddate", "2022-03-31") #end date of the financial year
dbutils.widgets.text("year", "2021/22") #financial year string

#get widgets as python variables
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")
year = dbutils.widgets.get("year") # year widget is exclusive to MHA Annual Publication

# COMMAND ----------

# initlise MHRunParameters
mha_annual = MHRunParameters(db_output, db_source, rp_enddate)

# create custom parameter json for mha run
mha_annual_params = {
  "db_output": mha_annual.db_output, #output database
  "db_source": mha_annual.db_source, #mhsds database
  "rp_enddate": mha_annual.rp_enddate, #end date of the financial year
  "rp_startdate": mha_annual.rp_startdate_12m, #start month of the financial year
  "start_month_id": mha_annual.start_month_id, #month_id in mhs000header for start month of financial year
  "month_id": mha_annual.end_month_id, #month_id in mhs000header for end month of financial year
  "year": year #financial year string
}
mha_annual_params

# COMMAND ----------

# DBTITLE 1,Drop/Create Tables needed for MHA Annual (including reference_database reference data)
#run "All Tables" notebook
dbutils.notebook.run("All Tables", 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Get Population data for Crude/Standardisation Rates calculations
#run "Population" notebook
# dbutils.notebook.run('Population', 0, mha_annual_params)
#dbutils.notebook.run('Population_2021census', 0, mha_annual_params) # notebook for for 2021/22 and 22/23
dbutils.notebook.run('Population_2021census_2324', 0, mha_annual_params) # notebook for for 23-24

# COMMAND ----------

# DBTITLE 1,Create Prep Tables for MHA 2022
#run "Annual_Measures_Prep" and "Monthly_Measures_Prep" notebooks
#these were split into 2 notebooks due massive notebook cell output
dbutils.notebook.run("Prep/Annual_Measures_Prep", 0, mha_annual_params)
dbutils.notebook.run("Prep/Monthly_Measures_Prep", 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Prep Tests
dbutils.notebook.run('Tests/Prep_Tests', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Aggregate Table 1, 2, 3, 4, Transfers, Repeat Detentions and Discharges following Detention Figures for MHA 2022
dbutils.notebook.run('Agg/MHA Annual - T1', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - T2', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - T3', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - T4', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - Transfers', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - Repeat Detentions', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - Discharges following detention', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - March 31st', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Aggregate MHA only and MHA + CTO Length of Stay Figures for MHA 2022
dbutils.notebook.run('Agg/MHA Annual - MHA LOS', 0, mha_annual_params)
dbutils.notebook.run('Agg/MHA Annual - MHA + CTO LOS', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Gather ECDS and Aggregate data
#1967 when counting latest RECORD_IDENTIFIER
#2209 when counting all RECORD_IDENTIFIERs (duplicates included)
dbutils.notebook.run('ECDS/MHA Annual - ECDS data', 0, mha_annual_params)
dbutils.notebook.run('ECDS/MHA Annual - ECDS agg', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Crude/Standardisation Rates Aggregation for Ethnicity, Gender, Age, IMD, CCG, STP then combine for MHA 2022 Figures
###currently using mid-year 2020 ethnicity/gender/age pop data and 2017 imd pop data
dbutils.notebook.run('Rates/Ethnicity - Detentions Standardisation', 0, mha_annual_params)
dbutils.notebook.run('Rates/Ethnicity - Short Term Orders Standardisation', 0, mha_annual_params)
dbutils.notebook.run('Rates/Ethnicity - CTOs Standardisation', 0, mha_annual_params)
dbutils.notebook.run('Rates/Ethnicity - Discharges Standardisation', 0, mha_annual_params)
dbutils.notebook.run('Rates/Gender - Detentions Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Gender - Short Term Orders Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Gender - CTOs Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Gender - Discharges Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Age - Detentions Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Age - Short Term Orders Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Age - CTOs Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/Age - Discharges Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/IMD - Detentions Crude', 0, mha_annual_params)

#currently using 2020 CCG/STP Population data
dbutils.notebook.run('Rates/CCG - Detentions Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/STP - Detentions Crude', 0, mha_annual_params)
dbutils.notebook.run('Rates/STP - Short Term Orders Crude', 0, mha_annual_params)

#Adding ECDS data to Detentions and Short Term Orders count to use for rates
dbutils.notebook.run('Rates/All - Crude', 0, mha_annual_params)

dbutils.notebook.run('Rates/Rates - Combine', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Combine Aggregation/Rates into a single output
dbutils.notebook.run('Output', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Agg Tests
dbutils.notebook.run('Tests/Agg_Tests', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Data Quality
dbutils.notebook.run('DQ', 0, mha_annual_params)

# COMMAND ----------

# DBTITLE 1,Detentions from 24 Providers who submitted to KP90 in 2015-16
 %sql
 select count(distinct UniqMHActEpisodeID) from $db_output.detentions
 where OrgIDProv IN ("NR5", "RTQ", "RVN", "RRP", "RXT", "TAD", "TAF", "RJ8", "RXM", "RMV", "RWK", "RV9", "RP7", "RMY", "RX4", "RHA", "TAH", "RRE", "RQY", "RW1", "RX2", "RX3", "RKL", "R1A", "RWV")
 and MHA_Logic_Cat_full in ('A','B','C','D','P')

# COMMAND ----------

 %md
 ### 2020-21 MHSDS Detentions: 51,759 ECDS Detentions: 1480 Total Detentions: 53,239 24Prov Detentions: 24,604
 ### 2021-22 MHSDS Detentions: 51,128 ECDS Detentions: 2209 Total Detentions: 53,337 24Prov Detentions: 23,207
 ### 2022-23 MHSDS Detentions: 49.163 ECDS Detentions: 2149 Total Detentions: 51,312 24Prov Detentions: 21,411

# COMMAND ----------

# DBTITLE 1,Suppressed Main CSV Output
 %sql
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

# DBTITLE 1,Length of Stay Final Output
 %sql
 refresh table $db_output.mha_los_output;
 refresh table $db_output.mha_los_cto_output;
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

# DBTITLE 1,MHA LOS and MHA + CTO Episode Counts Output
 %sql
 REFRESH TABLE $db_output.mha_spell_number;
 REFRESH TABLE $db_output.mha_spell_number_cto;
 SELECT 
 "MHA only" as section,
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN '10+'
   ELSE EPISODE_COUNT
   END AS EPISODE_COUNT,
 COUNT(*) as COUNT
 FROM 
 $db_output.mha_spell_number
 WHERE 
 END_DATE BETWEEN '$rp_startdate' and '$rp_enddate'
 AND START_DATE > '1900-01-01'
 GROUP BY 
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN '10+'
   ELSE EPISODE_COUNT
   END
 UNION ALL
 SELECT
 "MHA and CTO" as section,
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN '10+'
   ELSE EPISODE_COUNT
   END AS EPISODE_COUNT,
 COUNT(*) as COUNT
 FROM 
 $db_output.mha_spell_number_cto
 WHERE 
 END_DATE BETWEEN '$rp_startdate' and '$rp_enddate'
 AND START_DATE > '1900-01-01'
 GROUP BY 
 CASE 
   WHEN EPISODE_COUNT >= 10 THEN '10+'
   ELSE EPISODE_COUNT
   END
 ORDER BY section, EPISODE_COUNT ASC

# COMMAND ----------

# DBTITLE 1,Rates Final Output
 %sql
 refresh table $db_output.mha_final_rates;
 select
 demographic_breakdown, organisation_breakdown, primary_level, primary_level_desc, secondary_level, secondary_level_desc, count_of, sub_measure, count, population, CR, SR, CI
 from $db_output.mha_final_rates where primary_level is not null
 order by count_of, demographic_breakdown, organisation_breakdown, primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,Cross Tab Output (also PBI Output)
 %sql
 refresh table $db_output.mha_table_1e_final_rates;
 select * from $db_output.mha_table_1e_final_rates where Ethnic6 is not null
 order by Age, Ethnic6, Der_Gender

# COMMAND ----------

# DBTITLE 1,Ethnicity and Deprivation Output
 %sql
 refresh table $db_output.mha_table_1h;
 select * from $db_output.mha_table_1h

# COMMAND ----------

# DBTITLE 1,Number of people detained under Part II and Part III of the act
 %sql
 refresh table $db_output.mha_mhs08_additional;
 select * from $db_output.mha_mhs08_additional

# COMMAND ----------

 %md
 ### DQ outputs

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# DBTITLE 1,Number of Providers submitting and Detentions by provider type for the year ---Needed for CMS Chart
 %sql
 refresh table $db_output.dq_mha_prov_type_subm_detentions_year;
 select * from $db_output.dq_mha_prov_type_subm_detentions_year

# COMMAND ----------

# DBTITLE 1,DQ Table 1a
 %sql
 refresh table $db_output.dq_mha_prov_type_subm_year;
 select
 his.Org_Type as `Organisation Type`,
 his.T1a_2015_16 as `2015-16`,
 his.T1a_2016_17 as `2016-17`,
 his.T1a_2017_18 as `2017-18`,
 his.T1a_2018_19 as `2018-19`,
 his.T1a_2019_20 as `2019-20`,
 his.T1a_2020_21 as `2020-21`,
 his.T1a_2021_22 as `2021-22`,
 y.providers as `2022-23`
 from $db_output.dq_table_1a_history his
 left join $db_output.dq_mha_prov_type_subm_year y on his.Org_Type = y.Org_Type

# COMMAND ----------

# DBTITLE 1,DQ Table 1b
 %sql
 refresh table $db_output.dq_mha_prov_type_det_year;
 select
 his.Org_Type as `Organisation Type`,
 his.T1b_2015_16 as `2015-16`,
 his.T1b_2016_17 as `2016-17`,
 his.T1b_2017_18 as `2017-18`,
 his.T1b_2018_19 as `2018-19`,
 his.T1b_2019_20 as `2019-20`,
 his.T1b_2020_21 as `2020-21`,
 his.T1b_2021_22 as `2021-22`,
 y.detentions as `2022-23`
 from $db_output.dq_table_1b_history his
 left join $db_output.dq_mha_prov_type_det_year y on his.Org_Type = y.Org_Type

# COMMAND ----------

# DBTITLE 1,DQ Table 2
##Submissions by Organisation Type
spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_prov_submissions_org_type")
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
dq2_final = dq2_final[["Provider Type", "Apr-2022", "May-2022", "Jun-2022", "Jul-2022", "Aug-2022", "Sep-2022", "Oct-2022", "Nov-2022", "Dec-2022", "Jan-2023", "Feb-2023", "Mar-2023"]]
display(dq2_final)

# COMMAND ----------

# DBTITLE 1,Submissions over 12 months ---Needed for CMS Chart
 %sql
 refresh table $db_output.dq_mha_submissions_12m;
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

# DBTITLE 1,DQ Table 3
spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_table_3_prep3")
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

# DBTITLE 1,DQ Table 5
spark.sql(f"REFRESH TABLE {mha_annual.db_output}.dq_mha_time_recording")
dq5 = spark.table(f"{mha_annual.db_output}.dq_mha_time_recording")
dq5_p = dq5.toPandas()
dq5_p["MHA_Count_Perc"] = round((dq5_p["MHA_Count"] / dq5_p["MHA_Count"].sum()), 3)
dq5_p["Hosp_Count_Perc"] = round((dq5_p["Hosp_Count"] / dq5_p["Hosp_Count"].sum()), 3)
dq5_final = dq5_p[["time_recording", "MHA_Count_Perc", "Hosp_Count_Perc"]]
dq5_final.columns = ["Times recorded", "Mental Health Act episode %", "Hospital provider spell %"]
display(dq5_final)

# COMMAND ----------

# DBTITLE 1,DQ Table 6
 %sql
 refresh table $db_output.dq_table_6_year;
 select 
 his.Detention_Type,
 his.T6_2017_18 as `2017-18`,
 his.T6_2018_19 as `2018-19`,
 his.T6_2019_20 as `2019-20`,
 his.T6_2020_21 as `2020-21`,
 his.T6_2021_22 as `2021-22`,
 y.count as `2022-23`
 from $db_output.dq_table_6_history his
 left join $db_output.dq_table_6_year y on his.Detention_Type = y.Detention_Type

# COMMAND ----------

# DBTITLE 1,DQ Table 7
 %sql
 refresh table $db_output.dq_mha_submissions_12m;
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

# DBTITLE 1,DQ Table 8
 %sql
 select 
 ecds.Organisation_Code as `Org Code`, 
 ecds.Organisation_Name as `Organisation Name`,
 case when kp90.Org_Code is null then "No" else "Yes" end as `Submitted to KP90 in 2015-16?`
 from $db_output.ecds ecds
 left join $db_output.dq_kp90_submitters kp90 on ecds.Organisation_Code = kp90.Org_Code
 order by ecds.Organisation_Code

# COMMAND ----------

# DBTITLE 1,DQ Table 9
 %sql
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

# DBTITLE 1,DQ Table 10
 %sql
 ---People detained on 31st March in the year for 24 select Providers (who submitted in 2015-16) --to calculate estimated change
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
 his.T5_2021_22 as `People detained on 31st March 2022`,
 y.mhs08 as `People detained on 31st March 2023`
 from $db_output.dq_table_10_history his
 left join $db_output.mha_dq_table10_year y on his.Org_Code = y.OrgIDProv
 left join $db_output.mha_rd_org_daily_latest od on y.OrgIDProv = od.ORG_CODE
 where OrgIDProv IN ("NR5", "RTQ", "RVN", "RRP", "RXT", "TAD", "TAF", "RJ8", "RXM", "RMV", "RWK", "RV9", "RP7", "RMY", "RX4", "RHA", "TAH", "RRE", "RQY", "RW1", "RX2", "RX3", "RKL", "R1A", "RWV")
 order by case when his.Org_Type = "MH" then "NHS Facilities" else his.Org_Type end, od.NAME

# COMMAND ----------

 %md
 ### PBI Outputs

# COMMAND ----------

# DBTITLE 1,PBI Main Output
 %sql
 refresh table $db_output.mha_final_rates;
 select
 replace("$year", "/", "-") as year,
 demographic_breakdown, organisation_breakdown, primary_level, primary_level_desc, secondary_level, secondary_level_desc, count_of, sub_measure, count, CR
 from $db_output.mha_final_rates 
 where organisation_breakdown != "CCG"
 order by count_of, demographic_breakdown, organisation_breakdown, primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,DQ Table 2
 %sql
 REFRESH TABLE $db_output.mha_pbi_dq_table2;
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

# DBTITLE 1,DQ Table 3
 %sql
 REFRESH TABLE $db_output.mha_pbi_dq_table3;
 SELECT 
 ORGANISATION_CODE as `Organisation Code`, 
 ORGANISATION_NAME as `Organisation Name`, 
 TABLE_NAME as `Table Name`, 
 VALUES_STRING as `Values`, 
 CASE WHEN ORGANISATION_CODE = "England" THEN Records ELSE suppress(Records) end as Records,
 PERIOD_NAME as `Period Name`, 
 PERIOD_END_DATE as `Period End Date`, 
 CONCAT(ORGANISATION_NAME, ' - ', ORGANISATION_CODE) AS `Organisation ID`,
 replace("$year", "/", "-") as `Reporting Year`
 FROM $db_output.mha_pbi_dq_table3 
 WHERE TABLE_NAME != 'Any'
 ORDER BY PERIOD_END_DATE, ORGANISATION_NAME, TABLE_NAME