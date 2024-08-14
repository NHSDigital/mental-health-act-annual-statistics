# Databricks notebook source
dbutils.widgets.text("db_output", "personal_db")
dbutils.widgets.text("db_source", "mhsds_database")
dbutils.widgets.text("rp_enddate", "2022-03-31")
dbutils.widgets.text("rp_startdate", "2021-04-01")
dbutils.widgets.text("year", "2021/22")
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
year = dbutils.widgets.get("year") # year widget is exclusive to MHA Annual Publication

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.dataframe as df

# COMMAND ----------

def test_values_in_year(input_df: df, val_col: str, rp_startdate: str, rp_enddate: str) -> None:
  test_df = (
    input_df
    .filter(
      (~F.col(val_col).between(rp_startdate, rp_enddate))
    )
  )
  
  assert test_df.count() == 0, f"Table contains {val_col} values which occured outside of the financial year"
  print(f"{test_values_in_year.__name__}: PASSED")

# COMMAND ----------

def test_null_values(input_df: df, col_names: list) -> None:
  for col in col_names:
    test_df = (
      input_df
      .filter(F.col(col).isNull()
      )
    )

    assert test_df.count() == 0, f"{col} contains null values"
  print(f"{test_null_values.__name__}: PASSED")

# COMMAND ----------

def test_mha_logic_values(input_df: df) -> None:
  """
  A = Detentions on admission to hospital
  B = Detentions subsequent to admission
  C = Detentions following Place of Safety Order
  D = Detentions following revocation of CTO or Conditional Discharge
  E = Place of Safety Order
  F = Other short term holding order
  G = Renewal
  H = Transfer on Section
  J = 5(2) subsequent to 5(4)
  K = 37 subsequent to 35
  L = 3 subsequent to 2
  M = Guardianship
  P = Criminal Jusitce admissions
  N = Inconsistent value
  """
  exp_mha_logic_values = ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "P", "N"]
  test_df = (
    input_df
    .filter(
      (~F.col("MHA_Logic_Cat_full").isin(exp_mha_logic_values))
    )
  )
  
  assert test_df.count() == 0, "Invalid MHA_Logic_Cat_full values in detentions prep table"
  print(f"{test_mha_logic_values.__name__}: PASSED")

# COMMAND ----------

def test_duplicate_breakdown_values(input_df: df, breakdown_cols: list, value_col: str) -> None:
  for breakdown_col in breakdown_cols:
    test_df = (
      input_df
      .groupBy(F.col(value_col))
      .agg(F.count_distinct(F.col(breakdown_col)))
      .where(F.count_distinct(F.col(breakdown_col)) > 1)
    )

    assert test_df.count() == 0, f"Mulitple distinct {breakdown_col} values for each {value_col}"
  print(f"{test_duplicate_breakdown_values.__name__}: PASSED")

# COMMAND ----------

def test_percent_unknown_geog_values(input_df: df, code_col: str, value_col: str) -> None:
  test_df = (
  input_df
  .groupBy(F.col(code_col))
  .agg(F.count_distinct(F.col(value_col)).alias("VALUE"))
  )
  
  total_value = test_df.select(F.sum("VALUE")).collect()[0][0]
  unknown_value = test_df.filter(
    (F.col(code_col).isNull())
    | (F.col(code_col) == "UNKNOWN")
  ).select(F.sum("VALUE")).collect()[0][0]
    
  perc_unknown = unknown_value / total_value

  assert perc_unknown < 0.25, f"null/UNKNOWN values of {code_col} make up more than 25% of the total count of {value_col}: {perc_unknown}"
  print(f"{test_percent_unknown_geog_values.__name__}: PASSED ({round(perc_unknown, 2)})")

# COMMAND ----------

def get_measure_df(input_df: df, measure: str) -> df:
  test_df = (
      input_df
      .filter(F.col("Measure") == measure)
    )
  
  return test_df

# COMMAND ----------

def get_england_total(input_df: df, measure: str) -> int:
  england_df = (
      input_df
      .filter(
        (F.col("Measure") == measure)
        & (F.col("MeasureSubCategory") == "All")
        & (F.col("Demographic") == "All")
        & (F.col("OrganisationBreakdown") == "All submissions")
      )
    )
  england_total = england_df.collect()[0][9]
  
  return england_total

# COMMAND ----------

def get_demographic_totals(input_df: df, demographic: str, agg_col: str) -> df:
  demog_df = (
    input_df
    .filter(
      (F.col("Demographic") == demographic)
      & (F.col("MeasureSubCategory") == "All")
      & (F.col("OrganisationBreakdown") == "All submissions")     
    )    
  )
  if demographic == "All":
    demog_totals_df = (
      demog_df
      .groupBy(F.col("DemographicBreakdown"))
      .agg(F.sum(F.col(agg_col)).alias("VALUE"))
    )
  else:
    demog_totals_df = (
      demog_df
      .filter(F.col("DemographicBreakdown") != "All")
      .groupBy(F.col("DemographicBreakdown"))
      .agg(F.sum(F.col(agg_col)).alias("VALUE"))
    )
  
  return demog_totals_df

# COMMAND ----------

def get_org_level_totals(input_df: df, org_level: list, agg_col: str) -> df:
  org_df = (
    input_df
    .filter(
      (F.col("OrganisationBreakdown").isin(org_level))#using isin as nhs trust and independent are separate values (need both to sum up to national total)
      & (F.col("Demographic") == "All")
      & (F.col("MeasureSubCategory") == "All")
    )     
  )
  
  if org_df.count() <= 2: #i.e. NHS Trust Total and Independent Provider Total
    org_total_df = (
      org_df
     .groupBy(F.col("OrgID"))
     .agg(F.sum(F.col(agg_col)).alias("VALUE"))
    )
  else:
    org_total_df = (
      org_df
     .filter(F.col("OrgID") != "All prov")#remove summed up total for testing against national level total
     .groupBy(F.col("OrgID"))
     .agg(F.sum(F.col(agg_col)).alias("VALUE"))
    )
  
  return org_total_df

# COMMAND ----------

def get_subm_totals(input_df: df, agg_col: str) -> df:
  subm_df = (
    input_df
    .filter(
      (F.col("OrganisationBreakdown") == "All submissions")
      & (F.col("Demographic") == "All")
      & (F.col("MeasureSubCategory") != "All")
    )
    .groupBy(F.col("MeasureSubCategory"))
    .agg(F.sum(F.col(agg_col)).alias("VALUE"))
  )
  
  return subm_df

# COMMAND ----------

mha_checks_metadata = {
  "All detentions": {
    "demogs": ["Age", "Gender", "IMD"], #Ethnicity excluded
    "orgs": [["CCG"], ["Independent Health Provider", "NHS Trust"], ["STP"]],
    "sub_measures": 0,
    "count_of": "detentions",
    "rates": 1
  }, 
  "CTO recalls to hospital": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following admission to hospital": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following revocation of CTO": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following use of Place of Safety Order": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  },
  "Detentions on admission to hospital": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  },
  "Discharges following detention": {
    "demogs": ["Age", "Gender"], #Ethnicity excluded
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "discharges",
    "rates": 1
  },
  "Discharges from CTO": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "discharges",
    "rates": 1
  },
  "People detained in hospital on 31st March": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "people",
    "rates": 1
  },
  "People subject to Community Treatment Orders (CTOs) on 31st March": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "people",
    "rates": 1
  },
#   "People subject to repeat detentions": {
#     "measure_sub": {
#       "1" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "2" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "3" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "4" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "5" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "6" : ["All submissions", "Age", "Ethnicity", "Gender"],
#       "7 and over" : ["All submissions", "Age", "Ethnicity", "Gender"],
#     },   
#     "count_of": "people",
#     "rates": 1
#   },
  "People subject to the Act on 31st March": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "people",
    "rates": 1
  },
  "Revocations of CTO": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "detentions",
    "rates": 1
  },
  "Short term orders": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  },
  "Transfers on section": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 0,
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of CTOs": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of section 2": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of section 3": {
    "demogs": ["All"],
    "orgs": [["Independent Health Provider", "NHS Trust"]],
    "sub_measures": 1,
    "count_of": "detentions",
    "rates": 1
  }
}

# COMMAND ----------

def test_demog_breakdown_totals(input_df: df, england_total: int, demog: str, count_of: str, agg_col: str) -> None:
  demog_total_df = get_demographic_totals(input_df, demog, agg_col)
  demog_total = demog_total_df.select(F.sum(F.col("VALUE"))).collect()[0][0]
  if count_of == "people":
    assert int(demog_total) >= int(england_total), f"{demog} total is less than England total (dt: {demog_total}, et: {england_total})"
  else:
    assert int(demog_total) == int(england_total), f"{demog} total is not equal to England total (dt: {demog_total}, et: {england_total})"

# COMMAND ----------

def test_org_breakdown_totals(input_df: df, england_total: int, org: list, count_of: str, agg_col: str) -> None:
  org_total_df = get_org_level_totals(input_df, org, agg_col)
  org_total = org_total_df.select(F.sum(F.col("VALUE"))).collect()[0][0]
  if count_of == "people":
    assert int(org_total) >= int(england_total), f"{org} total is less than England total (ot: {org_total}, et: {england_total})"
  else:
    assert int(org_total) == int(england_total), f"{org} total is not equal to England total (ot: {org_total}, et: {england_total})"

# COMMAND ----------

def test_subm_breakdown_totals(input_df: df, england_total: int, subm: int, agg_col: str) -> None:
  if subm == 1:
    subm_df = get_subm_totals(input_df, agg_col)
    subm_total = subm_df.select(F.sum(F.col("VALUE"))).collect()[0][0]
  else:
    return  
  
  assert int(subm_total) >= int(england_total), f"sub measure total is less than England total (st: {subm_total}, et: {england_total})"

# COMMAND ----------

def test_activity_breakdown_totals(input_df: df, metadata: dict, agg_col: str) -> None:
  for measure in metadata:
    print(f"Testing {measure}")
    test_df = get_measure_df(input_df, measure)
    england_total = get_england_total(input_df, measure)
    count_of = metadata[measure]["count_of"] 
    demogs = metadata[measure]["demogs"]    
    for demog in demogs:      
      test_demog_breakdown_totals(test_df, england_total, demog, count_of, agg_col)
    orgs = metadata[measure]["orgs"]
    for org in orgs:
      test_org_breakdown_totals(test_df, england_total, org, count_of, agg_col)
    sub_measure_check = metadata[measure]["sub_measures"]
    test_subm_breakdown_totals(test_df, england_total, sub_measure_check, agg_col)
    print(f"{measure}:  PASSED")
  print(f"{test_activity_breakdown_totals.__name__}:  PASSED")

# COMMAND ----------

def test_s136_breakdown_totals(input_df: df) -> None:
  s136_df = (
    test_df
    .filter(
      (F.col("Measure") == "Short term orders")
      & (F.col("MeasureSubCategory") == "Section 136")
    )
  )

# COMMAND ----------

# DBTITLE 1,final detentions prep table as pyspark dataframe
det_df = spark.table(f"{db_output}.detentions")

# COMMAND ----------

# DBTITLE 1,execute tests for detentions prep table
test_values_in_year(det_df, "StartDateMHActLegalStatusClass", rp_startdate, rp_enddate)
test_null_values(det_df, ["UniqMHActEpisodeID", "UniqHospProvSpellID", "Person_ID"])
test_mha_logic_values(det_df)
test_duplicate_breakdown_values(det_df, ["EthnicCategory", "AgeRepPeriodEnd", "Der_Gender", "IMD_Decile", "STP_CODE"], "UniqMHActEpisodeID")
test_percent_unknown_geog_values(det_df, "IC_REC_CCG", "UniqMHActEpisodeID")
test_percent_unknown_geog_values(det_df, "STP_CODE", "UniqMHActEpisodeID")

# COMMAND ----------

# DBTITLE 1,final short term orders prep table as pyspark dataframe
sto_df = spark.table(f"{db_output}.short_term_orders")

# COMMAND ----------

# DBTITLE 1,execute tests for short term orders prep table
test_values_in_year(sto_df, "StartDateMHActLegalStatusClass", rp_startdate, rp_enddate)
test_null_values(sto_df, ["UniqMHActEpisodeID", "Person_ID"]) #UniqHospProvSpellID
test_mha_logic_values(sto_df)
test_percent_unknown_geog_values(sto_df, "IC_REC_CCG", "UniqMHActEpisodeID")
test_percent_unknown_geog_values(sto_df, "STP_CODE", "UniqMHActEpisodeID")

# COMMAND ----------

# DBTITLE 1,final community treatment orders prep table as pyspark dataframe
cto_df = spark.table(f"{db_output}.cto")

# COMMAND ----------

# DBTITLE 1,execute tests for community treatment orders prep table
test_values_in_year(cto_df, "StartDateCommTreatOrd", rp_startdate, rp_enddate)
test_null_values(cto_df, ["CTO_UniqMHActEpisodeID", "Person_ID"]) #"MHA_UniqMHActEpisodeID"

# COMMAND ----------

# DBTITLE 1,mha_los tables
mha_los_df = spark.table(f"{db_output}.mha_los_quartiles")
mha_los_cto_df = spark.table(f"{db_output}.mha_los_quartiles_cto")

# COMMAND ----------

test_duplicate_breakdown_values(mha_los_df, ["Category", "Description", "Higher_Ethnic_Category", "Ethnic_Category_Description", "Age", "Der_Gender", "IMD_Decile", "STP_CODE"], "ID")
test_duplicate_breakdown_values(mha_los_cto_df, ["Category", "Description", "Higher_Ethnic_Category", "Ethnic_Category_Description", "Age", "Der_Gender", "IMD_Decile", "STP_CODE"], "ID")

# COMMAND ----------

# DBTITLE 1,get final inserted data output as pyspark dataframe
insert_output_df = spark.table(f"{db_output}.mha_unformatted")

# COMMAND ----------

# DBTITLE 1,execute tests for mha_unformatted
test_activity_breakdown_totals(insert_output_df, mha_checks_metadata, "METRIC_VALUE")

# COMMAND ----------

# DBTITLE 1,get final unsuppressed output as pyspark dataframe
unsup_output_df = spark.table(f"{db_output}.mha_raw")

# COMMAND ----------

# DBTITLE 1,execute tests for mha_raw
test_activity_breakdown_totals(unsup_output_df, mha_checks_metadata, "MHSDS_COUNT")

# COMMAND ----------

# DBTITLE 1,get final suppressed output as pyspark dataframe
sup_output_df = spark.table(f"{db_output}.mha_suppressed")

# COMMAND ----------

# DBTITLE 1,execute tests for mha_suppressed
test_null_values(sup_output_df, sup_output_df.columns)