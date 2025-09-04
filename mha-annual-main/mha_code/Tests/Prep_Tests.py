# Databricks notebook source
dbutils.widgets.text("db_output", "$personal_db")
dbutils.widgets.text("db_source", "$db_source")
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

# DBTITLE 1,execute tests for mha_los tables
test_duplicate_breakdown_values(mha_los_df, ["Category", "Description", "Higher_Ethnic_Category", "Ethnic_Category_Description", "Age", "Der_Gender", "IMD_Decile", "STP_CODE"], "ID")
test_duplicate_breakdown_values(mha_los_cto_df, ["Category", "Description", "Higher_Ethnic_Category", "Ethnic_Category_Description", "Age", "Der_Gender", "IMD_Decile", "STP_CODE"], "ID")