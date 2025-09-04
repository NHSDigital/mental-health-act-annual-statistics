# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
year = dbutils.widgets.get("year")
rp_enddate = dbutils.widgets.get("rp_enddate")

# COMMAND ----------

spark.conf.set("spark.databricks.queryWatchdog.maxQueryTasks", 200000)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_ref_master;
 CREATE TABLE IF NOT EXISTS $db_output.mha_ref_master
 (
 Breakdown string,
 Demographic_Breakdown string,
 Demog_Code string,
 Demog_Name string,
 Organisation_Breakdown string,
 Org_Code string,
 Org_Name string
 )

# COMMAND ----------

# DBTITLE 1,Add all Demographic Values
 %sql
 truncate table $db_output.mha_ref_master;
 insert into $db_output.mha_ref_master values
 ---National Values---
 ('All submissions', 'All', 'All', 'All', 'All submissions', 'All prov', 'All providers'),
 ---Age Values---
 ('Age', 'Age', '15 and under', '15 and under', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', '16 to 17', '16 to 17', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', '18 to 34', '18 to 34', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', '35 to 49', '35 to 49', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', '50 to 64', '50 to 64', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', '65 and over', '65 and over', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', 'Unknown', 'Unknown', 'All submissions', 'All prov', 'All providers'),
 ('Age', 'Age', 'All', 'All', 'All submissions', 'All prov', 'All providers'),

 ---Ethnicity Values---
 -- Higher Level -- 
 ('Ethnicity', 'Ethnicity', 'White', 'White', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Mixed', 'Mixed', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Asian or Asian British', 'Asian or Asian British', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Black or Black British', 'Black or Black British', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Other Ethnic Groups', 'Other Ethnic Groups', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Not Stated', 'Not Stated', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Not Known', 'Not Known', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Unknown', 'Unknown', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'All', 'All', 'All submissions', 'All prov', 'All providers'),

 -- Lower Level --
 ('Ethnicity', 'Ethnicity', 'British', 'British', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Irish', 'Irish', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Any Other White background', 'Any Other White background', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'White and Black Caribbean', 'White and Black Caribbean', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'White and Black African', 'White and Black African', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'White and Asian', 'White and Asian', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Any Other Mixed background', 'Any Other Mixed background', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Indian', 'Indian', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Pakistani', 'Pakistani', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Bangladeshi', 'Bangladeshi', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Any Other Asian background', 'Any Other Asian background', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Caribbean', 'Caribbean', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'African', 'African', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Any Other Black background', 'Any Other Black background', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Chinese', 'Chinese', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Any Other Ethnic Group', 'Any Other Ethnic Group', 'All submissions', 'All prov', 'All providers'),
 ('Ethnicity', 'Ethnicity', 'Not Stated', 'Not Stated', 'All submissions', 'All prov', 'All providers'),
 -- ('Ethnicity', 'Not Known'),
 -- ('Ethnicity', 'Unknown'),

 -- Gender Values --
 ('Gender', 'Gender', '1', 'Male', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', '2', 'Female', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', '3', 'Non-binary', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', '4', 'Other (not listed)', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', '9', 'Indeterminate', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', 'Unknown', 'Unknown', 'All submissions', 'All prov', 'All providers'),
 ('Gender', 'Gender', 'All', 'All', 'All submissions', 'All prov', 'All providers'),

 -- IMD Values --
 ('IMD', 'IMD', '01 Most deprived', '01 Most deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '02 More deprived', '02 More deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '03 More deprived', '03 More deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '04 More deprived', '04 More deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '05 More deprived', '05 More deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '06 Less deprived', '06 Less deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '07 Less deprived', '07 Less deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '08 Less deprived', '08 Less deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '09 Less deprived', '09 Less deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', '10 Least deprived', '10 Least deprived', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', 'UNKNOWN', 'UNKNOWN', 'All submissions', 'All prov', 'All providers'),
 ('IMD', 'IMD', 'All', 'All', 'All submissions', 'All prov', 'All providers'),

 -- CCG non-org values --
 ('CCG', 'All', 'All', 'All', 'CCG', 'UNKNOWN', 'UNKNOWN'),
 ('CCG', 'All', 'All', 'All', 'CCG', 'All prov', 'All providers'),

 -- STP non-org values --
 ('STP', 'All', 'All', 'All', 'STP', 'UNKNOWN', 'UNKNOWN'),
 ('STP', 'All', 'All', 'All', 'STP', 'All prov', 'All providers'),

 -- NHS Trust non-org values --
 ('NHS Trust Total', 'All', 'All', 'All', 'NHS Trust', 'All prov', 'All providers'),
 ('NHS Trust Both', 'All', 'All', 'All', 'NHS Trust', 'All prov', 'All providers'),
 ('NHS Trust MHSDS', 'All', 'All', 'All', 'NHS Trust', 'All prov', 'All providers'),

 -- Independent Health Provider non-org values --
 ('Independent Health Provider Total', 'All', 'All', 'All', 'Independent Health Provider', 'All prov', 'All providers'),
 ('Independent Health Provider Both', 'All', 'All', 'All', 'Independent Health Provider', 'All prov', 'All providers'),
 ('Independent Health Provider MHSDS', 'All', 'All', 'All', 'Independent Health Provider', 'All prov', 'All providers')

# COMMAND ----------

 %sql
 insert into $db_output.mha_ref_master
 select 'CCG' as Breakdown,
 'All' as Demographic_Breakdown,
 'All' as Demog_Code,
 'All' as Demog_Name,
 'CCG' as Organisation_Breakdown,
 ORG_CODE as Org_Code,
 NAME as Org_Name
 from global_temp.RD_CCG_LATEST

# COMMAND ----------

 %sql
 insert into $db_output.mha_ref_master
 select 
 distinct
 'STP' as Breakdown,
 'All' as Demographic_Breakdown,
 'All' as Demog_Code,
 'All' as Demog_Name,
 'STP' as Organisation_Breakdown,
 STP_CODE as Org_Code,
 STP_NAME as Org_Name
 from $db_output.mha_stp_mapping

# COMMAND ----------

 %sql
 create or replace temp view mha_orgidprov_all_submissions as
 select distinct OrgIDProv as OrgIDProv from $db_output.detentions
 union --to keep 1 line for providers in mhsds and ecds
 select distinct Organisation_Code as OrgIDProv from $db_output.ECDS
 union ---monthly measures - not sure how these aren't in detentions?
 select distinct OrgIDProv from $db_output.mha_mhs09prov_prep
 union 
 select distinct OrgIDProv from $db_output.mha_mhs08prov_prep
 union 
 select distinct OrgIDProv from $db_output.mha_mhs10prov_prep

# COMMAND ----------

 %sql
 create or replace temp view mha_orgidprov_mhsds_submissions as
 select distinct OrgIDProv as OrgIDProv from $db_output.detentions
 union 
 select distinct OrgIDProv as OrgIDProv from $db_output.short_term_orders
 union 
 select distinct OrgIDProv as OrgIDProv from $db_output.cto
 union ---monthly measures - not sure how these aren't in detentions?
 select distinct OrgIDProv from $db_output.mha_mhs09prov_prep
 union 
 select distinct OrgIDProv from $db_output.mha_mhs08prov_prep
 union 
 select distinct OrgIDProv from $db_output.mha_mhs10prov_prep

# COMMAND ----------

 %sql
 refresh table $db_output.mha_rd_org_daily_latest;
 insert into $db_output.mha_ref_master
 select 
 case when od.ORG_TYPE_CODE = 'Independent Health Provider' then 'Independent Health Provider Both'
 else 'NHS Trust Both' end as Breakdown,
 'All' as Demographic_Breakdown,
 'All' as Demog_Code,
 'All' as Demog_Name,
 case when od.ORG_TYPE_CODE = 'Independent Health Provider' then 'Independent Health Provider Both'
 else 'NHS Trust Both' end as Organisation_Breakdown,
 s.OrgIDProv as Org_Code,
 od.NAME as Org_Name
 from mha_orgidprov_all_submissions s
 left join $db_output.mha_rd_org_daily_latest od on s.OrgIDProv = od.ORG_CODE
 where od.NAME is not null

# COMMAND ----------

 %sql
 refresh table $db_output.mha_rd_org_daily_latest;
 insert into $db_output.mha_ref_master
 select 
 case when od.ORG_TYPE_CODE = 'Independent Health Provider' then 'Independent Health Provider MHSDS'
 else 'NHS Trust MHSDS' end as Breakdown,
 'All' as Demographic_Breakdown,
 'All' as Demog_Code,
 'All' as Demog_Name,
 case when od.ORG_TYPE_CODE = 'Independent Health Provider' then 'Independent Health Provider MHSDS'
 else 'NHS Trust MHSDS' end as Organisation_Breakdown,
 s.OrgIDProv as Org_Code,
 od.NAME as Org_Name
 from mha_orgidprov_mhsds_submissions s
 left join $db_output.mha_rd_org_daily_latest od on s.OrgIDProv = od.ORG_CODE
 where od.NAME is not null

# COMMAND ----------

 %sql
 insert into $db_output.mha_ref_master
 select 'STP; Age' as breakdown,
 'Age' as Demographic_Breakdown,
 Demog_Code,
 Demog_Name,
 'STP' as Organisation_Breakdown,
 Org_Code,
 Org_Name
 from (select distinct Demog_Code, Demog_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "Age" and Demog_Code != "All") a
 cross join (select distinct Org_Code, Org_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "STP" and Org_Code != "All prov") b on a.tag = b.tag
 union all
 select 'STP; Ethnicity' as breakdown,
 'Ethnicity' as Demographic_Breakdown,
 Demog_Code,
 Demog_Name,
 'STP' as Organisation_Breakdown,
 Org_Code,
 Org_Name
 from (select distinct Demog_Code, Demog_Name, 'a' as tag from $db_output.mha_ref_master where Demog_Code in ("White", "Mixed", "Asian or Asian British", "Black or Black British", "Other Ethnic Groups", "Unknown")) a
 cross join (select distinct Org_Code, Org_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "STP" and Org_Code != "All prov") b on a.tag = b.tag
 union all
 select 'STP; Gender' as breakdown,
 'Gender' as Demographic_Breakdown,
 Demog_Code,
 Demog_Name,
 'STP' as Organisation_Breakdown,
 Org_Code,
 Org_Name
 from (select distinct Demog_Code, Demog_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "Gender" and Demog_Code != "All") a
 cross join (select distinct Org_Code, Org_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "STP" and Org_Code != "All prov") b on a.tag = b.tag
 union all
 select 'STP; IMD' as breakdown,
 'IMD' as Demographic_Breakdown,
 Demog_Code,
 Demog_Name,
 'STP' as Organisation_Breakdown,
 Org_Code,
 Org_Name
 from (select distinct Demog_Code, Demog_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "IMD" and Demog_Code != "All") a
 cross join (select distinct Org_Code, Org_Name, 'a' as tag from $db_output.mha_ref_master where Breakdown = "STP" and Org_Code != "All prov") b on a.tag = b.tag

# COMMAND ----------

mha_measures_metadata = {
  "All detentions": {
    "measure_sub": {
      "All" : ["All submissions", "Age", "Ethnicity", "Gender", "IMD", "CCG", "Independent Health Provider Both", "NHS Trust Both", "STP", "STP; Age", "STP; Ethnicity", "STP; Gender", "STP; IMD"]
    },   
    "count_of": "detentions",
    "rates": 1
  }, 
  "CTO recalls to hospital": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"]
      },
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following admission to hospital": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Both", "NHS Trust Both", "STP", "STP; Ethnicity"],      
      "Informal to section 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Informal to section 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 4 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 4 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(2) to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(2) to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(4) to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(4) to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 2": ["All submissions", "STP", "STP; Ethnicity"],
      "Section 3": ["All submissions", "STP", "STP; Ethnicity"]
    },   
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following revocation of CTO": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"]
    },   
    "count_of": "detentions",
    "rates": 1
  }, 
  "Detentions following use of Place of Safety Order": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "Section 135 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 135 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 136 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 136 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 135/136 to 2": ["All submissions", "STP", "STP; Ethnicity"],
      "Section 135/136 to 3": ["All submissions", "STP", "STP; Ethnicity"]
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Detentions on admission to hospital": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Both", "NHS Trust Both", "STP", "STP; Ethnicity"],
      "All detentions under Part 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "All detentions under Part 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "Detentions under other acts" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Detentions under other sections (38, 44 and 46)" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "Section 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "Section 35" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 36" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 37" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 37 with S41 restrictions" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 45A" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 47" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 47 with S49 restrictions" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 48" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 48 with S49 restrictions" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Discharges following detention": {
    "measure_sub": {
      "All" : ["All submissions", "Age", "Ethnicity", "Gender", "Independent Health Provider Total", "NHS Trust Total"]
    },   
    "count_of": "discharges",
    "rates": 1
  },
  "Discharges from CTO": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
    },   
    "count_of": "discharges",
    "rates": 1
  },
  "People detained in hospital on 31st March": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider MHSDS", "NHS Trust MHSDS"]
    },   
    "count_of": "people",
    "rates": 1
  },
  "People subject to Community Treatment Orders (CTOs) on 31st March": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider MHSDS", "NHS Trust MHSDS"]
    },   
    "count_of": "people",
    "rates": 1
  },
  "People subject to repeat detentions": {
    "measure_sub": {
      "1" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "2" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "3" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "4" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "5" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "6" : ["All submissions", "Age", "Ethnicity", "Gender"],
      "7 and over" : ["All submissions", "Age", "Ethnicity", "Gender"],
    },   
    "count_of": "people",
    "rates": 1
  },
  "People subject to the Act on 31st March": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider MHSDS", "NHS Trust MHSDS"]
    },   
    "count_of": "people",
    "rates": 1
  },
  "Revocations of CTO": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"]
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Short term orders": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total", "STP", "STP; Ethnicity"],
      "All place of safety orders" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "All uses of section 5" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 135" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 136" : ["All submissions", "Age", "Ethnicity", "Gender", "Independent Health Provider Both", "NHS Trust Both"],
      "Section 4" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(2)" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(4)" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Transfers on section": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Both", "NHS Trust Both"],
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of CTOs": {
    "measure_sub": {
      "All" : ["All submissions", "Age", "Ethnicity", "Gender", "Independent Health Provider MHSDS", "NHS Trust MHSDS"],
      "Informal to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Other sections to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 3 to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 37 to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 47 to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 48 to CTO" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of section 2": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 135 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 136 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 2 following admission (informal to s2)" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 2 on admission" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 4 to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(2) to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(4) to 2" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
    },   
    "count_of": "detentions",
    "rates": 1
  },
  "Uses of section 3": {
    "measure_sub": {
      "All" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Changes from other section to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 135 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 136 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 2 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 3 following admission (informal to s3)" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 3 on admission" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 4 to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(2) to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"],
      "Section 5(4) to 3" : ["All submissions", "Independent Health Provider Total", "NHS Trust Total"]
    },   
    "count_of": "detentions",
    "rates": 1
  }
}

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_csv_master_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_csv_master_prep
 (
 Year string,
 Measure string,
 MeasureSubCategory string,
 Demographic string,
 DemographicBreakdown string,
 OrganisationBreakdown string,
 OrgID string,
 OrgName string
 )

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.dataframe as df

# COMMAND ----------

def insert_into_csv_master(ref_df, year, measure, sub_measure, breakdown):
  csv_df = (
    ref_df
    .filter(F.col("Breakdown") == breakdown)
    .withColumn("Year", F.lit(year))
    .withColumn("Measure", F.lit(measure))                    
    .withColumn("MeasureSubCategory", F.lit(sub_measure))
    .withColumn("Demographic", F.col("Demographic_Breakdown"))
    .withColumn("DemographicBreakdown", F.col("Demog_Code"))
    .withColumn("OrganisationBreakdown", F.col("Organisation_Breakdown"))
    .withColumn("OrgID", F.col("Org_Code"))
    .withColumn("OrgName", F.col("Org_Name"))
    .select("Year", "Measure", "MeasureSubCategory", "Demographic", "DemographicBreakdown", "OrganisationBreakdown", "OrgID", "OrgName")
  )
  
  csv_df.write.insertInto(f"{db_output}.mha_csv_master_prep")

# COMMAND ----------

ref_df = spark.table(f"{db_output}.mha_ref_master")
for measure in mha_measures_metadata:
  sub_measures = mha_measures_metadata[measure]["measure_sub"]
  for sub_measure in sub_measures:
    sub_measure_breakdowns = mha_measures_metadata[measure]["measure_sub"][sub_measure]
    for breakdown in sub_measure_breakdowns:
      insert_into_csv_master(ref_df, year, measure, sub_measure, breakdown)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_csv_master;
 CREATE TABLE IF NOT EXISTS $db_output.mha_csv_master as
 select
 Year,
 Measure,
 MeasureSubCategory,
 Demographic,
 DemographicBreakdown,
 case when OrganisationBreakdown in ("NHS Trust Total", "NHS Trust Both", "NHS Trust MHSDS") then "NHS Trust" 
      when OrganisationBreakdown in ("Independent Health Provider Total", "Independent Health Provider Both", "Independent Health Provider MHSDS") then "Independent Health Provider" 
      else OrganisationBreakdown end as OrganisationBreakdown, ---consoling all levels of Provider level breakdowns into 1
 OrgID,
 OrgName
 from $db_output.mha_csv_master_prep

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_raw;
 CREATE TABLE IF NOT EXISTS $db_output.mha_raw AS

 SELECT 
 DISTINCT
 Z.Year,
 Z.Measure,
 Z.MeasureSubcategory,
 Z.Demographic,
 Z.DemographicBreakdown,
 Z.OrganisationBreakdown,
 CASE
   WHEN COALESCE(A.METRIC_VALUE, R.count) IS NOT NULL AND F.COUNT IS NOT NULL THEN 'Both'
   WHEN COALESCE(A.METRIC_VALUE, R.count) IS NOT NULL THEN 'MHSDS'
   WHEN F.COUNT IS NOT NULL THEN 'ECDS'
   WHEN z.OrgID = 'All prov' THEN 'Both'
   WHEN g.OrgID is not NULL THEN 'ECDS'
   WHEN h.OrgID is not NULL THEN 'MHSDS' 
   ELSE 'ERROR'
   END AS Datasource,
 Z.OrgID,
 Z.OrgName,
 COALESCE(A.METRIC_VALUE, R.count, C.count, CD.count) AS MHSDS_COUNT, ---Adding in "All" for demographic breakdowns
 F.COUNT AS ACUTE_COUNT,
 CASE
   WHEN COALESCE(A.METRIC_VALUE, R.count, C.count, CD.count) IS NOT NULL AND F.COUNT IS NOT NULL THEN COALESCE(A.METRIC_VALUE, R.count, C.count, CD.count) + F.COUNT 
   WHEN COALESCE(A.METRIC_VALUE, R.count, C.count, CD.count) IS NOT NULL THEN COALESCE(A.METRIC_VALUE, R.count, C.count)
   WHEN F.COUNT IS NOT NULL THEN F.COUNT
   WHEN COALESCE(A.METRIC_VALUE, R.count, C.count, CD.count) IS NULL AND F.COUNT IS NULL THEN 0
   ELSE 'ERROR'
   END AS Total_Count, ---COALESCE instead?
 COALESCE(R.CR, C.CR, CD.CR) as CR,
 COALESCE(R.SR, C.SR, CD.SR) as SR,
 COALESCE(R.CI, C.CI, CD.CI) as CI
 FROM
 $db_output.mha_csv_master z
 LEFT JOIN $db_output.mha_unformatted a on a.Measure = z.Measure ---Bring in MHSDS Counts
   and a.MeasureSubcategory = z.MeasureSubcategory 
   and a.OrganisationBreakdown = z.OrganisationBreakdown 
   and a.OrgID = z.OrgID 
   and a.Demographic = z.Demographic
   and a.DemographicBreakdown = z.DemographicBreakdown
 LEFT JOIN $db_output.ECDS_AGG F on z.Measure = f.Measure ---Bring in ECDS Counts
   and z.MeasureSubcategory = f.MeasureSubcategory 
   and z.OrganisationBreakdown = f.OrganisationBreakdown 
   and z.OrgID = f.OrgID and z.DemographicBreakdown = "All" and z.Demographic = "All"
 LEFT JOIN $db_output.mha_final_rates R on z.Measure = R.count_of ---Bring in National level rates
   and z.MeasureSubcategory = R.sub_measure
   and z.Demographic = R.demographic_breakdown 
   and z.DemographicBreakdown = R.primary_level
   and z.measureSubcategory in ("All", "Section 136")
   and R.organisation_breakdown = "All"
   and z.OrganisationBreakdown = "All submissions"
 LEFT JOIN $db_output.mha_final_rates C on z.Measure = C.count_of ---Bring in CCG/STP top-level rates
   and z.MeasureSubcategory = C.sub_measure
   and z.OrganisationBreakdown = C.organisation_breakdown
   and z.Demographic = C.demographic_breakdown  
   and z.OrgID = C.primary_level 
   and C.demographic_breakdown in ("All")
   and C.organisation_breakdown in ("CCG", "STP")
 LEFT JOIN $db_output.mha_final_rates CD on z.Measure = CD.count_of ---Bring in sub-national level rates
   and z.MeasureSubcategory = CD.sub_measure
   and z.OrganisationBreakdown = CD.organisation_breakdown
   and z.Demographic = CD.demographic_breakdown 
   and z.OrgID = CD.primary_level
   and z.DemographicBreakdown = CD.secondary_level
 LEFT JOIN (SELECT DISTINCT OrgID FROM $db_output.ECDS_AGG WHERE OrgID <> 'All prov') G ON G.OrgID = z.OrgID ---Get ECDS OrgID names
 LEFT JOIN (SELECT DISTINCT OrgID FROM $db_output.MHA_unformatted WHERE OrgID <> 'All prov') H ON H.OrgID = z.OrgID ---Get MHSDS OrgID Names

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.mha_raw;
 DROP TABLE IF EXISTS $db_output.mha_suppressed;
 CREATE TABLE IF NOT EXISTS $db_output.mha_suppressed AS
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
 COALESCE(CASE 
           WHEN OrgID = 'All prov' THEN MHSDS_Count
           WHEN MHSDS_COUNT < 5 THEN '*' 
           ELSE ROUND(MHSDS_Count/5.0,0)*5
           END,
           CASE 
             WHEN OrgID = 'All prov' THEN 0
             ELSE '*'
             END) AS MHSDS_Count,
 COALESCE(CASE 
           WHEN OrgID = 'All prov' THEN COALESCE(Acute_Count,0)
           WHEN Acute_Count = 0 THEN 0
           WHEN Acute_Count < 7 THEN '*' 
           ELSE ROUND(Acute_Count/5.0,0)*5
           END,
           CASE 
             WHEN OrgID = 'All prov' THEN 0
             ELSE '*'
             END) AS Acute_Count,
 COALESCE(CASE 
           WHEN OrgID = 'All prov' THEN Total_Count
           WHEN Total_Count = 0 AND Acute_Count = 0 THEN 0
           WHEN Total_Count < 7 AND Acute_Count < 7  THEN '*' 
           WHEN Total_Count < 5 AND MHSDS_Count < 5 THEN '*'
           WHEN Total_Count = 0 THEN '*'
           ELSE CAST(ROUND(Total_Count/5.0,0)*5 as int)
           END,
           CASE 
             WHEN OrgID = 'All prov' THEN 0
             ELSE '*'
             END) AS Total_Count,
 CASE WHEN CR = 0 or CR is null THEN "NULL" ELSE CR END as CrudeRate,
 CASE WHEN SR = 0 or SR is null THEN "NULL" ELSE SR END as StdRate,
 CASE WHEN CI = 0 or CI is null THEN "NULL" ELSE CI END as Confidence_interval            
 FROM
 $db_output.mha_raw 
 WHERE
 DATASOURCE <> 'ERROR'
 order by Measure, measureSubcategory, Demographic, DemographicBreakdown, OrganisationBreakdown, OrgID

# COMMAND ----------

if rp_enddate >= "2023-03-31":  # the last month of CCGs is June 2022
  #unsuppressed output
  sqlContext.sql(f"UPDATE {db_output}.mha_suppressed SET OrganisationBreakdown = REPLACE(OrganisationBreakdown,'CCG','Sub ICB') WHERE OrganisationBreakdown LIKE '%CCG%' AND Year = '{year}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.mha_suppressed SET OrganisationBreakdown = REPLACE(OrganisationBreakdown,'STP','ICB') WHERE OrganisationBreakdown LIKE '%STP%' AND Year = '{year}'") #update STP to ICB
  sqlContext.sql(f"UPDATE {db_output}.mha_los_output SET Geography = REPLACE(Geography,'STP','ICB') WHERE Geography = 'STP'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.mha_los_cto_output SET Geography = REPLACE(Geography,'STP','ICB') WHERE Geography = 'STP'") #update STP to ICB
  sqlContext.sql(f"UPDATE {db_output}.mha_final_rates SET organisation_breakdown = REPLACE(organisation_breakdown,'CCG','Sub ICB') WHERE organisation_breakdown LIKE '%CCG%'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.mha_final_rates SET organisation_breakdown = REPLACE(organisation_breakdown,'STP','ICB') WHERE organisation_breakdown LIKE '%STP%'") #update STP to ICB