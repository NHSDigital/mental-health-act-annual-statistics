-- Databricks notebook source
-- # %py
-- # db_source  = dbutils.widgets.get("db_source")
-- # rp_enddate = dbutils.widgets.get("rp_enddate")
-- # rp_startdate = dbutils.widgets.get("rp_startdate")
-- # year = dbutils.widgets.get("year")
-- # personal_db = dbutils.widgets.get("personal_db")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $db_output.ECDS_AGG
(Measure string,
MeasureSubcategory string,
OrganisationBreakdown string,
OrgID string,
Count int)

-- COMMAND ----------

TRUNCATE TABLE $db_output.ECDS_AGG

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG

 SELECT 
 'All detentions' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(All_detentions)  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'All detentions' as Measure,
 'All' as MeasureSubcategory,
Org_Type as OrganisationBreakdown,
 Organisation_Code as OrgID,
  SUM(All_detentions)  AS COUNT
 FROM $db_output.ECDS
 GROUP BY 
 Org_Type,
 Organisation_Code

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Detentions_on_admission_to_hospital) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All' as MeasureSubcategory,
 Org_Type as OrganisationBreakdown,
Organisation_Code as OrgID,
  SUM(Detentions_on_admission_to_hospital) AS COUNT
 FROM $db_output.ECDS
 GROUP BY 
 Org_Type,
 Organisation_Code

-- COMMAND ----------

 INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All detentions under Part 2' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(All_detentions_under_part_2) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

 INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 2' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_2) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 3' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_3) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All detentions under Part 3' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(All_detentions_under_part_3) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 35' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_35) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 36' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_36) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 37 with S41 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_37_with_section_41_restrictions) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 37' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_37) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 45A' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_45a) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 47 with S49 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_47_with_section_49_restrictions) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 47' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_47) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 48 with S49 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_48_with_section_49_restrictions) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Detentions under other sections (38,44 and 46)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Detentions_under_other_sections_38_44_and_46) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Detentions under other acts' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Detentions_under_other_acts) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Short_term_orders)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'All place of safety orders' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(All_place_of_safety_orders)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'Section 136' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(section_136)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'Section 136' as MeasureSubcategory,
 Org_type as OrganisationBreakdown,
 Organisation_Code as OrgID,
  SUM(section_136)
  AS COUNT
 FROM $db_output.ECDS
 GROUP BY
 Org_type,
 Organisation_Code

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG 
SELECT
 'Short term orders' as Measure,
 'All uses of section 5' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(All_uses_of_section_5)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'Section 5(2)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
SUM(Section_5_2)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Short term orders' as Measure,
 'Section 5(4)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_5_4)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Uses of section 2' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_2)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Uses of section 3' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_3)
  AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Uses of section 2' as Measure,
 'Section 2 on admission ' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_2) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
 'Uses of section 3' as Measure,
 'Section 3 on admission' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
  SUM(Section_3) AS COUNT
 FROM $db_output.ECDS

-- COMMAND ----------

INSERT INTO $db_output.ECDS_AGG
 SELECT 
Measure,
MeasureSubcategory,
"NHS TRUST" as OrganisationBreakdown,
OrgID,
COUNT
FROM $db_output.ECDS_AGG
WHERE
ORGID = 'All prov'