# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.ECDS_AGG;
 CREATE TABLE IF NOT EXISTS $db_output.ECDS_AGG
 ---table in which aggregate figures are held (to be joined to mha aggregate data later)
 (
 Measure string,
 MeasureSubcategory string,
 OrganisationBreakdown string,
 OrgID string,
 Count int
 )

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds all detentions count
 SELECT 
 'All detentions' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(All_detentions)  AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---provider ecds all detentions count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Detentions_on_admission_to_hospital) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---provider ecds detentions on admission to hospital count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds part 2 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All detentions under Part 2' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(All_detentions_under_part_2) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 2 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 2' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_2) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 3 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 3' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_3) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds part 2 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'All detentions under Part 3' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(All_detentions_under_part_3) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 35 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 35' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_35) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 36 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 36' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_36) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 37 with section 41 restrictions detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 37 with S41 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_37_with_section_41_restrictions) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 37 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 37' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_37) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 45A detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 45A' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_45a) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 47 with section 49 restrictions detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 47 with S49 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_47_with_section_49_restrictions) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 47 detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 47' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_47) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 48 with section 49 restrictions detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Section 48 with S49 restrictions' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_48_with_section_49_restrictions) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds under other sections (section 38, 44 and 47) detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Detentions under other sections (38,44 and 46)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Detentions_under_other_sections_38_44_and_46) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds under other acts detentions on admission to hospital count
 SELECT 
 'Detentions on admission to hospital' as Measure,
 'Detentions under other acts' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Detentions_under_other_acts) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds all short term orders count
 SELECT 
 'Short term orders' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Short_term_orders)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds all place of safety orders count
 SELECT 
 'Short term orders' as Measure,
 'All place of safety orders' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(All_place_of_safety_orders)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 136 short term orders count
 SELECT 
 'Short term orders' as Measure,
 'Section 136' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(section_136)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---provider ecds section 136 short term orders count
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG 
 ---national ecds section 5 short term orders count
 SELECT
 'Short term orders' as Measure,
 'All uses of section 5' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(All_uses_of_section_5)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 5(2) short term orders count
 SELECT 
 'Short term orders' as Measure,
 'Section 5(2)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_5_2)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 5(4) short term orders count
 SELECT 
 'Short term orders' as Measure,
 'Section 5(4)' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_5_4)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 2 count
 SELECT 
 'Uses of section 2' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_2)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 3 count
 SELECT 
 'Uses of section 3' as Measure,
 'All' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_3)
 AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 2 on admission count
 SELECT 
 'Uses of section 2' as Measure,
 'Section 2 on admission' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_2) AS COUNT
 FROM $db_output.ECDS

# COMMAND ----------

 %sql
 INSERT INTO $db_output.ECDS_AGG
 ---national ecds section 3 on admission count
 SELECT 
 'Uses of section 3' as Measure,
 'Section 3 on admission' as MeasureSubcategory,
 'All submissions' as OrganisationBreakdown,
 'All prov' as OrgID,
 SUM(Section_3) AS COUNT
 FROM $db_output.ECDS