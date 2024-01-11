# Databricks notebook source
 %md
 ###Reference data tables

# COMMAND ----------

# DBTITLE 1,Ref data - THIS NOT EQUIVALENT to get_rd_org_daily_latest()
 %sql
 
 DROP TABLE IF EXISTS $db_output.mha_rd_org_daily_latest; 
 CREATE TABLE $db_output.mha_rd_org_daily_latest AS
 ---MHA Logic for getting all valid providers in the financial year
 SELECT DISTINCT				
 ORG_CODE				
 ,NAME				
 ,CASE 				
 WHEN ORG_TYPE_CODE IN ('CT','TR') THEN 'NHS Trust'
 WHEN ORG_TYPE_CODE IN ('PH','LA','NN') THEN 'Independent Health Provider'
 END AS ORG_TYPE_CODE				
 				
 FROM		reference_database.ORG_DAILY		
 				
 WHERE			(BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)	
 			AND BUSINESS_START_DATE <= '$rp_enddate' AND ORG_TYPE_CODE in ('CC','CF','LB','PT','CT','OU','NS','TR','HA','LA','PH','NN')	
 			AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)	
 			AND ORG_OPEN_DATE <= '$rp_enddate'
             
 UNION
 ---This is to account for Providers who have closed/merged in the year or when a Provider Site was used as the submitter (for 21/22 it was RTV, ATM01, ATM02)
 SELECT DISTINCT
 ORG_CODE
 ,NAME
 ,CASE 				
 WHEN ORG_TYPE_CODE IN ('CT','TR') THEN 'NHS Trust'
 WHEN ORG_TYPE_CODE IN ('PH','LA','NN', 'PP') THEN 'Independent Health Provider'
 END AS ORG_TYPE_CODE				
 				
 FROM			reference_database.ORG_DAILY	
 				
 WHERE ORG_CODE IN ("RTV", "ATM01", "ATM02") AND BUSINESS_END_DATE is null

# COMMAND ----------

# DBTITLE 1,SIMILAR to get_rd_ccg_latest() but this doesn't use ODSAPI tables? Could be replaced maybe?
 %sql
 ----can be replaced by get_rd_ccg_latest()
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_LATEST AS
 
 SELECT 
 DISTINCT 
   ORG_CODE, 
   NAME
 FROM reference_database.org_daily
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
         AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
         AND ORG_TYPE_CODE = 'CC'
         AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
         AND ORG_OPEN_DATE <= '$rp_enddate'
         AND NAME NOT LIKE '%HUB'
         AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

# DBTITLE 1,SIMILAR get_mhs002gp_max_df() methodology but filter on rp_startdate and rp_enddate rather than UniqMonthID
 %sql
 CREATE OR REPLACE TEMPORARY VIEW CCG_PRAC AS 
 ---get latest GP for a person in the financial year
 SELECT 
 GP.Person_ID, 
 GP.OrgIDCCGGPPractice, 
 GP.OrgIDSubICBLocGP,
 GP.RecordNumber 
 FROM $db_source.mhs002gp AS GP
 INNER JOIN (SELECT 
               Person_ID, 
               MAX(RecordNumber) AS RecordNumber 
               FROM $db_source.mhs002gp 
               WHERE 
               GMPCodeReg NOT IN ('V81999','V81998','V819997') 
               AND EndDateGMPRegistration is NULL 
               GROUP BY Person_ID ) AS max_GP
          ON GP.Person_ID = max_GP.Person_ID AND GP.RecordNumber = max_GP.RecordNumber
  WHERE 
 (GP.RecordEndDate is null or GP.RecordEndDate >= '$rp_enddate') and GP.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'
  AND GMPCodeReg NOT IN ('V81999','V81998','V819997')
  AND EndDateGMPRegistration is NULL 

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
 ---Get relevant data fields for a single person to calculate CCG of GP Practice or Residence
 SELECT 
 a.Person_ID, 
 max(a.RecordNumber) as RecordNumber
 FROM $db_source.mhs001mpi AS a
 LEFT JOIN $db_source.mhs002gp AS b
     ON a.Person_ID = b.Person_ID 
         and a.uniqmonthid = b.uniqmonthid 
         and a.recordnumber = b.recordnumber
         AND b.GMPCodeReg NOT IN ('V81999', 'V81998','V81997')
         AND b.EndDateGMPRegistration is null
 LEFT JOIN global_temp.RD_CCG_LATEST AS c 
     ON a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN global_temp.RD_CCG_LATEST AS e 
     ON b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE 
 (e.ORG_CODE is not null or c.ORG_CODE is not null) 
 AND (a.RecordEndDate is null or a.RecordEndDate >= '$rp_enddate') and a.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'
 AND a.PATMRecInRP = true
 GROUP BY a.Person_ID

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Prep2 AS 
 
 SELECT 
 a.Person_ID, 
 CASE
     WHEN a.UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
     WHEN a.UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
     WHEN a.UNIQMONTHID <= 1467 then OrgIDCCGRes 
     WHEN a.UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
     ELSE 'ERROR'
     END as IC_Rec_CCG
 FROM $db_source.mhs001mpi AS a
 LEFT JOIN $db_source.mhs002gp AS b
    ON a.Person_ID = b.Person_ID 
        and a.uniqmonthid = b.uniqmonthid 
        and a.recordnumber = b.recordnumber
        AND b.GMPCodeReg NOT IN ('V81999', 'V81998','V81997')
        AND b.EndDateGMPRegistration is null
 INNER JOIN global_temp.CCG_PREP as ccg 
    on a.recordnumber = ccg.recordnumber
 LEFT JOIN global_temp.RD_CCG_LATEST AS c 
    ON a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN global_temp.RD_CCG_LATEST AS e 
    ON b.OrgIDCCGGPPractice = e.ORG_CODE
 
 WHERE 
 (e.ORG_CODE is not null or c.ORG_CODE is not null) 
 AND (a.RecordEndDate is null or a.RecordEndDate >= '$rp_enddate') and a.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG AS 
   
 SELECT 
 Person_ID,
 CASE 
   WHEN b.ORG_CODE is NULL THEN 'UNKNOWN' 
   ELSE b.ORG_CODE 
   END AS IC_Rec_CCG,
 CASE 
   WHEN NAME IS null THEN 'UNKNOWN' 
   ELSE NAME 
   END AS NAME
 FROM global_temp.CCG_PREP2 AS a
 LEFT JOIN global_temp.RD_CCG_LATEST AS b
   ON a.IC_Rec_CCG = b.ORG_CODE

# COMMAND ----------

# DBTITLE 0,ORG_DAILY required to get validf CCG breakdown
 %sql
 ---used to get STP/Region from CCG
 CREATE OR REPLACE TEMPORARY VIEW ORG_DAILY AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM reference_database.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 --AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 ---used to get relationship from CCG to STP/Region
 CREATE OR REPLACE TEMPORARY VIEW ORG_RELATIONSHIP_DAILY AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 reference_database.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_stp_mapping; 
 CREATE TABLE $db_output.mha_stp_mapping AS
 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 ORG_DAILY A
 LEFT JOIN ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

 %md
 ### MHA Latest Year Base Tables

# COMMAND ----------

 %sql
 create or replace temporary view mhs001_prov_latest as
 SELECT			 
 				B.orgidProv
 				,B.Person_ID
                 ,MAX(B.RecordNumber) as RecordNumber --get latest record for person_id, orgidprov combination
 FROM			$db_source.MHS001MPI B
 where (b.RecordEndDate is null or b.RecordEndDate >= '$rp_enddate') and b.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'
 group by B.UniqMonthID, B.orgidProv, B.Person_ID

# COMMAND ----------

 %sql
 -- Get latest MPI records for the year with some derived fields for demographic breakdowns
 DROP TABLE IF EXISTS $db_output.mhs001_latest; 
 CREATE TABLE $db_output.mhs001_latest AS
 
 SELECT			 
 				 B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.AgeRepPeriodEnd
                 ,B.EthnicCategory
                 ,B.LSOA2011
                 ,B.PatMRecInRP
                 ,B.RecordStartDate
                 ,B.RecordEndDate
                 ,B.RecordNumber
                 ,CASE WHEN B.GenderIDCode IN ('1', '2', '3', '4') THEN B.GenderIDCode --Person stated gender code in Male, Female, Non-binary, Other (not listed) then use GenderIDCode
                      WHEN B.Gender IN ('1', '2', '9') THEN B.Gender --Provider stated gender code in Male, Female, Indeterminate then use Gender code
                      ELSE "Unknown" END AS Der_Gender---NEW Gender breakdown for 2021/22
                 ,A.IC_REC_CCG
                 ,A.NAME as CCG_NAME  -- changed NAME to CCG_NAME to match with the detention column name as in MHA_unformatted_table notebook
                 ,C.STP_CODE
                 ,C.STP_NAME
 ,CASE
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
                 FROM $db_source.MHS001MPI AS B
                 INNER JOIN mhs001_prov_latest P ON B.Person_ID = P.Person_ID and B.OrgIDProv = P.OrgIDProv and B.RecordNumber = P.RecordNumber --get latest record for person_id, orgidprov combination
                 LEFT JOIN global_temp.CCG A ON A.PERSON_ID = b.PERSON_ID
                 LEFT JOIN $db_output.mha_stp_mapping c on a.IC_REC_CCG = c.CCG_CODE
                 LEFT JOIN reference_database.ENGLISH_INDICES_OF_DEP_V02 r on r.LSOA_CODE_2011 = B.LSOA2011 AND r.IMD_YEAR = '2019'
 				where (b.RecordEndDate is null or b.RecordEndDate >= '$rp_enddate') and b.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 -- Get latest MHA records for the year.
 
 DROP TABLE IF EXISTS $db_output.mhs401_latest; 
 CREATE TABLE $db_output.mhs401_latest AS
 
 SELECT			 
 				B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
                 ,B.InactTimeMHAPeriod
 				,B.StartDateMHActLegalStatusClass
 				,B.StartTimeMHActLegalStatusClass
 				,B.ExpiryDateMHActLegalStatusClass
 				,B.ExpiryTimeMHActLegalStatusClass
 				,B.EndDateMHActLegalStatusClass
 				,B.EndTimeMHActLegalStatusClass
 				,B.LegalStatusCode
 				,B.RecordStartDate
                 ,B.RecordEndDate
 				,B.RecordNumber
 
 FROM			$db_source.MHS401MHActPeriod
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 -- Get latest Conditional Discharge records for the year
 DROP TABLE IF EXISTS $db_output.mhs403_latest; 
 CREATE TABLE $db_output.mhs403_latest AS
  
 
 SELECT			 B.MHS403UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateMHCondDisch
 				,B.EndDateMHCondDisch
 				,B.CondDischEndReason
 				,B.AbsDischResp
 				,B.RecordStartDate
                 ,B.RecordEndDate
 				,B.RecordNumber
 
 FROM			$db_source.MHS403ConditionalDischarge
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 -- Get latest Community Treatment Order records for the year
 DROP TABLE IF EXISTS $db_output.mhs404_latest; 
 CREATE TABLE $db_output.mhs404_latest AS
 
 SELECT			 B.MHS404UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateCommTreatOrd
 				,B.ExpiryDateCommTreatOrd
 				,B.EndDateCommTreatOrd
 				,B.CommTreatOrdEndReason
 				,B.RecordStartDate
                 ,B.RecordEndDate
 				,B.RecordNumber
 
 FROM			$db_source.MHS404CommTreatOrder
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 -- Get latest Community Treatment Order Recall records for the year
 DROP TABLE IF EXISTS $db_output.mhs405_latest; 
 CREATE TABLE $db_output.mhs405_latest AS
 
 SELECT			 B.MHS405UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateCommTreatOrdRecall
 				,B.EndDateCommTreatOrdRecall
 				,B.RecordStartDate
                 ,B.RecordEndDate
 				,B.RecordNumber
 
 FROM			$db_source.MHS405CommTreatOrderRecall
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 -- Get latest Hospital Spell records for the year.
 
 DROP TABLE IF EXISTS $db_output.mhs501_latest; 
 CREATE TABLE $db_output.mhs501_latest AS
 
 SELECT			 
 				 B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqHospProvSpellID
                 ,B.InactTimeHPS
 				,B.StartDateHospProvSpell
 				,B.StartTimeHospProvSpell
 				,B.DischDateHospProvSpell
 				,B.DischTimeHospProvSpell
 				,B.SourceAdmMHHospProvSpell
 				,B.MethAdmMHHospProvSpell
 				,B.DestOfDischHospProvSpell
 				,B.MethOfDischMHHospProvSpell
 
 FROM			$db_source.MHS501HospProvSpell
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %md
 ### Main preparation tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_unformatted; 
 CREATE TABLE $db_output.mha_unformatted 
 (
 Year string,
 Measure string,
 MeasureSubCategory string,
 Demographic string,
 DemographicBreakdown string,
 OrganisationBreakdown string,
 DataSource string,
 OrgID string,
 OrgName string,
 METRIC_VALUE int
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.detentions;
 CREATE TABLE IF NOT EXISTS $db_output.detentions
 
 (
 Person_ID string
 ,MHA_RecordNumber string
 ,UniqMHActEpisodeID string
 ,orgidProv string
 ,NAME string
 ,ORG_TYPE_CODE string
 ,StartDateMHActLegalStatusClass date
 ,StartTimeMHActLegalStatusClass timestamp
 ,ExpiryDateMHActLegalStatusClass date
 ,EndDateMHActLegalStatusClass timestamp
 ,InactTimeMHAPeriod date---needed for MHA LOS methodology
 ,LegalStatusCode string
 ,MHA_RANK int
 ,HOSP_RANK int
 ,UniqHospProvSpellID string
 ,HOSP_ADM_RANK int
 ,PrevDischDestCodeHospProvSpell string
 ,AdmMethCodeHospProvSpell string
 ,StartDateHospProvSpell date
 ,StartTimeHospProvSpell timestamp
 ,DischDateHospProvSpell date
 ,DischTimeHospProvSpell timestamp
 ,InactTimeHPS date ---needed for MHA LOS methodology
 ,Detention_Cat string
 ,Detention_DateTime_Cat string 
 ,PrevUniqMHActEpisodeID string
 ,PrevRecordNumber string
 ,PrevLegalStatus string
 ,PrevMHAStartDate date
 ,PrevMHAEndDate date
 ,MHS404UniqID string
 ,CTORecordNumber string
 ,StartDateCommTreatOrd date
 ,EndDateCommTreatOrd date 
 ,CommTreatOrdEndReason string
 ,MHA_Logic_Cat_full string
 ,AgeRepPeriodEnd string
 ,EthnicCategory string
 ,Der_Gender string
 ,IC_REC_CCG string
 ,CCG_NAME string
 ,STP_CODE string
 ,STP_NAME string -- changed STP_Description to STP_NAME
 ,IMD_DECILE string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.short_term_orders;
 CREATE TABLE IF NOT EXISTS $db_output.short_term_orders
 
 (
 Person_ID string
 ,MHA_RecordNumber string
 ,UniqMHActEpisodeID string
 ,orgidProv string
 ,NAME string
 ,ORG_TYPE_CODE string
 ,StartDateMHActLegalStatusClass date
 ,StartTimeMHActLegalStatusClass timestamp
 ,ExpiryDateMHActLegalStatusClass date
 ,EndDateMHActLegalStatusClass timestamp
 ,InactTimeMHAPeriod date---needed for MHA LOS methodology
 ,LegalStatusCode string
 ,MHA_RANK int
 ,HOSP_RANK int
 ,UniqHospProvSpellID string
 ,HOSP_ADM_RANK int
 ,PrevDischDestCodeHospProvSpell string
 ,AdmMethCodeHospProvSpell string
 ,StartDateHospProvSpell date
 ,StartTimeHospProvSpell timestamp
 ,DischDateHospProvSpell date
 ,DischTimeHospProvSpell timestamp
 ,InactTimeHPS date ---needed for MHA LOS methodology
 ,Detention_Cat string
 ,Detention_DateTime_Cat string 
 ,PrevUniqMHActEpisodeID string
 ,PrevRecordNumber string
 ,PrevLegalStatus string
 ,PrevMHAStartDate date
 ,PrevMHAEndDate date
 ,MHS404UniqID string
 ,CTORecordNumber string
 ,StartDateCommTreatOrd date
 ,EndDateCommTreatOrd date 
 ,CommTreatOrdEndReason string
 ,MHA_Logic_Cat_full string
 ,AgeRepPeriodEnd string
 ,EthnicCategory string
 ,Der_Gender string
 ,IC_REC_CCG string
 ,CCG_NAME string
 ,STP_CODE string
 ,STP_NAME string
 ,IMD_DECILE string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cto;
 CREATE TABLE IF NOT EXISTS $db_output.cto
 
 (Person_ID STRING,
 CTO_UniqMHActEpisodeID STRING,
 MHS404UniqID STRING,
 orgidProv STRING,
 NAME STRING,
 ORG_TYPE_CODE STRING,
 StartDateCommTreatOrd DATE,
 ExpiryDateCommTreatOrd DATE,
 EndDateCommTreatOrd DATE,
 RecordEndDate DATE, ---needed for MHA LOS methodology
 CommTreatOrdEndReason STRING,
 MHA_UniqMHActEpisodeID STRING,
 LegalStatusCode STRING,
 StartDateMHActLegalStatusClass DATE,
 ExpiryDateMHActLegalStatusClass DATE,
 EndDateMHActLegalStatusClass DATE,
 AgeRepPeriodEnd STRING,
 EthnicCategory STRING,
 Der_Gender STRING
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.restrictive_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.restrictive_lookup
 (
 CODE STRING,
 LEGAL_STATUS STRING,
 Description STRING,
 CATEGORY STRING
 )

# COMMAND ----------

 %sql
 INSERT INTO $db_output.restrictive_lookup 
 
 Values
 ('31','34','Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('31','37','Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('30','15','Formally detained under Mental Health Act Section 47 with section 49 restrictions','Court and prison disposals'),
 ('29','17','Formally detained under Mental Health Act Section 48 with section 49 restrictions','Court and prison disposals'),
 ('28','9','Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('27','14','Formally detained under Mental Health Act Section 46','Court and prison disposals'),
 ('26','13','Formally detained under Mental Health Act Section 44','Court and prison disposals'),
 ('25','16','Formally detained under Mental Health Act Section 47','Court and prison disposals'),
 ('24','18','Formally detained under Mental Health Act Section 48','Court and prison disposals'),
 ('23.1','31','Formally detained under Criminal Proceedings (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991','Court and prison disposals'),
 ('23','10','Formally detained under Mental Health Act Section 37','Court and prison disposals'),
 ('22','12','Formally detained under Mental Health Act Section 38','Court and prison disposals'),
 ('21.5','36','Subject to guardianship under Mental Health Act Section 37','Court and prison disposals'),
 ('21','8','Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('20','7','Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('15','32','Formally detained under other acts','Court and prison disposals'),
 ('9','3','Formally detained under Mental Health Act Section 3','Part II'),
 ('7','33','Supervised Discharge (Mental Health (Patients in the Community) Act 1995)','Previous legislation other Acts'),
 ('6.5','35','Subject to guardianship under Mental Health Act Section 7','Court and prison disposals'),
 ('6','2','Formally detained under Mental Health Act Section 2','Part II'),
 ('5','4','Formally detained under Mental Health Act Section 4','Part II'),
 ('4','20','Formally detained under Mental Health Act Section 136','Place of safety'),
 ('3','19','Formally detained under Mental Health Act Section 135','Place of safety'),
 ('2','5','Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('1','6','Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('0','1','Informal (Not formally detained and not receiving supervised aftercare)','Informal'),
 ('-1','98','Not Applicable','Invalid / missing'),
 ('-1','99','Not Known','Invalid / missing'),
 ('-1','-1','Invalid Data Supplied','Invalid / missing'),
 ('0','01','Informal (Not formally detained and not receiving supervised aftercare)','Informal'),
 ('6','02','Formally detained under Mental Health Act Section 2','Part II'),
 ('9','03','Formally detained under Mental Health Act Section 3','Part II'),
 ('5','04','Formally detained under Mental Health Act Section 4','Part II'),
 ('2','05','Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('1','06','Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('20','07','Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('28','09','Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('21','08','Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('8','CTO','Community Treatment Order', 'Civil Community-based Detention')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.excel_lod_mha_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.excel_lod_mha_lookup
 (
 Description STRING,
 CATEGORY STRING
 )

# COMMAND ----------

 %sql
 INSERT INTO $db_output.excel_lod_mha_lookup 
 
 Values
 ('All', 'All Detentions'),
 ('All', 'Part II'),
 ('Formally detained under Mental Health Act Section 2','Part II'),
 ('Formally detained under Mental Health Act Section 3','Part II'),
 ('Formally detained under Mental Health Act Section 4','Part II'),
 ('Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('All', 'Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 37','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 38','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 46','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 47 with section 49 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 47','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 48 with section 49 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 48','Court and prison disposals'),
 ('Subject to guardianship under Mental Health Act Section 7','Court and prison disposals'),
 ('Subject to guardianship under Mental Health Act Section 37','Court and prison disposals'),
 ('Formally detained under other acts','Court and prison disposals'),
 ('Formally detained under Criminal Proceedings (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991','Court and prison disposals'),
 ('All', 'Place of safety'),
 ('Formally detained under Mental Health Act Section 136','Place of safety'),
 ('Formally detained under Mental Health Act Section 135','Place of safety')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_quartiles;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_quartiles
 
 (
 ID string,
 PERSON_ID string,
 MHA_STARTDATE date,
 MHA_ENDDATE date,
 DESCRIPTION string,
 CATEGORY string,
 MHA_LOS int,
 AgeRepPeriodEnd int,
 Age string,
 Der_Gender string,
 ETHNICCATEGORY string,
 HIGHER_ETHNIC_CATEGORY string,
 ETHNIC_CATEGORY_DESCRIPTION string,
 IC_REC_CCG string,
 CCG_NAME string,
 STP_CODE string,
 STP_NAME string,
 IMD_DECILE string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los
 
 (Geography string,
 OrgCode string,
 OrgName string,
 MHA_Most_Severe_Category string,
 MHA_Most_severe_Section string,
 Demographic string,
 Demographic_Category string,
 METRIC_VALUE string,
 LOWER_QUARTILE float,
 MEDIAN float,
 UPPER_QUARTILE float)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.restrictive_lookup_mha_cto;
 CREATE TABLE IF NOT EXISTS $db_output.restrictive_lookup_mha_cto
 (
 CODE STRING,
 LEGAL_STATUS STRING,
 Description STRING,
 CATEGORY STRING
 )

# COMMAND ----------

 %sql
 INSERT INTO $db_output.restrictive_lookup_mha_cto 
 
 Values
 ('31','34','Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('31','37','Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('30','15','Formally detained under Mental Health Act Section 47 with section 49 restrictions','Court and prison disposals'),
 ('29','17','Formally detained under Mental Health Act Section 48 with section 49 restrictions','Court and prison disposals'),
 ('28','9','Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('27','14','Formally detained under Mental Health Act Section 46','Court and prison disposals'),
 ('26','13','Formally detained under Mental Health Act Section 44','Court and prison disposals'),
 ('25','16','Formally detained under Mental Health Act Section 47','Court and prison disposals'),
 ('24','18','Formally detained under Mental Health Act Section 48','Court and prison disposals'),
 ('23.1','31','Formally detained under Criminal Proceedings (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991','Court and prison disposals'),
 ('23','10','Formally detained under Mental Health Act Section 37','Court and prison disposals'),
 ('22','12','Formally detained under Mental Health Act Section 38','Court and prison disposals'),
 ('21.5','36','Subject to guardianship under Mental Health Act Section 37','Court and prison disposals'),
 ('21','8','Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('20','7','Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('15','32','Formally detained under other acts','Court and prison disposals'),
 ('9','3','Formally detained under Mental Health Act Section 3','Part II'),
 ('7','33','Supervised Discharge (Mental Health (Patients in the Community) Act 1995)','Previous legislation other Acts'),
 ('6.5','35','Subject to guardianship under Mental Health Act Section 7','Court and prison disposals'),
 ('6','2','Formally detained under Mental Health Act Section 2','Part II'),
 ('5','4','Formally detained under Mental Health Act Section 4','Part II'),
 ('4','20','Formally detained under Mental Health Act Section 136','Place of safety'),
 ('3','19','Formally detained under Mental Health Act Section 135','Place of safety'),
 ('2','5','Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('1','6','Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('0','1','Informal (Not formally detained and not receiving supervised aftercare)','Informal'),
 ('-1','98','Not Applicable','Invalid / missing'),
 ('-1','99','Not Known','Invalid / missing'),
 ('-1','-1','Invalid Data Supplied','Invalid / missing'),
 ('0','01','Informal (Not formally detained and not receiving supervised aftercare)','Informal'),
 ('6','02','Formally detained under Mental Health Act Section 2','Part II'),
 ('9','03','Formally detained under Mental Health Act Section 3','Part II'),
 ('5','04','Formally detained under Mental Health Act Section 4','Part II'),
 ('2','05','Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('1','06','Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('20','07','Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('28','09','Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('21','08','Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('8','CTO','Community Treatment Order', 'Civil Community-based Detention')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.excel_lod_mha_cto_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.excel_lod_mha_cto_lookup
 (
 Description STRING,
 CATEGORY STRING
 )

# COMMAND ----------

 %sql
 INSERT INTO $db_output.excel_lod_mha_cto_lookup 
 
 Values
 ('All', 'All Detentions'),
 ('All', 'Part II'),
 ('Formally detained under Mental Health Act Section 2','Part II'),
 ('Formally detained under Mental Health Act Section 3','Part II'),
 ('Formally detained under Mental Health Act Section 4','Part II'),
 ('Formally detained under Mental Health Act Section 5(2)','Part II'),
 ('Formally detained under Mental Health Act Section 5(4)','Part II'),
 ('All', 'Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 35','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 36','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 37 with section 41 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 37','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 38','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 45A','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 46','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 47 with section 49 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 47','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 48 with section 49 restrictions','Court and prison disposals'),
 ('Formally detained under Mental Health Act Section 48','Court and prison disposals'),
 ('Subject to guardianship under Mental Health Act Section 7','Court and prison disposals'),
 ('Subject to guardianship under Mental Health Act Section 37','Court and prison disposals'),
 ('Formally detained under other acts','Court and prison disposals'),
 ('Formally detained under Criminal Proceedings (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991','Court and prison disposals'),
 ('All', 'Place of safety'),
 ('Formally detained under Mental Health Act Section 136','Place of safety'),
 ('Formally detained under Mental Health Act Section 135','Place of safety'),
 ('Community Treatment Order', 'Civil Community-based Detention')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_quartiles_cto;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_quartiles_cto
 
 (
 ID string,
 PERSON_ID string,
 MHA_STARTDATE date,
 MHA_ENDDATE date,
 DESCRIPTION string,
 CATEGORY string,
 mha_los int,
 AgeRepPeriodEnd int,
 Age string,
 Der_Gender string,
 ETHNICCATEGORY string,
 HIGHER_ETHNIC_CATEGORY string,
 ETHNIC_CATEGORY_DESCRIPTION string,
 IC_REC_CCG string,
 CCG_NAME string,
 STP_CODE string,
 STP_NAME string,
 IMD_DECILE string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cto_los1;
 CREATE TABLE IF NOT EXISTS $db_output.mha_cto_los1
 
 (Geography string,
 OrgCode string,
 OrgName string,
 MHA_Most_Severe_Category string,
 MHA_Most_severe_Section string,
 Demographic string,
 Demographic_Category string,
 METRIC_VALUE string,
 LOWER_QUARTILE float,
 MEDIAN float,
 UPPER_QUARTILE float)