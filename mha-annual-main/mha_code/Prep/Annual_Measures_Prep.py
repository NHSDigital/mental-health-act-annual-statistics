# Databricks notebook source
 %md
 ### MHA General Prep

# COMMAND ----------

 %sql
 ---Get latest CTO record for a person, provider and startdate of CTO
 CREATE OR REPLACE TEMPORARY VIEW MHS404CTO_Latest_Ranked AS

 SELECT
 *

 FROM
     (SELECT 
     *
     ,dense_rank() over (partition by Person_ID, orgidProv, StartDateCommTreatOrd 
     order by RecordStartDate DESC, CASE WHEN EndDateCommTreatOrd is not null then 2 else 1 end DESC, EndDateCommTreatOrd DESC, MHS404UniqID DESC) 
     AS CTO_DUP_RANK ---Order by oldest RecordStartDate, want open CTOs first to take priority, EndDate CTO and RowID desc 
     FROM $db_output.mhs404_latest) a
 where CTO_DUP_RANK = '1'
 order by Person_ID, StartDateCommTreatOrd

# COMMAND ----------

 %sql
 ---Join latest Revoked CTOs to MHA record
 CREATE OR REPLACE TEMPORARY VIEW Revoked_CTO AS

 SELECT			B.Person_ID,
 				b.UniqMHActEpisodeID,
 				B.MHS404UniqID,
 				B.StartDateCommTreatOrd,
 				B.EndDateCommTreatOrd,
 				B.CommTreatOrdEndReason,
 				a.LegalStatusCode,
 				A.StartDateMHActLegalStatusClass,
 				A.EndDateMHActLegalStatusClass,
                 B.RecordEndDate, ---needed for MHA LOS methodology
 				B.recordnumber

 FROM			MHS404CTO_Latest_Ranked
 					AS B
 	LEFT JOIN $db_source.MHS401MHActPeriod AS A ON B.UniqMHActEpisodeID = A.UniqMHActEpisodeID AND (a.RecordEndDate is null or a.RecordEndDate >= '$rp_enddate') and a.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'
 				
 				where B.CommTreatOrdEndReason = '02'

# COMMAND ----------

 %sql
 -- Rank hospital spells in order. This is done per patient. 

 -- This means that for each patient their hospital spells will be ranked into chronological order. This allows us to get the Previous Discharge Destination for a hospital spell which is used later.

 CREATE OR REPLACE TEMPORARY VIEW MHS501_Ranked AS

 SELECT
 *
 ,dense_rank() over (partition by Person_ID order by StartDateHospProvSpell ASC, StartTimeHospProvSpell ASC, Case when DischDateHospProvSpell is not null then 1 else 2 end asc, Case when DischDateHospProvSpell is not null then DischDateHospProvSpell end asc, DischTimeHospProvSpell ASC, Case when DischDateHospProvSpell is null then 1 else 2 end asc, UniqMonthID DESC) AS HOSP_ADM_RANK

 FROM	(SELECT 
 		*
 		,dense_rank() over (partition by Person_ID, orgidProv, StartDateHospProvSpell, DischDateHospProvSpell  order by UniqMonthID DESC, UniqHospProvSpellID DESC) AS HOSP_DUP_RANK
 		FROM $db_output.mhs501_latest) AS A
 WHERE HOSP_DUP_RANK = '1'
 ORDER BY 
 Person_ID, StartDateHospProvSpell, DischDateHospProvSpell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW HOSP_ADM AS

 Select 
 distinct 
 HOSP_ADM_RANK,
 (HOSP_ADM_RANK - 1) AS PREV_HOSP_ADM_RANK
 FROM 
 MHS501_Ranked

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW MHS501HospSpell_Latest_Ranked AS
 SELECT 
  a.UniqMonthID
 ,a.orgidProv
 ,a.Person_ID
 ,a.UniqHospProvSpellID
 ,a.StartDateHospProvSpell
 ,a.StartTimeHospProvSpell
 ,a.DischDateHospProvSpell
 ,a.DischTimeHospProvSpell
 ,a.SourceAdmMHHospProvSpell
 ,a.MethAdmMHHospProvSpell
 ,a.DestOfDischHospProvSpell
 ,a.MethOfDischMHHospProvSpell
 ,a.InactTimeHPS ---needed for MHA LOS methodology
 ,a.HOSP_ADM_RANK
 ,B.StartDateHospProvSpell as PrevStartDateHospProvSpell ---Prev Hosp Spell Destination
 ,b.DischDateHospProvSpell as PrevDischDateHospProvSpell
 ,b.DestOfDischHospProvSpell as PrevDestOfDischHospProvSpell
 FROM 
 MHS501_Ranked a
 left join HOSP_ADM rnk on a.HOSP_ADM_RANK = rnk.HOSP_ADM_RANK
 left join MHS501_Ranked b on a.Person_ID = b.Person_ID and b.HOSP_ADM_RANK = rnk.PREV_HOSP_ADM_RANK

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_kp90;
 CREATE TABLE IF NOT EXISTS $db_output.mha_kp90 AS

 SELECT
  A.Person_ID
 ,A.UniqMHActEpisodeID
 ,A.RecordNumber
 ,A.orgidProv
 ,A.StartDateMHActLegalStatusClass
 ,A.StartTimeMHActLegalStatusClass
 ,A.ExpiryDateMHActLegalStatusClass
 ,A.EndDateMHActLegalStatusClass
 ,A.EndTimeMHActLegalStatusClass
 ,A.InactTimeMHAPeriod ---needed for MHA LOS methodology
 ,A.LegalStatusCode
 ,dense_rank() over (partition by a.Person_ID, A.orgidProv ,a.StartDateMHActLegalStatusClass, A.LegalStatusCode order by A.RecordStartDate DESC, ExpiryDateMHActLegalStatusClass DESC, EndDateMHActLegalStatusCLass DESC, A.UniqMHActEpisodeID DESC) AS MHA_RANK
 ,B.UniqHospProvSpellID
 ,B.HOSP_ADM_RANK
 ,B.PrevDestOfDischHospProvSpell ---NEW DATA ITEM
 ,B.MethAdmMHHospProvSpell ---NEW DATA ITEM
 ,B.StartDateHospProvSpell
 ,B.StartTimeHospProvSpell
 ,B.DischDateHospProvSpell
 ,B.DischTimeHospProvSpell
 ,B.InactTimeHPS ---needed for MHA LOS methodology
 ,CASE
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA'
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
 	ELSE 'UNKNOWN' END AS Detention_Cat 
 ,CASE
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
 	ELSE 'UNKNOWN' END AS Detention_DateTime_Cat 

 FROM
 $db_output.mhs401_latest A
 LEFT JOIN MHS501HospSpell_Latest_Ranked B ON A.Person_ID = B.Person_ID and A.orgidProv = B.orgidProv and CASE
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
 	ELSE 'UNKNOWN' END <> 'NA'
 ORDER BY 
  A.Person_ID
 ,a.StartDateMHActLegalStatusClass
 ,MHA_RANK
 ,A.UniqMHActEpisodeID
 ,b.StartDateHospProvSpell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW KP90a AS

 SELECT
  A.Person_ID
 ,A.UniqMHActEpisodeID
 ,A.RecordNumber
 ,A.orgidProv
 ,A.StartDateMHActLegalStatusClass
 ,A.StartTimeMHActLegalStatusClass
 ,A.ExpiryDateMHActLegalStatusClass
 ,A.EndDateMHActLegalStatusClass
 ,A.InactTimeMHAPeriod ---needed for MHA LOS methodology
 ,A.LegalStatusCode
 ,MHA_RANK
 ,dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass ASC, A.StartTimeMHActLegalStatusClass ASC, CASE when A.EndDateMHActLegalStatusClass is null then 1 else 2 end asc, A.EndDateMHActLegalStatusClass ASC, CASE when A.EndTimeMHActLegalStatusClass is null then 1 else 2 end asc, A.EndTimeMHActLegalStatusClass ASC) AS HOSP_RANK
 ,UniqHospProvSpellID
 ,HOSP_ADM_RANK
 ,PrevDestOfDischHospProvSpell ---NEW DATA ITEM
 ,MethAdmMHHospProvSpell ---NEW DATA ITEM
 ,StartDateHospProvSpell
 ,StartTimeHospProvSpell
 ,DischDateHospProvSpell
 ,DischTimeHospProvSpell
 ,InactTimeHPS ---needed for MHA LOS methodology
 ,CASE
 	WHEN UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
 	WHEN StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA'
 	WHEN DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass > StartDateHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass < StartDateHospProvSpell THEN 'TOS'
 	ELSE 'UNKNOWN' END AS Detention_Cat 
 ,CASE
 	WHEN UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
 	WHEN StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass or (a.StartDateMHActLegalStatusClass = DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > DischTimeHospProvSpell) THEN 'NA'
 	WHEN DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = StartTimeHospProvSpell THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > StartTimeHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass > StartDateHospProvSpell THEN 'DSA'
 	WHEN A.StartDateMHActLegalStatusClass < StartDateHospProvSpell THEN 'TOS'
 	ELSE 'UNKNOWN' END AS Detention_DateTime_Cat 
 FROM
 $db_output.mha_kp90 A
 WHERE
 MHA_RANK = '1'
 ORDER BY 
  A.Person_ID
 ,a.StartDateMHActLegalStatusClass
 ,MHA_RANK
 ,A.UniqMHActEpisodeID
 ,StartDateHospProvSpell

# COMMAND ----------

 %sql
 /*
 A table is created to link the curernt MHA episode to any exisiting previous one. This link is done using PersonID and Hospital Spell Number. 
 The current MHA episode is selected using = @RANK and the join uses = @RANK -1.
 */
 CREATE OR REPLACE TEMPORARY VIEW HOSP_RANK AS 

 SELECT 
 DISTINCT
 HOSP_RANK,
 (HOSP_RANK - 1) AS PREV_HOSP_RANK
 FROM KP90a

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW KP90_1 AS

 SELECT 
 A.Person_ID
 ,A.UniqMHActEpisodeID
 ,A.RecordNumber
 ,A.orgidProv
 ,A.StartDateMHActLegalStatusClass
 ,A.StartTimeMHActLegalStatusClass
 ,A.ExpiryDateMHActLegalStatusClass
 ,A.EndDateMHActLegalStatusClass
 ,A.InactTimeMHAPeriod ---needed for MHA LOS methodology
 ,A.LegalStatusCode
 ,A.MHA_RANK
 ,A.HOSP_RANK
 ,A.UniqHospProvSpellID
 ,A.HOSP_ADM_RANK
 ,A.PrevDestOfDischHospProvSpell
 ,A.MethAdmMHHospProvSpell
 ,A.StartDateHospProvSpell
 ,A.StartTimeHospProvSpell
 ,A.DischDateHospProvSpell
 ,A.DischTimeHospProvSpell
 ,A.InactTimeHPS ---needed for MHA LOS methodology
 ,A.Detention_Cat
 ,A.Detention_DateTime_Cat
 ,B.UniqMHActEpisodeID as PrevUniqMHActEpisodeID
 ,B.RecordNumber as PrevRecordNumber
 ,B.LegalStatusCode AS PrevLegalStatus
 ,B.StartDateMHActLegalStatusClass as PrevMHAStartDate
 ,B.EndDateMHActLegalStatusClass as PrevMHAEndDate
 FROM KP90a A 
 LEFT JOIN HOSP_RANK rnk on a.HOSP_RANK = rnk.HOSP_RANK 
 LEFT JOIN KP90a B ON A.Person_ID = B.Person_ID AND A.UniqHospProvSpellID = B.UniqHospProvSpellID AND B.HOSP_RANK = rnk.PREV_HOSP_RANK

# COMMAND ----------

 %sql
 /* 
 This is the final data sheet. Some Organisations are hard coded as they have since expired in the year and as such dont get pulled through in the ORG_DAILY table.

 The MHA_Logic_Cat provides basic logic on how each MHA episode falls into the certain categories.

 MHA_Logic_Cat_Full is the full logic and includes much more detail. This is the one which should be used..

 Everything is included in the group by as some rows seemed to be coming through as complete duplicates.

 Hard coded Organisations are due to some Orgs expiring midway through the year.
 */

 CREATE OR REPLACE TEMPORARY VIEW KP90_2 AS

 SELECT 
 A.Person_ID
 ,A.RecordNumber as MHA_RecordNumber
 ,A.UniqMHActEpisodeID
 ,A.orgidProv
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
 	WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
 	WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
 	ELSE b.NAME END as NAME
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
 	ELSE b.ORG_TYPE_CODE END as ORG_TYPE_CODE
 ,A.StartDateMHActLegalStatusClass
 ,A.StartTimeMHActLegalStatusClass
 ,A.ExpiryDateMHActLegalStatusClass
 ,A.EndDateMHActLegalStatusClass
 ,A.InactTimeMHAPeriod ---needed for MHA LOS methodology
 ,A.LegalStatusCode
 ,A.MHA_RANK
 ,A.HOSP_RANK
 ,A.UniqHospProvSpellID
 ,A.HOSP_ADM_RANK
 ,A.PrevDestOfDischHospProvSpell
 ,A.MethAdmMHHospProvSpell
 ,A.StartDateHospProvSpell
 ,A.StartTimeHospProvSpell
 ,A.DischDateHospProvSpell
 ,A.DischTimeHospProvSpell
 ,A.InactTimeHPS ---needed for MHA LOS methodology
 ,A.Detention_Cat
 ,A.Detention_DateTime_Cat
 ,PrevUniqMHActEpisodeID
 ,PrevRecordNumber
 ,PrevLegalStatus
 ,PrevMHAStartDate
 ,PrevMHAEndDate
 ,CASE
 	when c.MHS404UniqID is null then d.MHS404UniqID
 	ELSE c.MHS404UniqID
 	END AS MHS404UniqID
 ,CASE
 	when c.RecordNumber is null then d.RecordNumber
 	ELSE c.RecordNumber
 	END AS CTORecordNumber
 ,CASE
 	when c.StartDateCommTreatOrd is null then d.StartDateCommTreatOrd
 	ELSE c.StartDateCommTreatOrd
 	END AS StartDateCommTreatOrd
 ,CASE
 	when c.EndDateCommTreatOrd is null then d.EndDateCommTreatOrd
 	ELSE c.EndDateCommTreatOrd
 	END AS EndDateCommTreatOrd
 ,CASE
 	when c.CommTreatOrdEndReason is null then d.CommTreatOrdEndReason
 	ELSE c.CommTreatOrdEndReason
 	END AS CommTreatOrdEndReason
 FROM KP90_1 a
 left join $db_output.mha_rd_org_daily_latest b on a.orgidProv = b.ORG_CODE 
 left join Revoked_CTO c on a.PrevUniqMHActEpisodeID = c.UniqMHActEpisodeID and c.StartDateCommTreatOrd < a.StartDateHospProvSpell and (c.EndDateCommTreatOrd > a.StartDateHospProvSpell or c.EndDateCommTreatOrd is null)
 left join Revoked_CTO d on a.UniqMHActEpisodeID = d.UniqMHActEpisodeID and d.StartDateCommTreatOrd < a.StartDateHospProvSpell and (d.EndDateCommTreatOrd > a.StartDateHospProvSpell or d.EndDateCommTreatOrd is null)
 WHERE MHA_RANK = 1 
 GROUP BY 
 A.Person_ID
 ,A.RecordNumber
 ,A.UniqMHActEpisodeID
 ,A.orgidProv
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
 	WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
 	WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
 	ELSE b.NAME END
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
 	ELSE b.ORG_TYPE_CODE END
 ,A.StartDateMHActLegalStatusClass
 ,A.StartTimeMHActLegalStatusClass
 ,A.ExpiryDateMHActLegalStatusClass
 ,A.EndDateMHActLegalStatusClass
 ,A.InactTimeMHAPeriod ---needed for MHA LOS methodology
 ,A.LegalStatusCode
 ,A.MHA_RANK
 ,A.HOSP_RANK
 ,A.UniqHospProvSpellID
 ,A.HOSP_ADM_RANK
 ,A.PrevDestOfDischHospProvSpell
 ,A.MethAdmMHHospProvSpell
 ,A.StartDateHospProvSpell
 ,A.StartTimeHospProvSpell
 ,A.DischDateHospProvSpell
 ,A.DischTimeHospProvSpell
 ,A.InactTimeHPS ---needed for MHA LOS methodology
 ,A.Detention_Cat
 ,A.Detention_DateTime_Cat
 ,PrevUniqMHActEpisodeID
 ,PrevRecordNumber
 ,PrevLegalStatus
 ,PrevMHAStartDate
 ,PrevMHAEndDate
 ,CASE
 	when c.MHS404UniqID is null then d.MHS404UniqID
 	ELSE c.MHS404UniqID
 	END
 ,CASE
 	when c.RecordNumber is null then d.RecordNumber
 	ELSE c.RecordNumber
 	END
 ,CASE
 	when c.StartDateCommTreatOrd is null then d.StartDateCommTreatOrd
 	ELSE c.StartDateCommTreatOrd
 	END
 ,CASE
 	when c.EndDateCommTreatOrd is null then d.EndDateCommTreatOrd
 	ELSE c.EndDateCommTreatOrd
 	END
 ,CASE
 	when c.CommTreatOrdEndReason is null then d.CommTreatOrdEndReason
 	ELSE c.CommTreatOrdEndReason
 	END
 ORDER BY 
  Person_ID
 ,StartDateMHActLegalStatusClass
 ,StartDateHospProvSpell
 ,HOSP_RANK

# COMMAND ----------

 %sql
 /*

 For the MHA_Final logic is calculated to work out how the MHA is being used. This field is MHA_Logic_Cat_full.

 Categories for MHA_Logic_Cat_full:
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

 #AM- JULY2025: Fixed the missing bracket in the condition to define MHA_Logic_Cat_full as B, Same fix as done on monthly pub menh_bbrb earlier in 2025 as part of developing new MHA measures

 */
 DROP TABLE IF EXISTS $db_output.mha_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_final AS

 SELECT 
 a.*
 ,CASE 
 	WHEN LegalStatusCode = '01' 
     THEN NULL
 	WHEN LegalStatusCode in ('02','03') 
           and StartDateMHActLegalStatusClass = StartDateHospProvSpell 
           and (StartTimeMHActLegalStatusClass <= StartTimeHospProvSpell or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null)
           and ((PrevLegalStatus is null or PrevLegalStatus = '01') or (DATEDIFF(StartDateHospProvSpell,PrevMHAEndDate) > 1)) 
     THEN 'A'
 	WHEN LegalStatusCode = '02' 
           AND StartDateHospProvSpell between DATE_ADD(StartDateMHActLegalStatusClass,-5) and StartDateMHActLegalStatusClass 
           and MethAdmMHHospProvSpell = '2A' 
     THEN 'A'
     WHEN LegalStatusCode in ('02','03') 
               and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass 
                   and ((StartTimeHospProvSpell < StartTimeMHActLegalStatusClass) or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null))
                   or StartDateMHActLegalStatusClass > StartDateHospProvSpell)
               and (((PrevLegalStatus is null or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1)))) 
                   or ((PrevLegalStatus in ('04','05','06') and (PrevMHAEndDate = StartDateMHActLegalStatusClass or PrevMHAEndDate = DATE_ADD(StartDateMHActLegalStatusClass,-1)))
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))))) 
         THEN 'B'
 	WHEN LegalStatusCode in ('02','03') 
           and ((StartDateMHActLegalStatusClass = PrevMHAEndDate) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) or (StartDateMHActLegalStatusClass = PrevMHAStartDate)))
           and PrevLegalStatus in ('19','20') 
           and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass) or (StartDateHospProvSpell >= PrevMHAStartDate) or (StartDateMHActLegalStatusClass = PrevMHAEndDate)) 
     THEN 'C'
 	WHEN  (LegalStatusCode in ('03','09','10','15','16') 
               and ((StartDateMHActLegalStatusClass >= DATE_ADD(EndDateCommTreatOrd,-1) or EndDateCommTreatOrd is null) 
               and CommTreatOrdEndReason = '02') 
               and (StartDateHospProvSpell = StartDateMHActLegalStatusClass or StartDateHospProvSpell >= StartDateCommTreatOrd))
               or (LegalStatusCode = '03' and CommTreatOrdEndReason = '02' and StartDateMHActLegalStatusClass < StartDateHospProvSpell and EndDateCommTreatOrd between DATE_ADD(StartDateHospProvSpell,-1) 
               and DATE_ADD(StartDateHospProvSpell,2))  
         THEN 'D'
 	WHEN LegalStatusCode in ('19','20') 
           and (PrevMHAStartDate is null or PrevLegalStatus = '01' or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
     THEN 'E'
 	WHEN ((LegalStatusCode in ('04','05','06') 
           and StartDateMHActLegalStatusClass = StartDateHospProvSpell) or (LegalStatusCode in ('05','06') 
           and StartDateMHActLegalStatusClass >= StartDateHospProvSpell)) 
           and ((PrevLegalStatus  is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass) or PrevLegalStatus = '01' or (PrevLegalStatus in ('02','03') 
           and PrevMHAEndDate = StartDateMHActLegalStatusClass)) 
     THEN 'F'
 	WHEN LegalStatusCode IN ('03','07','08','09','10','12','15','16','17','18') 
           AND StartDateMHActLegalStatusClass > StartDateHospProvSpell 
           AND LegalStatusCode = PrevLegalStatus
           and ((StartDateMHActLegalStatusClass BETWEEN PrevMHAEndDate AND DATE_ADD(PrevMHAEndDate,1) or PrevMHAEndDate is null)) 
     THEN 'G'
 	WHEN LegalStatusCode IN ('02','03','07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
           and StartDateHospProvSpell > StartDateMHActLegalStatusClass 
           and (MethAdmMHHospProvSpell in ('81','2B','11','12','13') or PrevDestOfDischHospProvSpell in ('49','51','50','52','53','87')) 
     THEN 'H'
 	WHEN LegalStatusCode = '05' 
           and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
           and PrevLegalStatus = '06' 
           and (StartDateMHActLegalStatusClass >= PrevMHAStartDate 
           and (PrevMHAEndDate is null or PrevMHAEndDate <= EndDateMHActLegalStatusClass or EndDateMHActLegalStatusClass is null)) 
     THEN 'J'
 	WHEN LegalStatusCode = '10' 
           and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
           and PrevLegalStatus = '07' 
           and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
     THEN 'K'
 	WHEN LegalStatusCode = '03' 
           and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
           and PrevLegalStatus = '02' 
           and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
     THEN 'L'
 	WHEN LegalStatusCode in ('35','36') 
     THEN 'M'
 	WHEN LegalStatusCode in ('07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
           and StartDateMHActLegalStatusClass <= StartDateHospProvSpell 
           and (EndDateMHActLegalStatusClass is null or EndDateMHActLegalStatusClass >= StartDateHospProvSpell)
           and MethAdmMHHospProvSpell in ('11','12','13') 
           and (PrevLegalStatus is null or (PrevLegalStatus in ('01','02','03','04','05','06') 
           and (PrevMHAEndDate is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass))) 
           and (MHS404UniqID is null or (EndDateCommTreatOrd is not null 
           and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))) 
     THEN 'P'
 	ELSE 'N'
 	END as MHA_Logic_Cat_full
 ,d.AgeRepPeriodEnd
 ,d.EthnicCategory
 ,d.Der_Gender ---New for MHA 2021/22
 ,COALESCE(D.IC_REC_CCG, "UNKNOWN") as IC_REC_CCG
 ,COALESCE(D.CCG_NAME, "UNKNOWN")as CCG_NAME  -- CA changed NAME to CCG_NAME to match with the detention column name as in MHA_unformatted_table notebook
 ,COALESCE(D.STP_CODE, "UNKNOWN") as STP_CODE
 ,COALESCE(D.STP_NAME, "UNKNOWN") as STP_NAME
 ,D.IMD_DECILE
 from KP90_2 a 
 left join $db_output.mhs001_latest d on a.Person_ID = d.Person_ID and a.orgidProv = d.orgidProv
 ORDER BY 
  a.Person_ID
 ,StartDateMHActLegalStatusClass
 ,StartDateHospProvSpell
 ,HOSP_RANK

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cto_final;
 CREATE TABLE IF NOT EXISTS $db_output.cto_final AS 

 SELECT 
 A.Person_ID,
 A.UniqMHActEpisodeID as CTO_UniqMHActEpisodeID,
 A.MHS404UniqID,
 A.orgidProv
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
 	WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
 	WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
 	ELSE c.NAME END as NAME
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
 	ELSE c.ORG_TYPE_CODE END as ORG_TYPE_CODE,
 A.StartDateCommTreatOrd,
 A.ExpiryDateCommTreatOrd,
 A.EndDateCommTreatOrd,
 A.RecordEndDate, ---needed for MHA LOS methodology
 A.CommTreatOrdEndReason,
 B.UniqMHActEpisodeID as MHA_UniqMHActEpisodeID,
 B.LegalStatusCode,
 B.StartDateMHActLegalStatusClass,
 B.ExpiryDateMHActLegalStatusClass,
 B.EndDateMHActLegalStatusClass
 ,d.AgeRepPeriodEnd
 ,d.EthnicCategory
 ,d.Der_Gender
 FROM
 MHS404CTO_Latest_Ranked A
 left join $db_output.mha_final B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID
 LEFT JOIN $db_output.mha_rd_org_daily_latest C ON A.orgidProv = C.ORG_CODE
 left join $db_output.mhs001_latest d on a.Person_ID = d.Person_ID AND A.ORGIDPROV = D.ORGIDPROV
 GROUP BY
 A.Person_ID,
 A.UniqMHActEpisodeID,
 A.MHS404UniqID,
 A.orgidProv
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
 	WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
 	WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
 	ELSE c.NAME END
 ,CASE
 	WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
 	WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
 	ELSE c.ORG_TYPE_CODE END,
 A.StartDateCommTreatOrd,
 A.ExpiryDateCommTreatOrd,
 A.EndDateCommTreatOrd,
 A.RecordEndDate, ---needed for MHA LOS methodology
 A.CommTreatOrdEndReason,
 B.UniqMHActEpisodeID,
 B.LegalStatusCode,
 B.StartDateMHActLegalStatusClass,
 B.ExpiryDateMHActLegalStatusClass,
 B.EndDateMHActLegalStatusClass
 ,d.AgeRepPeriodEnd
 ,d.EthnicCategory
 ,d.Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.detentions

 SELECT 
 A.*
 FROM
 $db_output.mha_final A
 INNER JOIN (SELECT 
 			UniqMHActEpisodeID,
 			MIN(StartDateHospProvSpell) AS HOSP_START ---Get first Hospital Spell for each MHA ID
 			FROM
 			$db_output.mha_final
 			WHERE
 			StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'
 			GROUP BY
 			UniqMHActEpisodeID) AS B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START
 WHERE
 StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.short_term_orders

 SELECT 
 *
 FROM 
 $db_output.mha_final
 WHERE
 StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cto

 SELECT 
 *
 FROM 
 $db_output.cto_final a
 where 
 StartDateCommTreatOrd between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %md
 ### MHA only LOS Prep

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW SINGLE_MPI_RECORD_IN_YEAR AS
 SELECT 
 MPI.orgidProv
 ,MPI.Person_ID
 ,MPI.AgeRepPeriodEnd
 ,MPI.EthnicCategory
 ,MPI.LSOA2011
 ,MPI.PatMRecInRP
 ,MPI.RecordStartDate
 ,MPI.RecordEndDate
 ,MPI.RecordNumber
 ,MPI.Der_Gender---NEW Gender breakdown for 2021/22
 ,MPI.IC_REC_CCG
 ,MPI.CCG_NAME  -- changed NAME to CCG_NAME to match with the detention column name as in MHA_unformatted_table notebook
 ,MPI.STP_CODE
 ,MPI.STP_NAME
 ,MPI.IMD_Decile
 FROM $db_output.mhs001_latest mpi
 INNER JOIN (SELECT DISTINCT PERSON_ID, MAX(UNIQMONTHID) as UniqMonthID from $db_output.mhs001_latest WHERE PatMRecInRP = TRUE GROUP BY PERSON_ID) B ON mpi.Person_ID = B.Person_ID AND mpi.UniqMonthID = B.UniqMonthID
 WHERE PatMRecInRP = true

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW ALL_MHA AS 

 SELECT 
 DISTINCT
 PERSON_ID, 
 UNIQMHACTEPISODEID,
 LEGALSTATUSCODE,
 STARTDATEMHACTLEGALSTATUSCLASS,
 CASE
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND (InactTimeMHAPeriod IS NULL OR InactTimeMHAPeriod >= '$rp_enddate') 
   AND (DISCHDATEHOSPPROVSPELL IS NULL OR DISCHDATEHOSPPROVSPELL >= '$rp_enddate') THEN date_add('$rp_enddate', 1)
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND DISCHDATEHOSPPROVSPELL < InactTimeMHAPeriod THEN DISCHDATEHOSPPROVSPELL
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND InactTimeMHAPeriod IS NOT NULL THEN InactTimeMHAPeriod
   ELSE ENDDATEMHACTLEGALSTATUSCLASS
   END AS ENDDATEMHACTLEGALSTATUSCLASS,
 DISCHDATEHOSPPROVSPELL
 FROM
 $db_output.detentions
 WHERE
 (LEGALSTATUSCODE IS NOT NULL AND LEGALSTATUSCODE IN ('02','2','03','3','04','4','05','5','06','6','07','7','08','8','09','9','10','12','13','14','15','16','17','18','19','20','31','32','35','36','37','38'
 ))

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW ALL_EPI AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.UniqMHActEpisodeID,
 A.LEGALSTATUSCODE,
 B.DAY_DATE
 FROM
 ALL_MHA A
 LEFT JOIN $reference_data.CALENDAR B on B.DAY_DATE BETWEEN A.STARTDATEMHACTLEGALSTATUSCLASS AND A.EndDateMHActLegalStatusClass
 ORDER BY 1

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW ALL_EPI_1 AS 

 SELECT 
 DISTINCT
 PERSON_ID,
 DAY_DATE,
 RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC) AS RANK,
 DATE_ADD(DAY_DATE, -(RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC))) AS GROUP
 FROM
   (SELECT
   DISTINCT
   PERSON_ID,
   DAY_DATE
   FROM
   ALL_EPI
   ) A
 ORDER BY 1

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW CONT_EPI AS

 SELECT 
 DISTINCT
 PERSON_ID,
 MIN(DAY_DATE) AS START_DATE,
 MAX(DAY_DATE) AS END_DATE
 FROM 
 ALL_EPI_1
 GROUP BY 
 PERSON_ID,
 GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MHA_SEVERITY AS

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 MAX(CODE) AS MHA_SEVERITY
 FROM 
 CONT_EPI A
 LEFT JOIN ALL_EPI B ON B.DAY_DATE BETWEEN A.START_DATE AND A.END_DATE AND A.PERSON_ID = B.PERSON_ID
 LEFT JOIN $db_output.RESTRICTIVE_LOOKUP C on B.LEGALSTATUSCODE = C.LEGAL_STATUS 
 GROUP BY 
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MHA_LOS AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 B.DESCRIPTION,
 B.CATEGORY,
 DATEDIFF(CASE
             WHEN A.END_DATE = '2100-01-01' THEN '$rp_enddate' ELSE A.END_DATE END
             ,A.START_DATE) AS MHA_LOS
 FROM 
 MHA_SEVERITY A
 LEFT JOIN $db_output.RESTRICTIVE_LOOKUP B on A.MHA_SEVERITY = B.CODE

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MHA_LOS_PREP AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 A.DESCRIPTION,
 A.CATEGORY,
 A.MHA_LOS,
 C.AgeRepPeriodEnd,
 C.EthnicCategory,
 C.Der_Gender,
 C.IC_REC_CCG,
 C.CCG_NAME,
 C.STP_CODE,
 C.STP_NAME,
 C.IMD_DECILE
 FROM 
 MHA_LOS A 
 LEFT JOIN SINGLE_MPI_RECORD_IN_YEAR C ON A.PERSON_ID = C.PERSON_ID AND C.PatMRecInRP = true ---get only latest submitted mpi record (in cases where same person_id seen by more than one provider in FY)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_prep_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_prep_final AS

 SELECT
 DISTINCT
 PERSON_ID,
 START_DATE AS MHA_STARTDATE,
 END_DATE AS MHA_ENDDATE,
 DESCRIPTION,
 CATEGORY,
 MHA_LOS,
 AgeRepPeriodEnd,
 CASE
   WHEN AgeRepPeriodEnd <= 15 THEN '15 and under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65  THEN '65 and over'
   ELSE 'UNKNOWN' 
   END AS Age,
 Der_Gender,
 ETHNICCATEGORY,
 CASE
   WHEN EthnicCategory IN ('A','B','C') THEN 'White'
   WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
   WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
   WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
   WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
   ELSE 'Unknown'
   END AS HIGHER_ETHNIC_CATEGORY,
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British'
     WHEN EthnicCategory = 'B' THEN 'White Irish'
     WHEN EthnicCategory = 'C' THEN 'Any Other White background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END AS ETHNIC_CATEGORY_DESCRIPTION,
 COALESCE(IC_REC_CCG, "UNKNOWN") as IC_REC_CCG,
 COALESCE(CCG_NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(STP_CODE, "UNKNOWN") as STP_CODE,
 COALESCE(STP_NAME, "UNKNOWN") as STP_NAME,
 COALESCE(IMD_DECILE, 'Not stated/Not known/Invalid') AS IMD_DECILE
 FROM
 MHA_LOS_PREP
 WHERE
 START_DATE > '1900-01-01'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los_quartiles

 SELECT 
 DISTINCT
 CONCAT(PERSON_ID, MHA_STARTDATE, MHA_ENDDATE) AS ID,
 *
 FROM 
 $db_output.mha_los_prep_final
 WHERE
 MHA_ENDDATE BETWEEN '$rp_startdate' and '$rp_enddate'
 ORDER BY 
 1,2

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_spell_number;
 CREATE TABLE IF NOT EXISTS $db_output.mha_spell_number

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 COUNT(DISTINCT B.UniqMHActEpisodeID) AS Episode_COUNT
 FROM 
 CONT_EPI A
 LEFT JOIN ALL_EPI B ON B.DAY_DATE BETWEEN A.START_DATE AND A.END_DATE AND A.PERSON_ID = B.PERSON_ID
 LEFT JOIN $db_output.restrictive_lookup C on B.LEGALSTATUSCODE = C.LEGAL_STATUS 
 GROUP BY 
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE

# COMMAND ----------

 %md 
 ### MHA + CTO LOS Prep

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW all_mha_cto AS 

 SELECT 
 DISTINCT
 PERSON_ID, 
 UNIQMHACTEPISODEID,
 LEGALSTATUSCODE,
 STARTDATEMHACTLEGALSTATUSCLASS,
 CASE
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND (InactTimeMHAPeriod IS NULL OR InactTimeMHAPeriod >= '$rp_enddate') 
   AND (DISCHDATEHOSPPROVSPELL IS NULL OR DISCHDATEHOSPPROVSPELL >= '$rp_enddate') THEN date_add('$rp_enddate', 1)
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND DISCHDATEHOSPPROVSPELL < InactTimeMHAPeriod THEN DISCHDATEHOSPPROVSPELL
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND InactTimeMHAPeriod IS NOT NULL THEN InactTimeMHAPeriod
   ELSE ENDDATEMHACTLEGALSTATUSCLASS
   END AS ENDDATEMHACTLEGALSTATUSCLASS,
 DISCHDATEHOSPPROVSPELL
 FROM
 $db_output.detentions
 WHERE
 (LEGALSTATUSCODE IS NOT NULL AND LEGALSTATUSCODE IN ('02','2','03','3','04','4','05','5','06','6','07','7','08','8','09','9','10','12','13','14','15','16','17','18','19','20','31','32','35','36','37','38'
 ))

 UNION ALL

 SELECT 
 PERSON_ID, 
 CTO_UNIQMHACTEPISODEID  AS UNIQMHAEPISODEID, 
 'CTO' AS LEGALSTATUSCODE,
 STARTDATECOMMTREATORD,
 CASE
   WHEN EndDateCommTreatOrd IS NULL AND (RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate')  THEN date_add('$rp_enddate', 1)
   WHEN EndDateCommTreatOrd IS NULL AND RecordEndDate IS NOT NULL THEN RecordEndDate
   ELSE EndDateCommTreatOrd
   END as EndDateCommTreatOrd,
 '' AS DISCHDATEHOSPPROVSPELL
 FROM
 $db_output.cto

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW all_epi_cto AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.UNIQMHACTEPISODEID,
 A.LEGALSTATUSCODE,
 B.DAY_DATE
 FROM
 all_mha_cto A
 LEFT JOIN $reference_data.CALENDAR B on B.DAY_DATE BETWEEN A.STARTDATEMHACTLEGALSTATUSCLASS AND A.EndDateMHActLegalStatusClass
 ORDER BY 1 

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW all_epi_1_cto AS 

 SELECT 
 DISTINCT
 PERSON_ID,
 DAY_DATE,
 RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC) AS RANK,
 DATE_ADD(DAY_DATE, -(RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC))) AS GROUP
 FROM
   (SELECT
   DISTINCT
   PERSON_ID,
   DAY_DATE
   FROM
   all_epi_cto
   --WHERE DAY_DATE IS NOT NULL
   ) A
 ORDER BY 1

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW cont_epi_cto AS

 SELECT 
 DISTINCT
 PERSON_ID,
 MIN(DAY_DATE) AS START_DATE,
 MAX(DAY_DATE) AS END_DATE
 FROM 
 all_epi_1_cto
 GROUP BY 
 PERSON_ID,
 GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW mha_severity_cto AS

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 MAX(CODE) AS mha_severity
 FROM 
 cont_epi_cto A
 LEFT JOIN all_epi_cto B ON B.DAY_DATE BETWEEN A.START_DATE AND A.END_DATE AND A.PERSON_ID = B.PERSON_ID
 LEFT JOIN $db_output.restrictive_lookup_mha_cto C on B.LEGALSTATUSCODE = C.LEGAL_STATUS 
 GROUP BY 
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW mha_los_cto AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 B.DESCRIPTION,
 B.CATEGORY,
 DATEDIFF(CASE
             WHEN A.END_DATE = '2100-01-01' THEN '$rp_enddate' ELSE A.END_DATE END
             ,A.START_DATE) AS mha_los
 FROM 
 mha_severity_cto A
 LEFT JOIN $db_output.restrictive_lookup_mha_cto B on A.mha_severity = B.CODE

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MHA_LOS_PREP_CTO AS 

 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 A.DESCRIPTION,
 A.CATEGORY,
 A.MHA_LOS,
 C.AgeRepPeriodEnd,
 C.EthnicCategory,
 C.Der_Gender,
 C.IC_REC_CCG,
 C.CCG_NAME,
 C.STP_CODE,
 C.STP_NAME,
 C.IMD_DECILE
 FROM 
 mha_los_cto A 
 LEFT JOIN SINGLE_MPI_RECORD_IN_YEAR C ON A.PERSON_ID = C.PERSON_ID AND C.PatMRecInRP = true ---get only latest submitted mpi record (in cases where same person_id seen by more than one provider in FY)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_cto_prep_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_cto_prep_final AS

 SELECT
 DISTINCT
 PERSON_ID,
 START_DATE AS MHA_STARTDATE,
 END_DATE AS MHA_ENDDATE,
 DESCRIPTION,
 CATEGORY,
 MHA_LOS,
 AgeRepPeriodEnd,
 CASE
   WHEN AgeRepPeriodEnd <= 15 THEN '15 and under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65  THEN '65 and over'
   ELSE 'UNKNOWN' 
   END AS Age,
 Der_Gender,
 ETHNICCATEGORY,
 CASE
   WHEN EthnicCategory IN ('A','B','C') THEN 'White'
   WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
   WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
   WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
   WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
   ELSE 'Unknown'
   END AS HIGHER_ETHNIC_CATEGORY,
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British'
     WHEN EthnicCategory = 'B' THEN 'White Irish'
     WHEN EthnicCategory = 'C' THEN 'Any Other White background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END AS ETHNIC_CATEGORY_DESCRIPTION,
 COALESCE(IC_REC_CCG, "UNKNOWN") as IC_REC_CCG,
 COALESCE(CCG_NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(STP_CODE, "UNKNOWN") as STP_CODE,
 COALESCE(STP_NAME, "UNKNOWN") as STP_NAME,
 COALESCE(IMD_DECILE, 'Not stated/Not known/Invalid') AS IMD_DECILE
 FROM
 MHA_LOS_PREP_CTO
 WHERE
 START_DATE > '1900-01-01'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los_quartiles_cto

 SELECT 
 DISTINCT
 CONCAT(PERSON_ID, MHA_STARTDATE, MHA_ENDDATE) AS ID,
 *
 FROM 
 $db_output.mha_los_cto_prep_final
 WHERE
 MHA_ENDDATE BETWEEN '$rp_startdate' and '$rp_enddate'
 ORDER BY 
 1,2

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_spell_number_cto;
 CREATE TABLE IF NOT EXISTS $db_output.mha_spell_number_cto
 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE,
 COUNT(DISTINCT B.UNIQMHACTEPISODEID) AS Episode_COUNT
 FROM 
 cont_epi_cto A
 LEFT JOIN all_epi_cto B ON B.DAY_DATE BETWEEN A.START_DATE AND A.END_DATE AND A.PERSON_ID = B.PERSON_ID
 LEFT JOIN $db_output.restrictive_lookup_mha_cto C on B.LEGALSTATUSCODE = C.LEGAL_STATUS 
 GROUP BY 
 A.PERSON_ID,
 A.START_DATE,
 A.END_DATE

# COMMAND ----------

 %md
 ### Table 1e data (Age x Ethnicity x Gender)

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cross_tab_pop_higher;
 CREATE TABLE IF NOT EXISTS $db_output.mha_cross_tab_pop_higher
 select 
 CASE
 	WHEN Age_group = '0 to 4' THEN '17 and under'
 	WHEN Age_group = '5 to 7' THEN '17 and under'
 	WHEN Age_group = '8 to 9' THEN '17 and under'
 	WHEN Age_group = '10 to 14' THEN '17 and under'
 	WHEN Age_group = '15' THEN '17 and under'
 	WHEN Age_group = '16 to 17' THEN '17 and under'
 	WHEN Age_group = '18 to 19' THEN '18 and over'
 	WHEN Age_group = '20 to 24' THEN '18 and over'
 	WHEN Age_group = '25 to 29' THEN '18 and over'
 	WHEN Age_group = '30 to 34' THEN '18 and over'
 	WHEN Age_group = '35 to 39' THEN '18 and over'
 	WHEN Age_group = '40 to 44' THEN '18 and over'
 	WHEN Age_group = '45 to 49' THEN '18 and over'
 	WHEN Age_group = '50 to 54' THEN '18 and over'
 	WHEN Age_group = '55 to 59' THEN '18 and over'
 	WHEN Age_group = '60 to 64' THEN '18 and over'
 	WHEN Age_group = '65 to 69' THEN '18 and over'
 	WHEN Age_group = '70 to 74' THEN '18 and over'
 	WHEN Age_group = '75 to 79' THEN '18 and over'
 	WHEN Age_group = '80 to 84' THEN '18 and over'
 	WHEN Age_group = '85 and over' THEN '18 and over'
 	end AS Age_higher,
 CASE     
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian background', 'Pakistani')
     THEN 'Asian or Asian British'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black background')
     THEN 'Black or Black British'
     WHEN ethnic_group_formatted IN ('Any Other Mixed background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab')
     THEN 'Other Ethnic Groups'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White background')
     THEN 'White'
 END as Ethnic6,  
 Der_Gender,
 sum(population) as population
 from $db_output.pop_health
 group by
 CASE
 	WHEN Age_group = '0 to 4' THEN '17 and under'
 	WHEN Age_group = '5 to 7' THEN '17 and under'
 	WHEN Age_group = '8 to 9' THEN '17 and under'
 	WHEN Age_group = '10 to 14' THEN '17 and under'
 	WHEN Age_group = '15' THEN '17 and under'
 	WHEN Age_group = '16 to 17' THEN '17 and under'
 	WHEN Age_group = '18 to 19' THEN '18 and over'
 	WHEN Age_group = '20 to 24' THEN '18 and over'
 	WHEN Age_group = '25 to 29' THEN '18 and over'
 	WHEN Age_group = '30 to 34' THEN '18 and over'
 	WHEN Age_group = '35 to 39' THEN '18 and over'
 	WHEN Age_group = '40 to 44' THEN '18 and over'
 	WHEN Age_group = '45 to 49' THEN '18 and over'
 	WHEN Age_group = '50 to 54' THEN '18 and over'
 	WHEN Age_group = '55 to 59' THEN '18 and over'
 	WHEN Age_group = '60 to 64' THEN '18 and over'
 	WHEN Age_group = '65 to 69' THEN '18 and over'
 	WHEN Age_group = '70 to 74' THEN '18 and over'
 	WHEN Age_group = '75 to 79' THEN '18 and over'
 	WHEN Age_group = '80 to 84' THEN '18 and over'
 	WHEN Age_group = '85 and over' THEN '18 and over'
 	end,
 CASE     
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian background', 'Pakistani')
     THEN 'Asian or Asian British'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black background')
     THEN 'Black or Black British'
     WHEN ethnic_group_formatted IN ('Any Other Mixed background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab')
     THEN 'Other Ethnic Groups'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White background')
     THEN 'White' END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cross_tab_pop;
 CREATE TABLE IF NOT EXISTS $db_output.mha_cross_tab_pop
 select 
 CASE
 	WHEN Age_group = '0 to 4' THEN '15 and under'
 	WHEN Age_group = '5 to 7' THEN '15 and under'
 	WHEN Age_group = '8 to 9' THEN '15 and under'
 	WHEN Age_group = '10 to 14' THEN '15 and under'
 	WHEN Age_group = '15' THEN '15 and under'
 	WHEN Age_group = '16 to 17' THEN '16 to 17'
 	WHEN Age_group = '18 to 19' THEN '18 to 34'
 	WHEN Age_group = '20 to 24' THEN '18 to 34'
 	WHEN Age_group = '25 to 29' THEN '18 to 34'
 	WHEN Age_group = '30 to 34' THEN '18 to 34'
 	WHEN Age_group = '35 to 39' THEN '35 to 49'
 	WHEN Age_group = '40 to 44' THEN '35 to 49'
 	WHEN Age_group = '45 to 49' THEN '35 to 49'
 	WHEN Age_group = '50 to 54' THEN '50 to 64'
 	WHEN Age_group = '55 to 59' THEN '50 to 64'
 	WHEN Age_group = '60 to 64' THEN '50 to 64'
 	WHEN Age_group = '65 to 69' THEN '65 and over'
 	WHEN Age_group = '70 to 74' THEN '65 and over'
 	WHEN Age_group = '75 to 79' THEN '65 and over'
 	WHEN Age_group = '80 to 84' THEN '65 and over'
 	WHEN Age_group = '85 and over' THEN '65 and over'
 	end AS Age,
 CASE     
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian background', 'Pakistani')
     THEN 'Asian or Asian British'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black background')
     THEN 'Black or Black British'
     WHEN ethnic_group_formatted IN ('Any Other Mixed background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab')
     THEN 'Other Ethnic Groups'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White background')
     THEN 'White'
 END as Ethnic6,  
 Der_Gender,
 sum(population) as population
 from $db_output.pop_health
 group by
 CASE
 	WHEN Age_group = '0 to 4' THEN '15 and under'
 	WHEN Age_group = '5 to 7' THEN '15 and under'
 	WHEN Age_group = '8 to 9' THEN '15 and under'
 	WHEN Age_group = '10 to 14' THEN '15 and under'
 	WHEN Age_group = '15' THEN '15 and under'
 	WHEN Age_group = '16 to 17' THEN '16 to 17'
 	WHEN Age_group = '18 to 19' THEN '18 to 34'
 	WHEN Age_group = '20 to 24' THEN '18 to 34'
 	WHEN Age_group = '25 to 29' THEN '18 to 34'
 	WHEN Age_group = '30 to 34' THEN '18 to 34'
 	WHEN Age_group = '35 to 39' THEN '35 to 49'
 	WHEN Age_group = '40 to 44' THEN '35 to 49'
 	WHEN Age_group = '45 to 49' THEN '35 to 49'
 	WHEN Age_group = '50 to 54' THEN '50 to 64'
 	WHEN Age_group = '55 to 59' THEN '50 to 64'
 	WHEN Age_group = '60 to 64' THEN '50 to 64'
 	WHEN Age_group = '65 to 69' THEN '65 and over'
 	WHEN Age_group = '70 to 74' THEN '65 and over'
 	WHEN Age_group = '75 to 79' THEN '65 and over'
 	WHEN Age_group = '80 to 84' THEN '65 and over'
 	WHEN Age_group = '85 and over' THEN '65 and over'
 	end,
 CASE     
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian background', 'Pakistani')
     THEN 'Asian or Asian British'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black background')
     THEN 'Black or Black British'
     WHEN ethnic_group_formatted IN ('Any Other Mixed background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab')
     THEN 'Other Ethnic Groups'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White background')
     THEN 'White' END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e AS
 SELECT
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
 WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
 WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
 WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
 WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
 END as Age,
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 END as Ethnic6,
 Der_Gender,
 COUNT(DISTINCT UniqMHActEpisodeID) AS DETENTION_COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 and EthnicCategory IS not null and EthnicCategory not in ('Z','99')
 and AgeRepPeriodEnd is not null
 GROUP BY 
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 15 then '15 and under'
 WHEN AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 WHEN AgeRepPeriodEnd between 18 and 34 then '18 to 34'
 WHEN AgeRepPeriodEnd between 35 and 49 then '35 to 49'
 WHEN AgeRepPeriodEnd between 50 and 64 then '50 to 64'
 WHEN AgeRepPeriodEnd >= 65 then '65 and over' 
 END,
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_higher;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_higher AS
 SELECT
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 17 then '17 and under'
 WHEN AgeRepPeriodEnd >= 18 then '18 and over' 
 END as Age_higher,
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 END as Ethnic6,
 Der_Gender,
 COUNT(DISTINCT UniqMHActEpisodeID) AS DETENTION_COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 and EthnicCategory IS not null and EthnicCategory not in ('Z','99')
 and AgeRepPeriodEnd is not null
 GROUP BY 
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 17 then '17 and under'
 WHEN AgeRepPeriodEnd >= 18 then '18 and over' 
 END,
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_rates AS
 SELECT
 c.Age,
 c.Ethnic6,
 c.Der_Gender,
 c.DETENTION_COUNT as Count,
 coalesce(p.population, 0) as Population
 from $db_output.mha_table_1e c
 left join $db_output.mha_cross_tab_pop p on c.Age = p.Age and c.Ethnic6 = p.Ethnic6 and c.Der_Gender = p.Der_Gender
 union
 SELECT
 c.Age_higher,
 c.Ethnic6,
 c.Der_Gender,
 c.DETENTION_COUNT as Count,
 coalesce(p.population, 0) as Population
 from $db_output.mha_table_1e_higher c
 left join $db_output.mha_cross_tab_pop_higher p on c.Age_higher = p.Age_higher and c.Ethnic6 = p.Ethnic6 and c.Der_Gender = p.Der_Gender
 order by c.Age, c.Ethnic6, c.Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_lookup AS
 select 
 a.Age,
 a.Ethnic6,
 b.Der_Gender
 from (select distinct Age, Ethnic6, 'a' as tag from $db_output.mha_table_1e_rates) a
 cross join (select distinct Der_Gender, 'a' as tag from $db_output.mha_table_1e_rates) b on a.tag = b.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_final_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_final_rates AS
 select
 l.Age,
 l.Ethnic6,
 CASE WHEN l.Der_Gender = "1" THEN "Male"
      WHEN l.Der_Gender = "2" THEN "Female"
      WHEN l.Der_Gender = "3" THEN "Non-binary"
      WHEN l.Der_Gender = "4" THEN "Other (not listed)"
      WHEN l.Der_Gender = "9" THEN "Indeterminate"
      ELSE "Unknown" END AS Der_Gender,
 coalesce(r.Count, 0) as Count,
 coalesce(r.Population, 0) as Population
 from $db_output.mha_table_1e_lookup l
 left join $db_output.mha_table_1e_rates r on l.Age = r.Age and l.Ethnic6 = r.Ethnic6 and l.Der_Gender = r.Der_Gender
 order by Age, Ethnic6, Der_Gender

# COMMAND ----------

 %md
 ### Table 1h data (IMD x Ethnicity)

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW IMD_TLEthnicity AS -- CA creating as temp view for purppose of merging into one table with the lower level ethnicity dat

 SELECT
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 ELSE 'Unknown'
 END as DemographicBreakdown
 ,CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END as IMD
 ,COUNT(DISTINCT UniqMHActEpisodeID) as COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 and (EthnicCategory is not null and EthnicCategory <> '-1') --Does this need updating?
 GROUP BY
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
 WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
 ELSE 'Unknown'
 END,
 CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW IMD_LLEthnicity AS -- CA creating as temp view for purpose of merging into one table with the top level ethnicity data

 SELECT
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British' -- added white to suit Table 1h structure
     WHEN EthnicCategory = 'B' THEN 'White Irish' -- added white to suit Table 1h structure
     WHEN EthnicCategory = 'C' THEN 'Any Other White background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END as DemographicBreakdown,
 CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END AS IMD
 ,COUNT(DISTINCT UniqMHActEpisodeID) AS COUNT
 FROM
 $db_output.detentions
 WHERE
 MHA_Logic_Cat_full in ('A','B','C','D','P')
 and (EthnicCategory is not null and EthnicCategory <> '-1') --Need updating?
 GROUP BY
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British'
     WHEN EthnicCategory = 'B' THEN 'White Irish'
     WHEN EthnicCategory = 'C' THEN 'Any Other White background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any Other Ethnic Group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END,
 CASE 
   WHEN IMD_Decile is not null then IMD_Decile
   ELSE 'Not stated/Not known/Invalid' END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1h;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1h AS
 select * from IMD_TLEthnicity
 union 
 select * from IMD_LLEthnicity
 order by 1,2