# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW MHS404CTO_Latest_Ranked AS
 ---get latest community treatment record for a person, provider and startdate of CTO
 SELECT *
 
 FROM
     (SELECT 
     *
     ,dense_rank() over (partition by Person_ID, orgidProv, StartDateCommTreatOrd ---for each person_id, provider and startdate of community treatment order
     order by RecordStartDate DESC, CASE WHEN EndDateCommTreatOrd is not null then 2 else 1 end DESC, EndDateCommTreatOrd DESC, MHS404UniqID DESC) 
     AS CTO_DUP_RANK ---Order by oldest RecordStartDate, want open CTOs first to take priority, EndDate CTO and RowID desc 
     FROM $db_output.mhs404_latest) a
 where CTO_DUP_RANK = 1 ---filter for latest person_id, provider and startdate of community treatment order only 
 order by Person_ID, StartDateCommTreatOrd

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Revoked_CTO AS
 ---join latest revoked community treatment orders to mha record
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
 
 FROM MHS404CTO_Latest_Ranked AS B
 LEFT JOIN $db_source.mhs401mhactperiod AS A ON B.UniqMHActEpisodeID = A.UniqMHActEpisodeID 
 AND (a.RecordEndDate is null or a.RecordEndDate >= '$rp_enddate') and a.RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mha records in financial year only could use $db_output.mhs401_latest instead?
 where B.CommTreatOrdEndReason = '02' ---filter for revoked community treatment orders only

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW MHS501_Ranked AS
 ---rank hospital spells in order per patient
 ---means that for each patient their hospital spells will be ranked into chronological order
 ---allows us to get the Previous Discharge Destination for a hospital spell which is used later
 SELECT
 *
 ,dense_rank() over (partition by Person_ID ---for each person_id
 order by StartDateHospProvSpell ASC, StartTimeHospProvSpell ASC, ---order by start date and time of hospital spells in chronological order
 Case when DischDateHospProvSpell is not null then 1 else 2 end asc, ---if discharge date is populated then prefer those hospital spells
 Case when DischDateHospProvSpell is not null then DischDateHospProvSpell end asc, ---if more than discharge date is populated then prefer the earliest discharge date
 DischTimeHospProvSpell ASC, ---if two hsopital spells were discharged on same day prefer the earliest time
 Case when DischDateHospProvSpell is null then 1 else 2 end asc,  ---if discharge date is populated then prefer those hospital spells
 UniqMonthID DESC ---latest submitted (same) hospital spell
 )
 AS HOSP_ADM_RANK
 
 FROM	
 (SELECT *,
 dense_rank() over (partition by Person_ID, orgidProv, StartDateHospProvSpell, DischDateHospProvSpell ---for each person_id, provider, startdate and discharge date of hospital spell combination
 order by UniqMonthID DESC, UniqHospProvSpellID DESC ---order by latest submitted hospital spell and hospital spell (for cases where a patient had more than one hopsital spell in month with same provider)
 ) AS HOSP_DUP_RANK
 FROM $db_output.mhs501_latest) AS A
 WHERE HOSP_DUP_RANK = 1 ---if same hospital spell is open for more than one month it will appear in multiple, this ensures we get the latest submitted (same) hospital spell
 ORDER BY 
 Person_ID, StartDateHospProvSpell, DischDateHospProvSpell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW HOSP_ADM AS
 ---for each hospital admission rank get the ranking before
 Select 
 distinct 
 HOSP_ADM_RANK,
 (HOSP_ADM_RANK - 1) AS PREV_HOSP_ADM_RANK
 FROM 
 MHS501_Ranked

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW MHS501HospSpell_Latest_Ranked AS
 ---gets a patients hospital spell and subsequent hospital spell
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
 FROM MHS501_Ranked a
 left join HOSP_ADM rnk on a.HOSP_ADM_RANK = rnk.HOSP_ADM_RANK
 left join MHS501_Ranked b on a.Person_ID = b.Person_ID and b.HOSP_ADM_RANK = rnk.PREV_HOSP_ADM_RANK

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_kp90;
 CREATE TABLE IF NOT EXISTS $db_output.mha_kp90 AS
 ---combines mha records in year with hospital spells ranked
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
 ,dense_rank() over (partition by a.Person_ID, A.orgidProv ,a.StartDateMHActLegalStatusClass, A.LegalStatusCode ---for each person_id, provider, startdate of legal status and legal status code
 order by 
 A.RecordStartDate DESC, ExpiryDateMHActLegalStatusClass DESC, EndDateMHActLegalStatusCLass DESC, A.UniqMHActEpisodeID DESC ---order by latest record, legal status class expiry date, legal status end date and episode_id
 ) AS MHA_RANK
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
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL' ---patient has no hospital spell if hospitalspell_id is null
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA' ---not applicable
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' ---patient may have hospital spell but may not be part of mha record
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell THEN 'DOA' ---detained on admission
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA' ---detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS' ---transfered on section
 	ELSE 'UNKNOWN' END AS Detention_Cat 
 ,CASE
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL' ---patient has no hospital spell if hospitalspell_id is null
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass ---patient may have hospital spell but may not be part of mha record
     or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' --patient may have hospital spell but may not be part of mha record 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA' 
     ---if start date and time of detention and hosp spell equal then detained on admission
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
     ---if start date of detention and hosp spell equal and time of detention is after hosp spell then detained subsequent to admission 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell
     and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
     ---if start date of detention and hosp spell equal and person_id, hosp spell combination is the earliest date in fyear then detained on admission    
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell 
     and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
     ---if start date of detention and hosp spell equal and person_id, hosp spell combination after the earliest date in fyear then detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
     ---if start date of detention is after hosp spell then detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
     ---if start date of detention is before hosp spell then transfered on section
 	ELSE 'UNKNOWN' END AS Detention_DateTime_Cat 
 
 FROM
 $db_output.mhs401_latest A
 LEFT JOIN MHS501HospSpell_Latest_Ranked B 
 ON A.Person_ID = B.Person_ID and A.orgidProv = B.orgidProv ---join mha record to hosp spell by person_id and provider code
 and CASE WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL' ---if no hosp spell records 
 WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass ---if start of hosp spell after end of detention
 or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass ---if start of hosp spell after expiry of detention
 or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) ---if start of detention on discharge date and time of detention after discharge time
 THEN 'NA' ---not applicable
 WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' ---if discharge of hosp spell before start of detention
 WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA' ---if start date and time of hosp spell and detention equal
 WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA' ---if start date equal and detention time after hosp spell
 WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA' ---detention after hosp spell
 WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS' ---detention before hosp spell
 ELSE 'UNKNOWN' END <> 'NA' ---mha records join to valid/applicable hospital spells only
 ORDER BY 
  A.Person_ID
 ,a.StartDateMHActLegalStatusClass
 ,MHA_RANK
 ,A.UniqMHActEpisodeID
 ,b.StartDateHospProvSpell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW KP90a AS
 ---filters table above so that only the latest episode id for each person_id, provider, startdate of legal status and legal status code is used
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
 ,dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID ---for each patient and hosp spell
 order by A.StartDateMHActLegalStatusClass ASC, A.StartTimeMHActLegalStatusClass ASC, ---order by earliest date and time of detention
 CASE when A.EndDateMHActLegalStatusClass is null then 1 else 2 end asc, ---prefer null end date of detention (i.e. still open) to populated end of detention
 A.EndDateMHActLegalStatusClass ASC, ---earliest end date of detention
 CASE when A.EndTimeMHActLegalStatusClass is null then 1 else 2 end asc, ---prefer null end time of detention (i.e. still open) to populated end of detention
 A.EndTimeMHActLegalStatusClass ASC ---earliest end time of detention
 ) AS HOSP_RANK
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
 	WHEN UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL' ---patient has no hospital spell if hospitalspell_id is null
 	WHEN StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA' ---not applicable
 	WHEN DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' ---patient may have hospital spell but may not be part of mha record
 	WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell THEN 'DOA' ---detained on admission
 	WHEN A.StartDateMHActLegalStatusClass > StartDateHospProvSpell THEN 'DSA' ---detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass < StartDateHospProvSpell THEN 'TOS' ---transfered on section
 	ELSE 'UNKNOWN' END AS Detention_Cat 
 ,CASE
 	WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL' ---patient has no hospital spell if hospitalspell_id is null
 	WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass ---patient may have hospital spell but may not be part of mha record
     or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
 	WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' --patient may have hospital spell but may not be part of mha record 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA' 
     ---if start date and time of detention and hosp spell equal then detained on admission
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
     ---if start date of detention and hosp spell equal and time of detention is after hosp spell then detained subsequent to admission 
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell
     and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
     ---if start date of detention and hosp spell equal and person_id, hosp spell combination is the earliest date in fyear then detained on admission    
 	WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell 
     and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
     ---if start date of detention and hosp spell equal and person_id, hosp spell combination after the earliest date in fyear then detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
     ---if start date of detention is after hosp spell then detained subsequent to admission
 	WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
     ---if start date of detention is before hosp spell then transfered on section
 	ELSE 'UNKNOWN' END AS Detention_DateTime_Cat 
 FROM
 $db_output.mha_kp90 A
 WHERE MHA_RANK = 1 ---filter for latest submitted person_id, provider, startdate of legal status and legal status code
 ORDER BY 
  A.Person_ID
 ,a.StartDateMHActLegalStatusClass
 ,MHA_RANK
 ,A.UniqMHActEpisodeID
 ,StartDateHospProvSpell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW HOSP_RANK AS 
 ---A table is created to link the curernt MHA episode to any exisiting previous one. This link is done using PersonID and Hospital Spell Number. 
 ---The current MHA episode is selected using = HOSP_RANK and the join uses = HOSP_RANK -1.
 SELECT 
 DISTINCT
 HOSP_RANK,
 (HOSP_RANK - 1) AS PREV_HOSP_RANK
 FROM KP90a

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW KP90_1 AS
 ---gets detention data with hosp spells in order
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
 LEFT JOIN HOSP_RANK rnk on a.HOSP_RANK = rnk.HOSP_RANK ---current hosp spell
 LEFT JOIN KP90a B ON A.Person_ID = B.Person_ID AND A.UniqHospProvSpellID = B.UniqHospProvSpellID AND B.HOSP_RANK = rnk.PREV_HOSP_RANK ---previous hosp spell

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW KP90_2 AS
 ---final data sheet. Some Organisations are hard coded as they have since expired in the year and as such dont get pulled through in the organisation reference data as of end of financial year
 ---MHA_Logic_Cat provides basic logic on how each MHA episode falls into the certain categories
 ---MHA_Logic_Cat_Full is the full logic and includes much more detail
 ---everything is included in the group by as some rows seemed to be coming through as complete duplicates
 
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
 DROP TABLE IF EXISTS $db_output.mha_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_final AS
 ---for the MHA_Final logic is calculated to work out how the MHA is being used. This field is MHA_Logic_Cat_full.
 
 ---Categories for MHA_Logic_Cat_full:
 ---A = Detentions on admission to hospital
 ---B = Detentions subsequent to admission
 ---C = Detentions following Place of Safety Order
 ---D = Detentions following revocation of CTO or Conditional Discharge
 ---E = Place of Safety Order
 ---F = Other short term holding order
 ---G = Renewal
 ---H = Transfer on Section
 ---J = 5(2) subsequent to 5(4)
 ---K = 37 subsequent to 35
 ---L = 3 subsequent to 2
 ---M = Guardianship
 ---P = Criminal Justice admissions
 ---N = Inconsistent value
 
 SELECT 
 a.*
 ,CASE 
 	WHEN LegalStatusCode = '01' ---Informal detentions on current detention are not counted
     THEN NULL
 	WHEN LegalStatusCode in ('02','03') ---Section 2 or Section 3 formal detention
       and StartDateMHActLegalStatusClass = StartDateHospProvSpell ---same start date for detention and hosp spell
       and (StartTimeMHActLegalStatusClass <= StartTimeHospProvSpell or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null) ---hosp spell time after detention
       and (
         (PrevLegalStatus is null or PrevLegalStatus = '01') ---no previous detention or previous detention was informal
         or (DATEDIFF(StartDateHospProvSpell,PrevMHAEndDate) > 1) ---start of hosp spell more than 1 date of previous detention end date
       ) 
     THEN 'A' ---Detention on admission to hospital
 	WHEN LegalStatusCode = '02' ---Section 2 fomal detention
       AND StartDateHospProvSpell between DATE_ADD(StartDateMHActLegalStatusClass,-5) and StartDateMHActLegalStatusClass ---start of hosp spell between (start of detention -5 days) and start of detention
       and MethAdmMHHospProvSpell = '2A' ---emergency admission requested by general practitioner
     THEN 'A' ---Detention on admission to hospital
 	WHEN LegalStatusCode in ('02','03') ---Section 2 or Section 3 formal detention
       and (
         (
           StartDateHospProvSpell = StartDateMHActLegalStatusClass 
           and (StartTimeHospProvSpell < StartTimeMHActLegalStatusClass) ---start date of detention and hosp spell same and detention time after hosp spell
           or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null ---or start date or time not populated
         ) 
         or StartDateMHActLegalStatusClass > StartDateHospProvSpell ---or detention start date after start date of hosp spell
       ) 
       and (
         PrevLegalStatus is null or PrevMHAEndDate < StartDateMHActLegalStatusClass ---no previous detention or previous detention before start of current detention
         and (
           MHS404UniqID is null ---no community treatment order record
           or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))---community treatement order end date is populated and before (start date of detention -1 day)
         )   
         or (
         PrevLegalStatus in ('04','05','06') ---Section 4, Section 5(2) or Section 5(4) formal detention
         and (
           PrevMHAEndDate = StartDateMHActLegalStatusClass ---previous detention end date and start date of current detention same day
           or PrevMHAEndDate = DATE_ADD(StartDateMHActLegalStatusClass,-1)) ---previous detention end date equal to day before start date of current detention
         ) 
       ) 
       and (
         MHS404UniqID is null ---no community treatment order record
         or (
           EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1) ---community treatement order end date is populated and before (start date of detention -1 day)
         ) 
       ) 
     THEN 'B' ---Detentions subsequent to admission
 	WHEN LegalStatusCode in ('02','03') ---Section 2 or Section 3 formal detention
       and (
         (StartDateMHActLegalStatusClass = PrevMHAEndDate) ---previous detention ended same day as start of current detention 
         or (
           StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) ---start of current detention one day before end of previous detention 
           or (StartDateMHActLegalStatusClass = PrevMHAStartDate) ---start of current detention same day as start date of previous detention
         )
       )
       and PrevLegalStatus in ('19','20') ---Section 135 or Section 136 formal detention
       and (
         (StartDateHospProvSpell = StartDateMHActLegalStatusClass) ---start date of current detention and hosp spell same day
         or (StartDateHospProvSpell >= PrevMHAStartDate) ---hosp spell start date after start of previous detention 
         or (StartDateMHActLegalStatusClass = PrevMHAEndDate) ---start date of current detention equal to end date of previous detention
       ) 
     THEN 'C' ---Detentions following Place of Safety Order
 	WHEN LegalStatusCode in ('03','09','10','15','16') ---Section 3, Section 37(S41 restrictions), Section 37, Section 47(S49 restrictions) or Section 47 formal detention
       and (
         (
           StartDateMHActLegalStatusClass >= DATE_ADD(EndDateCommTreatOrd,-1) ---start of detention after or equal to a day before end of community treatment oder 
           or EndDateCommTreatOrd is null ---no community treatement order
         ) 
         and CommTreatOrdEndReason = '02' ---community treatment order revoked
       ) 
       and (
         StartDateHospProvSpell = StartDateMHActLegalStatusClass ---start of detention and hosp spell same day 
         or StartDateHospProvSpell >= StartDateCommTreatOrd ---start of hosp spell on or after start of community treatment order
       )
       or (
         LegalStatusCode = '03' and CommTreatOrdEndReason = '02' ---Section 3 formal detention and community treatment order revoked
         and StartDateMHActLegalStatusClass < StartDateHospProvSpell ---start of hosp spell after start of detention
         and EndDateCommTreatOrd between DATE_ADD(StartDateHospProvSpell,-1) and DATE_ADD(StartDateHospProvSpell,2) --end of community treatment order between day before hosp spell and 2 days after start of hosp spell       
       )  
     THEN 'D' ---Detentions following revocation of CTO or Conditional Discharge
 	WHEN LegalStatusCode in ('19','20') ---Section 135 or Section 136 formal detention
       and (
         PrevMHAStartDate is null or PrevLegalStatus = '01' ---no previous detention or previous detention was informal
         or PrevMHAEndDate < StartDateMHActLegalStatusClass ---previous detention before start of current detention
       ) 
     THEN 'E' ---Place of Safety Order
 	WHEN (
         LegalStatusCode in ('04','05','06') ---Section 4, Section 5(2) or Section 5(4)
         and StartDateMHActLegalStatusClass = StartDateHospProvSpell ---start of detention and hosp spell same day
       ) 
       or (
         LegalStatusCode in ('05','06') ---Section 5(2) or Section 5(4) formal detention
         and StartDateMHActLegalStatusClass >= StartDateHospProvSpell ---start of detention after or on same day as start of hosp spell
       ) 
       and (
         (
           PrevLegalStatus  is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass ---no previous detention or end of previous detention before or on start of current detention
         ) 
         or PrevLegalStatus = '01' ---informal detention 
         or (
           PrevLegalStatus in ('02','03') ---Section 2 or Section 3 formal detention
           and PrevMHAEndDate = StartDateMHActLegalStatusClass ---previous detention end same day as start of current detention
         )
       ) 
     THEN 'F' ---Other short term holding order
 	WHEN LegalStatusCode IN ('03','07','08','09','10','12','15','16','17','18') 
     ---Section 3, Section 35, Section 36, Section 37(S41 restrictions), Section 37, Section 38, Section 47(S49 restrictions), Section 47, Section 48(S49 restrictions) or Section 48 formal detention
       AND StartDateMHActLegalStatusClass > StartDateHospProvSpell ---start of detention after start of hosp spell
       AND LegalStatusCode = PrevLegalStatus ---previous and current detention legal status code same
       and (
         (
           StartDateMHActLegalStatusClass BETWEEN PrevMHAEndDate AND DATE_ADD(PrevMHAEndDate,1) ---start of detention between previous detention end and a day
           or PrevMHAEndDate is null ---no previous detention
         )
       ) 
     THEN 'G' ---Renewal
 	WHEN LegalStatusCode IN ('02','03','07','08','09','10','12','13','14','15','16','17','18','31','32','34') ---34 LegalStatusCode no longer valid for MHSDS v5
     ---Section 2, Section 3, Section 35, Section 36, Section 37(S41 restrictions), Section 37, Section 38, Section 33, Section 46, Section 47, Section 48(S49 restrictions), Section 48, Criminal insanity act, detained under other acts formal detention 
       and StartDateHospProvSpell > StartDateMHActLegalStatusClass ---start of hosp spell after start of detention
       and (
         MethAdmMHHospProvSpell in ('81','2B','11','12','13') ---method of hospital admission in other, emergency or elective admission (waiting list, booked or planned)
         or PrevDestOfDischHospProvSpell in ('49','51','50','52','53','87') 
         ---discharge destination of previous hospital spell to other nhs provider (medium or high security, general, maternity ward or learnining disability ward) or independent healthcare provider
       ) 
     THEN 'H' ---Transfer on Section
 	WHEN LegalStatusCode = '05' ---Section 5(2) formal detention
       and StartDateMHActLegalStatusClass >= StartDateHospProvSpell ---start of detention after or on start of hosp spell 
       and PrevLegalStatus = '06' ---previous detention was Section 5(4)
       and (
         StartDateMHActLegalStatusClass >= PrevMHAStartDate ---start of detention after or on start date of previous detention
         and (
           PrevMHAEndDate is null or PrevMHAEndDate <= EndDateMHActLegalStatusClass ---no previous detention or end date of previous detention before or on end date of current detention
           or EndDateMHActLegalStatusClass is null ---end date of detention is not populated
         )
       ) 
     THEN 'J' ---Section 5(2) subsequent to Section 5(4)
 	WHEN LegalStatusCode = '10' ---Section 37 formal detention
       and StartDateMHActLegalStatusClass >= StartDateHospProvSpell ---start date of detention after or on start of hosp spell
       and PrevLegalStatus = '07' ---previous detention was Section 35
       and (
         PrevMHAEndDate is null ---no previous detention
         or PrevMHAEndDate = StartDateMHActLegalStatusClass ---end of previous detention same day as start of current detention
         or StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) ---start of current detention same day as (end date of previous detention plus 1 day)
       ) 
     THEN 'K' ---Section 37 subsequent to Section 35
 	WHEN LegalStatusCode = '03' ---Section 3 formal detention
       and StartDateMHActLegalStatusClass >= StartDateHospProvSpell ---start of detention after or on start of hosp spell
       and PrevLegalStatus = '02' ---previous detention was Section 2
       and (
         PrevMHAEndDate is null ---no previous detention
         or PrevMHAEndDate = StartDateMHActLegalStatusClass ---end of previous detention same day as start of current detention
         or StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) ---start of detetnion same day as (end date of previous detention plus 1 day) 
       ) 
     THEN 'L' ---Section 3 subsequent to Section 2
 	WHEN LegalStatusCode in ('35','36') ---subject to guardianship Section 7 or subject to guardianship Section 37
     THEN 'M' ---Guardianship
 	WHEN LegalStatusCode in ('07','08','09','10','12','13','14','15','16','17','18','31','32','34') ---34 LegalStatusCode no longer valid for MHSDS v5
     ---Section 35, Section 36, Section 37(S41 restrictions), Section 37, Section 38, Section 44, Section 46, Section 47(S49 restrictions), Section 47, Section 48(S49 restrictions), Section 48, Criminal insanity act, detained under other acts formal detention 
       and StartDateMHActLegalStatusClass <= StartDateHospProvSpell ---start of detention before or on start of hosp spell
       and (EndDateMHActLegalStatusClass is null or EndDateMHActLegalStatusClass >= StartDateHospProvSpell) ---end of detention not populated or end of detention after or on start of hosp spell
       and MethAdmMHHospProvSpell in ('11','12','13') ---method of admission elective admission (waiting list, booked or planned)
       and (
         PrevLegalStatus is null ---no previous detention
         or (
           PrevLegalStatus in ('01','02','03','04','05','06') ---previous detention was informal, Section 2, Section 3, Section 4, Section 5(2) or Section 5(4) formal detention
           and (PrevMHAEndDate is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass) ---no previous detention end date or end of previous detention before or on start of current detention
         )
       ) 
       and (
         MHS404UniqID is null ---no community treatment order record
         or (
           EndDateCommTreatOrd is not null ---end of ommunity treatment order populated
           and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1) ---end of community treatment order before day before start of detention
         )
       ) 
     THEN 'P' ---Criminal Justice admission
 	ELSE 'N' ---Inconsistent value
 	END as MHA_Logic_Cat_full
 ,d.AgeRepPeriodEnd
 ,d.EthnicCategory
 ,d.Der_Gender ---New for MHA 2021/22
 ,D.IC_REC_CCG
 ,D.CCG_NAME  -- CA changed NAME to CCG_NAME to match with the detention column name as in MHA_unformatted_table notebook
 ,D.STP_CODE
 ,D.STP_NAME
 ,D.IMD_DECILE
 from KP90_2 a 
 left join $db_output.mhs001_latest d on a.Person_ID = d.Person_ID and a.orgidProv = d.orgidProv ---join mpi data to get demographic information
 ORDER BY 
  a.Person_ID
 ,StartDateMHActLegalStatusClass
 ,StartDateHospProvSpell
 ,HOSP_RANK

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cto_final;
 CREATE TABLE IF NOT EXISTS $db_output.cto_final AS 
 ---gets all data for community treatment orders and joins to mpi table to get demographic data
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
 ---inserts mha data into final detention prep table for aggregation
 ---only a single hospital and earliest spell is used for each UniqMHActEpisodeID
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
 			StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate' ---start of detention in financial year only
 			GROUP BY
 			UniqMHActEpisodeID) AS B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START
 WHERE
 StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate' ---start of detention in financial year only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.short_term_orders
 ---inserts mha data into final short term orders prep table for aggregation
 SELECT 
 *
 FROM 
 $db_output.mha_final
 WHERE
 StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate' ---start of detention in financial year only

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cto
 ---inserts cto data into final community treatment orders prep table for aggregation
 SELECT 
 *
 FROM 
 $db_output.cto_final a
 where 
 StartDateCommTreatOrd between '$rp_startdate' and '$rp_enddate' ---start of community treatment order in financial year only

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW ALL_MHA AS 
 ---get distinct record for each person_id, detention, legalstatus, start and end date of detention and discharge date
 SELECT 
 DISTINCT
 PERSON_ID, 
 UNIQMHACTEPISODEID,
 LEGALSTATUSCODE,
 STARTDATEMHACTLEGALSTATUSCLASS,
 CASE
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND (InactTimeMHAPeriod IS NULL OR InactTimeMHAPeriod >= '$rp_enddate') 
   AND (DISCHDATEHOSPPROVSPELL IS NULL OR DISCHDATEHOSPPROVSPELL >= '$rp_enddate') THEN date_add('$rp_enddate', 1)
   ---if no detention end date and date detention would become inactive if it didn't flow again is not populated or after financial year and discharge date is not populated or after financial year then use financial year end plus 1 day
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND DISCHDATEHOSPPROVSPELL < InactTimeMHAPeriod THEN DISCHDATEHOSPPROVSPELL
   ---if no detention end date and discharge date before date detention would become inactive if it didn't flow again then use discharge date
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND InactTimeMHAPeriod IS NOT NULL THEN InactTimeMHAPeriod
   ---if no detention end date and date detention would become inactive if it didn't flow again is populated then use that
   ELSE ENDDATEMHACTLEGALSTATUSCLASS
   ---otherwise use end date of detention
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
 ---gets a single row for each day that a single person_id, detention, legalstatus was active i.e. if a detention lasted 3 days there would be 3 rows for each of those days
 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.UniqMHActEpisodeID,
 A.LEGALSTATUSCODE,
 B.DAY_DATE
 FROM
 ALL_MHA A
 LEFT JOIN $ref_database.CALENDAR B on B.DAY_DATE BETWEEN A.STARTDATEMHACTLEGALSTATUSCLASS AND A.EndDateMHActLegalStatusClass
 ORDER BY 1

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW ALL_EPI_1 AS 
 ---for each person_id and each day detention was active get rank of each day and day before the first day date for that person_id
 SELECT 
 DISTINCT
 PERSON_ID,
 DAY_DATE,
 RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC) AS RANK,
 DATE_ADD(DAY_DATE, -(RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC))) AS GROUP ---day date minus it's rank for each day date (gets day before first day date for person_id)
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
 ---gets the first and last day date for each person_id
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
 ---joins data in table above to restrictive_lookup table created in All Tables
 ---looks at legalstatuscode of detention and gets the mha_severity code associated 
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
 GROUP BY A.PERSON_ID, A.START_DATE, A.END_DATE

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MHA_LOS AS 
 ---gets the mha detention category and description for each person_id and calculates difference in days between start and end date of detention
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
 ---joins mha length of stay data to the mpi table to get demographic and geographical details for each person_id
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
 LEFT JOIN $db_output.mhs001_latest C ON A.PERSON_ID = C.PERSON_ID AND C.PatMRecInRP = true ---get only latest submitted mpi record (in cases where same person_id seen by more than one provider in FY)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_prep_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_prep_final AS
 ---final prep table for mha length of stay, creates derived demographic fields to desired groupings
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
     WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END AS ETHNIC_CATEGORY_DESCRIPTION,
 IC_REC_CCG,
 CCG_NAME,
 STP_CODE,
 STP_NAME,
 COALESCE(IMD_DECILE, 'Not stated/Not known/Invalid') AS IMD_DECILE
 FROM
 MHA_LOS_PREP
 WHERE
 START_DATE > '1900-01-01'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los_quartiles
 ---inserts data into $db_output.mha_los_quartiles tables while creating a derived ID for each person_id, mha_startdate, mha_enddate combination
 SELECT 
 DISTINCT
 CONCAT(PERSON_ID, MHA_STARTDATE, MHA_ENDDATE) AS ID,
 *
 FROM 
 $db_output.mha_los_prep_final
 WHERE
 MHA_ENDDATE BETWEEN '$rp_startdate' and '$rp_enddate' ---end of detention had to occur in financial year
 ORDER BY 
 1,2

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_spell_number;
 CREATE TABLE IF NOT EXISTS $db_output.mha_spell_number
 ---for each person_id, start date and end date of detention get count of detentions
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

 %sql
 CREATE OR REPLACE TEMP VIEW all_mha_cto AS 
 ---get distinct record for each person_id, detention, legalstatus, start and end date of detention and discharge date plus community treatment order data
 SELECT 
 DISTINCT
 PERSON_ID, 
 UNIQMHACTEPISODEID,
 LEGALSTATUSCODE,
 STARTDATEMHACTLEGALSTATUSCLASS,
 CASE
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND (InactTimeMHAPeriod IS NULL OR InactTimeMHAPeriod >= '$rp_enddate') 
   AND (DISCHDATEHOSPPROVSPELL IS NULL OR DISCHDATEHOSPPROVSPELL >= '$rp_enddate') THEN date_add('$rp_enddate', 1)
   ---if no detention end date and date detention would become inactive if it didn't flow again is not populated or after financial year and discharge date is not populated or after financial year then use financial year end plus 1 day
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND DISCHDATEHOSPPROVSPELL < InactTimeMHAPeriod THEN DISCHDATEHOSPPROVSPELL
   ---if no detention end date and discharge date before date detention would become inactive if it didn't flow again then use discharge date
   WHEN ENDDATEMHACTLEGALSTATUSCLASS IS NULL AND InactTimeMHAPeriod IS NOT NULL THEN InactTimeMHAPeriod
   ---if no detention end date and date detention would become inactive if it didn't flow again is populated then use that
   ELSE ENDDATEMHACTLEGALSTATUSCLASS
   ---otherwise use end date of detention
   END AS ENDDATEMHACTLEGALSTATUSCLASS,
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
   WHEN EndDateCommTreatOrd IS NULL AND (RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') THEN date_add('$rp_enddate', 1)
   ---if no end of community treatment order and no record end date or after finanical year then use financial year end plus 1 day
   WHEN EndDateCommTreatOrd IS NULL AND RecordEndDate IS NOT NULL THEN RecordEndDate
    ---if no end of community treatment order and record end date is populated then use that
   ELSE EndDateCommTreatOrd
   ---otherwise use end date of community treatment order
   END as EndDateCommTreatOrd,
 '' AS DISCHDATEHOSPPROVSPELL
 FROM
 $db_output.cto

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW all_epi_cto AS 
 ---gets a single row for each day that a single person_id, detention, legalstatus was active i.e. if a detention/cto lasted 3 days there would be 3 rows for each of those days
 SELECT 
 DISTINCT
 A.PERSON_ID,
 A.UNIQMHACTEPISODEID,
 A.LEGALSTATUSCODE,
 B.DAY_DATE
 FROM
 all_mha_cto A
 LEFT JOIN $ref_database.CALENDAR B on B.DAY_DATE BETWEEN A.STARTDATEMHACTLEGALSTATUSCLASS AND A.EndDateMHActLegalStatusClass
 ORDER BY 1 

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW all_epi_1_cto AS 
 ---for each person_id and each day detention was active get rank of each day and day before the first day date for that person_id
 SELECT 
 DISTINCT
 PERSON_ID,
 DAY_DATE,
 RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC) AS RANK,
 DATE_ADD(DAY_DATE, -(RANK() OVER (PARTITION BY PERSON_ID ORDER BY DAY_DATE ASC))) AS GROUP ---day date minus it's rank for each day date (gets day before first day date for person_id)
 FROM
   (SELECT
   DISTINCT
   PERSON_ID,
   DAY_DATE
   FROM
   all_epi_cto
   ) A
 ORDER BY 1

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW cont_epi_cto AS
 ---gets the first and last day date for each person_id
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
 ---joins data in table above to restrictive_lookup_mha_cto table created in All Tables
 ---looks at legalstatuscode of detention and gets the mha_severity code associated 
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
 ---gets the mha detention category and description for each person_id and calculates difference in days between start and end date of detention
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
 ---joins mha length of stay data to the mpi table to get demographic and geographical details for each person_id
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
 LEFT JOIN $db_output.mhs001_latest C ON A.PERSON_ID = C.PERSON_ID AND C.PatMRecInRP = true ---get only latest submitted mpi record (in cases where same person_id seen by more than one provider in FY)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_los_cto_prep_final;
 CREATE TABLE IF NOT EXISTS $db_output.mha_los_cto_prep_final AS
 ---final prep table for mha length of stay, creates derived demographic fields to desired groupings
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
     WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
     WHEN EthnicCategory = 'Z' THEN 'Not Stated'
     WHEN EthnicCategory = '99' THEN 'Not Known'
     ELSE 'Unknown'
     END AS ETHNIC_CATEGORY_DESCRIPTION,
 IC_REC_CCG,
 CCG_NAME,
 STP_CODE,
 STP_NAME,
 COALESCE(IMD_DECILE, 'Not stated/Not known/Invalid') AS IMD_DECILE
 FROM
 MHA_LOS_PREP_CTO
 WHERE
 START_DATE > '1900-01-01'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.mha_los_quartiles_cto
 ---inserts data into $db_output.mha_los_quartiles tables while creating a derived ID for each person_id, mha_startdate, mha_enddate combination
 SELECT 
 DISTINCT
 CONCAT(PERSON_ID, MHA_STARTDATE, MHA_ENDDATE) AS ID,
 *
 FROM 
 $db_output.mha_los_cto_prep_final
 WHERE
 MHA_ENDDATE BETWEEN '$rp_startdate' and '$rp_enddate' ---end of detention/cto had to occur in financial year
 ORDER BY 
 1,2

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_spell_number_cto;
 CREATE TABLE IF NOT EXISTS $db_output.mha_spell_number_cto
 ---for each person_id, start date and end date of detention get count of detentions/cto
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

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cross_tab_pop_higher;
 CREATE TABLE IF NOT EXISTS $db_output.mha_cross_tab_pop_higher 
 ---count of population grouped by age (higher level), ethnicity and gender
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
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani')
     THEN 'Asian'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background')
     THEN 'Black'
     WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any other ethnic group', 'Arab')
     THEN 'Other'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background')
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
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani')
     THEN 'Asian'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background')
     THEN 'Black'
     WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any other ethnic group', 'Arab')
     THEN 'Other'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background')
     THEN 'White' END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_cross_tab_pop;
 CREATE TABLE IF NOT EXISTS $db_output.mha_cross_tab_pop
 ---count of population grouped by age (lower level), ethnicity and gender
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
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani')
     THEN 'Asian'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background')
     THEN 'Black'
     WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any other ethnic group', 'Arab')
     THEN 'Other'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background')
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
     WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani')
     THEN 'Asian'
     WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background')
     THEN 'Black'
     WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean')
     THEN 'Mixed'
     WHEN ethnic_group_formatted IN ('Chinese', 'Any other ethnic group', 'Arab')
     THEN 'Other'
     WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background')
     THEN 'White' END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e AS
 ---count of detentions grouped by age (lower level), ethnicity and gender
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
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black'
 WHEN EthnicCategory IN ('R','S') THEN 'Other'
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
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black'
 WHEN EthnicCategory IN ('R','S') THEN 'Other'
 END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_higher;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_higher AS
 ---count of detentions grouped by age (higher level), ethnicity and gender
 SELECT
 CASE 
 WHEN AgeRepPeriodEnd between 0 and 17 then '17 and under'
 WHEN AgeRepPeriodEnd >= 18 then '18 and over' 
 END as Age_higher,
 CASE
 WHEN EthnicCategory IN ('A','B','C') THEN 'White'
 WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black'
 WHEN EthnicCategory IN ('R','S') THEN 'Other'
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
 WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian'
 WHEN EthnicCategory IN ('M','N','P') THEN 'Black'
 WHEN EthnicCategory IN ('R','S') THEN 'Other'
 END,
 Der_Gender

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_table_1e_rates;
 CREATE TABLE IF NOT EXISTS $db_output.mha_table_1e_rates AS
 ---this joins detention and population count for each of the demographics and unions for age lower and age higher breakdowns 
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
 ---this creates a lookup tables so that every possible combination of age, ethnicity and gender is in the final tables
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
 ---this creates the final prep table used for aggregation needed for Table 1e figures in Excel sheet. 
 select
 l.Age,
 l.Ethnic6,
 CASE WHEN l.Der_Gender = "1" THEN "Male"
      WHEN l.Der_Gender = "2" THEN "Female"
      WHEN l.Der_Gender = "3" THEN "Non-binary"
      WHEN l.Der_Gender = "4" THEN "Other (not listed)"
      WHEN l.Der_Gender = "9" THEN "Indeterminate"
      ELSE "Unknown" END AS Der_Gender,
 coalesce(r.Count, 0) as Count, ---if age, ethnicity, gender combination doesn't exists coalesce to 0
 coalesce(r.Population, 0) as Population ---if age, ethnicity, gender combination doesn't exists coalesce to 0
 from $db_output.mha_table_1e_lookup l
 left join $db_output.mha_table_1e_rates r on l.Age = r.Age and l.Ethnic6 = r.Ethnic6 and l.Der_Gender = r.Der_Gender
 order by Age, Ethnic6, Der_Gender

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW IMD_TLEthnicity AS 
 ---count of detentions grouped by depriavtion and upper ethnicity
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
 and (EthnicCategory is not null and EthnicCategory <> '-1')
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
 CREATE OR REPLACE TEMPORARY VIEW IMD_LLEthnicity AS 
 ---count of detentions grouped by depriavtion and lower ethnicity
 SELECT
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British' -- added white to suit Table 1h structure
     WHEN EthnicCategory = 'B' THEN 'White Irish' -- added white to suit Table 1h structure
     WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
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
 and (EthnicCategory is not null and EthnicCategory <> '-1')
 GROUP BY
 CASE 
     WHEN EthnicCategory = 'A' THEN 'White British'
     WHEN EthnicCategory = 'B' THEN 'White Irish'
     WHEN EthnicCategory = 'C' THEN 'Any Other White Background'
     WHEN EthnicCategory = 'D' THEN 'White and Black Caribbean' 
     WHEN EthnicCategory = 'E' THEN 'White and Black African'
     WHEN EthnicCategory = 'F' THEN 'White and Asian'
     WHEN EthnicCategory = 'G' THEN 'Any Other Mixed Background'
     WHEN EthnicCategory = 'H' THEN 'Indian'
     WHEN EthnicCategory = 'J' THEN 'Pakistani'
     WHEN EthnicCategory = 'K' THEN 'Bangladeshi'
     WHEN EthnicCategory = 'L' THEN 'Any Other Asian Background'
     WHEN EthnicCategory = 'M' THEN 'Caribbean'
     WHEN EthnicCategory = 'N' THEN 'African'
     WHEN EthnicCategory = 'P' THEN 'Any Other Black Background'
     WHEN EthnicCategory = 'R' THEN 'Chinese'
     WHEN EthnicCategory = 'S' THEN 'Any other ethnic group'
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
 ----unions two tables above to combine upper and lower ethncity breakdowns
 select * from IMD_TLEthnicity
 union 
 select * from IMD_LLEthnicity
 order by 1,2