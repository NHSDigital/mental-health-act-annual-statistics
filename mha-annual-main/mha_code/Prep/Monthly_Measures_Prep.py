# Databricks notebook source
 %md
 ### MHA Monthly Measures Prep

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS001MPI_latest_month_data AS
     SELECT MPI.*
            ,CCG.IC_Rec_CCG
           ,CCG.NAME
       FROM $db_source.MHS001MPI MPI

  LEFT JOIN global_temp.CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = '$month_id'
            AND MPI.PatMRecInRP = true 

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS101Referral_open_end_rp AS
     SELECT ref.*
       FROM $db_source.MHS101Referral AS ref
      WHERE ref.UniqMonthID = '$month_id'
            AND (ref.ServDischDate IS NULL OR ref.ServDischDate > '$rp_enddate')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS401MHActPeriod_STO_open_end_rp AS
     SELECT	*
       FROM	$db_source.MHS401MHActPeriod AS STO
      WHERE  STO.UniqMonthID = '$month_id' 
             AND STO.legalstatuscode IN ('04', '05', '06', '19', '20')
      	    AND (STO.EndDateMHActLegalStatusClass IS NULL OR STO.EndDateMHActLegalStatusClass > '$rp_enddate')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS11_INTERMEDIATE AS
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
  --           ,AMHServiceRefEndRP
  --           ,CYPServiceRefEndRP
  --           ,LDAServiceRefEndRP
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM	global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON REF.Person_ID = STO.Person_ID
 INNER JOIN  global_temp.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID

# COMMAND ----------

 %sql
 /**MHS10 - PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE AT END OF REPORTING PERIOD - INTERMEDIATE**/
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS10_INTERMEDIATE AS
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 --            ,AMHServiceRefEndRP                              --v6_changes
 --           ,CYPServiceRefEndRP
 --            ,LDAServiceRefEndRP
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM  global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND MHA.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.UniqMHActEpisodeID = CTO.UniqMHActEpisodeID 
             AND CTO.UniqMonthID = '$month_id' 
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 		    ON MHA.UniqMHActEpisodeID = CTOR.UniqMHActEpisodeID 
             AND CTOR.UniqMonthID = '$month_id'  
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.UniqMHActEpisodeID = CD.UniqMHActEpisodeID 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber
 INNER JOIN  global_temp.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NOT NULL OR CTOR.Person_ID IS NOT NULL OR CD.UniqMHActEpisodeID IS NOT NULL)
 			AND STO.RecordNumber IS NULL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS501HospProvSpell_open_end_rp AS
     SELECT HSP.*
       FROM $db_source.MHS501HospProvSpell HSP
      WHERE HSP.UniqMonthID = '$month_id'
            AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell > '$rp_enddate') 

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_open_end_rp AS
     SELECT WRD.*
       FROM $db_source.MHS502WardStay WRD
      WHERE WRD.UniqMonthID = '$month_id' 
 		   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay > '$rp_enddate')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs09_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs09_prep
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 --            ,AMHServiceWSEndRP
 --            ,CYPServiceWSEndRP
 --            ,LDAServiceWSEndRP            
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM	global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND MHA.UniqMonthID = '$month_id' 
             AND MHA.legalstatuscode IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38')
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.RecordNumber = CTO.RecordNumber 
             AND CTO.UniqMonthID = '$month_id'  
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.RecordNumber = CTOR.RecordNumber 
             AND CTOR.UniqMonthID = '$month_id' 
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.RecordNumber = CD.RecordNumber 
             AND CD.UniqMonthID = '$month_id'  
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber             
 INNER JOIN  global_temp.MHS501HospProvSpell_open_end_rp AS HSP 
 			ON REF.UniqServReqID = HSP.UniqServReqID 
 INNER JOIN   global_temp.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID 
   LEFT JOIN global_temp.MHS502WardStay_Open_End_RP AS WRD
 	        ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NULL AND CTOR.Person_ID IS NULL AND CD.UniqMHActEpisodeID IS NULL)
 			AND STO.RecordNumber IS NULL
             AND MHA.Person_ID IS NOT NULL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS401MHActPeriod_GRD_open_end_rp AS 
     SELECT	*
       FROM	$db_source.MHS401MHActPeriod
      WHERE  UniqMonthID = '$month_id' 
             AND legalstatuscode IN ('35', '36')
      	    AND (EndDateMHActLegalStatusClass IS NULL OR EndDateMHActLegalStatusClass > '$rp_enddate')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs08_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs08_prep AS
 SELECT                 ref.Person_ID, 
                        ref.RecordNumber
                   FROM global_temp.MHS101Referral_open_end_rp as REF
             INNER JOIN global_temp.MHS401MHActPeriod_GRD_open_end_rp MHA
                        ON REF.Person_ID = MHA.Person_ID
                  UNION SELECT Person_ID, RecordNumber	FROM $db_output.mha_mhs09_prep
                  UNION SELECT Person_ID, RecordNumber	FROM global_temp.MHS10_INTERMEDIATE
                  UNION SELECT Person_ID, RecordNumber	FROM global_temp.MHS11_INTERMEDIATE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs10_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs10_prep
 SELECT                 MHS10.Person_ID, 
                        MHS10.RecordNumber
 FROM global_temp.MHS10_INTERMEDIATE AS MHS10 
   LEFT JOIN global_temp.MHS11_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID           
   LEFT JOIN $db_output.mha_mhs09_prep AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS11Prov_INTERMEDIATE AS
 --INSERT INTO menh_analysis.MHS11Prov_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
 --            ,AMHServiceRefEndRP
 --            ,CYPServiceRefEndRP
 --            ,LDAServiceRefEndRP
             ,AgeRepPeriodEnd
       FROM	global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON REF.Person_ID = STO.Person_ID 
             AND REF.OrgIDProv = STO.OrgIDProv 
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv 
             AND  MPI.UniqMonthID = '$month_id'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS10Prov_INTERMEDIATE AS
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
             ,od.ORG_TYPE_CODE AS ProvType
  --           ,AMHServiceRefEndRP
  --          ,CYPServiceRefEndRP
  --           ,LDAServiceRefEndRP
             ,AgeRepPeriodEnd
       FROM	global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND REF.OrgIDProv = MHA.OrgIDProv 
             AND MHA.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.UniqMHActEpisodeID = CTO.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CTO.OrgIDProv 
             AND CTO.UniqMonthID = '$month_id' 
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.UniqMHActEpisodeID = CTOR.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CTOR.OrgIDProv 
             AND CTOR.UniqMonthID = '$month_id'
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.UniqMHActEpisodeID = CD.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CD.OrgIDProv 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv 
             AND  MPI.UniqMonthID = '$month_id'
 left join $db_output.mha_rd_org_daily_latest od on REF.orgidProv = od.ORG_CODE 
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NOT NULL OR CTOR.Person_ID IS NOT NULL OR CD.UniqMHActEpisodeID IS NOT NULL)
 			AND STO.RecordNumber IS NULL

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs09prov_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs09prov_prep AS
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
             ,od.ORG_TYPE_CODE AS ProvType
 --            ,AMHServiceWSEndRP
 --            ,CYPServiceWSEndRP
 --            ,LDAServiceWSEndRP    
             ,AgeRepPeriodEnd
       FROM	global_temp.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
 --             AND REF.OrgIDProv = MHA.OrgIDProv removing this a provider which provided detention in March may have closed before 31st March -- results in undercount otherwise
             AND MHA.UniqMonthID = '$month_id'
             AND MHA.legalstatuscode IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38') 
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 		    ON MHA.RecordNumber = CTO.RecordNumber 
             AND MHA.OrgIDProv = CTO.OrgIDProv 
             AND CTO.UniqMonthID = '$month_id'
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.RecordNumber = CTOR.RecordNumber 
             AND MHA.OrgIDProv = CTOR.OrgIDProv 
             AND CTOR.UniqMonthID = '$month_id'
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.RecordNumber = CD.RecordNumber 
             AND MHA.OrgIDProv = CD.OrgIDProv 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber 
             AND MHA.OrgIDProv = STO.OrgIDProv 
 INNER JOIN  $db_source.MHS001MPI MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv
             AND MPI.UniqMonthID = '$month_id'
 INNER JOIN  global_temp.MHS501HospProvSpell_open_end_rp AS HSP
 			ON REF.UniqServReqID = HSP.UniqServReqID 
             AND REF.OrgIDProv = HSP.OrgIDProv 
 LEFT JOIN global_temp.MHS502WardStay_Open_End_RP AS WRD
 	   ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID
 left join $db_output.mha_rd_org_daily_latest od on REF.orgidProv = od.ORG_CODE 
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NULL AND CTOR.Person_ID IS NULL AND CD.UniqMHActEpisodeID IS NULL)
 			AND STO.RecordNumber IS NULL            

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs08prov_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs08prov_prep AS

 SELECT                 REF.Person_ID, 
                        REF.RecordNumber,
                        REF.OrgIDProv,
                        od.ORG_TYPE_CODE AS ProvType
                   FROM global_temp.MHS101Referral_open_end_rp AS REF
             INNER JOIN global_temp.MHS401MHActPeriod_GRD_open_end_rp AS MHA
                        ON REF.Person_ID = MHA.Person_ID
                        AND REF.OrgIDProv = MHA.OrgIDProv
                    LEFT JOIN $db_output.mha_rd_org_daily_latest od on REF.orgidProv = od.ORG_CODE 
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv, od.ORG_TYPE_CODE AS ProvType 
                  FROM $db_output.mha_mhs09prov_prep mhs09
                  LEFT JOIN $db_output.mha_rd_org_daily_latest od on mhs09.orgidProv = od.ORG_CODE 
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv, od.ORG_TYPE_CODE AS ProvType 
                  FROM global_temp.MHS10Prov_INTERMEDIATE mhs10
                  LEFT JOIN $db_output.mha_rd_org_daily_latest od on mhs10.orgidProv = od.ORG_CODE
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv, od.ORG_TYPE_CODE AS ProvType 
                  FROM global_temp.MHS11Prov_INTERMEDIATE mhs11
                  LEFT JOIN  $db_output.mha_rd_org_daily_latest od on mhs11.orgidProv = od.ORG_CODE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs10prov_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs10prov_prep AS

 SELECT MHS10.Person_ID, MHS10.RecordNumber, MHS10.OrgIDProv, od.ORG_TYPE_CODE AS ProvType
        FROM global_temp.MHS10Prov_INTERMEDIATE AS MHS10 
   LEFT JOIN global_temp.MHS11Prov_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID
             AND MHS10.OrgIDProv = MHS11.OrgIDProv
   LEFT JOIN $db_output.mha_mhs09prov_prep AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID
             AND MHS10.OrgIDProv = MHS09.OrgIDProv
      INNER JOIN $db_source.MHS001MPI AS PRSN 
             ON MHS10.Person_ID = PRSN.Person_ID
             AND MHS10.OrgIDProv = PRSN.OrgIDProv
             AND PRSN.UniqMonthID = '$month_id'
       LEFT JOIN $db_output.mha_rd_org_daily_latest od on MHS10.orgidProv = od.ORG_CODE 
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs08_additional_prep;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs08_additional_prep AS

 SELECT			PRSN.Person_ID,
 				REF.UniqServReqID,
 				REF.OrgIDProv,
 				MHA.UniqMHActEpisodeID,
 				MHA.LegalStatusCode,
 				HSP.StartDateHospProvSpell,
 				MHa.StartDateMHActLegalStatusClass

 FROM			$db_source.MHS001MPI
 					AS PRSN
 				LEFT OUTER JOIN $db_source.MHS101Referral
 					AS REF
 					ON PRSN.Person_ID = REF.Person_ID AND REF.UniqMonthID = '$month_id'
 				LEFT OUTER JOIN $db_source.MHS401MHActPeriod
 					AS MHA
 					ON PRSN.Person_ID = MHA.Person_ID AND MHA.UniqMonthID = '$month_id' AND MHA.LegalStatusCode IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38')
 				LEFT OUTER JOIN $db_source.MHS404CommTreatOrder
 					AS CTO
 					ON MHA.RecordNumber = CTO.RecordNumber AND CTO.UniqMonthID = '$month_id' AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
 				LEFT OUTER JOIN $db_source.MHS405CommTreatOrderRecall
 					AS CTOR
 					ON MHA.RecordNumber = CTOR.RecordNumber AND CTOR.UniqMonthID = '$month_id' AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
 				LEFT OUTER JOIN $db_source.MHS403ConditionalDischarge
 					AS CD
 					ON MHA.RecordNumber = CD.RecordNumber AND CD.UniqMonthID = '$month_id' AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
 				LEFT OUTER JOIN $db_source.MHS401MHActPeriod
 					AS STO
 					ON MHA.RecordNumber = STO.RecordNumber AND STO.UniqMonthID = '$month_id' AND STO.LegalStatusCode IN ('04', '05', '06', '19', '20')
 				LEFT OUTER JOIN $db_source.MHS501HospProvSpell
 					AS HSP
 					ON REF.UniqServReqID = HSP.UniqServReqID AND HSP.UniqMonthID = '$month_id' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell > '$rp_enddate')
 				
 WHERE			PRSN.UniqMonthID = '$month_id'
 			AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate')
 			AND (MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND REF.Person_ID IS NOT NULL
 			AND MHA.Person_ID IS NOT NULL
 			AND HSP.Person_ID IS NOT NULL
 			AND (CTO.Person_ID IS NULL AND CTOR.Person_ID IS NULL AND CD.UniqMHActEpisodeID IS NULL)
 			AND STO.RecordNumber IS NULL

 GROUP BY		PRSN.Person_ID,
 				REF.UniqServReqID,
 				REF.OrgIDProv,
 				MHA.UniqMHActEpisodeID,
 				MHA.LegalStatusCode,
 				HSP.StartDateHospProvSpell,
 				MHa.StartDateMHActLegalStatusClass

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mha_mhs08_additional;
 CREATE TABLE IF NOT EXISTS $db_output.mha_mhs08_additional AS
 SELECT		'Part 2' as MHA_Part, 
             'ENGLAND' as OrgType, 
             count(distinct Person_ID) as mhs09			
 FROM		$db_output.mha_mhs08_additional_prep				
 WHERE		LegalStatusCode in ('02', '03')
 UNION
 SELECT		'Part 2' as MHA_Part, 
             case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end as OrgType,
             count(distinct Person_ID) as mhs09	
 FROM		$db_output.mha_mhs08_additional_prep
 WHERE		LegalStatusCode in ('02', '03')
 group by	case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end 
 UNION 
 SELECT		'Part 3' as MHA_Part, 
             'England' as OrgType, 
             count(distinct Person_ID) as mhs09
 FROM		$db_output.mha_mhs08_additional_prep
 WHERE		LegalStatusCode in ('07','08','09','10','37','38','15','16','17','18','12', '13', '14')
 UNION
 SELECT		'Part 3' as MHA_Part, 
             case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end as OrgType,
             count(distinct Person_ID) as mhs09
 FROM		$db_output.mha_mhs08_additional_prep
 WHERE		LegalStatusCode in ('07','08','09','10','37','38','15','16','17','18','12', '13', '14') 			
 group by    case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end
 UNION
 SELECT		'Other' as MHA_Part, 
             'England' as OrgType, 
             count(distinct Person_ID) as mhs09
 FROM		$db_output.mha_mhs08_additional_prep
 WHERE		LegalStatusCode not in ('02','03','07','08','09','10','37','38','15','16','17','18','12', '13', '14') 
 UNION
 SELECT      'Other' as MHA_Part,
             case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end as OrgType, 
             count(distinct Person_ID) as mhs09
 FROM		$db_output.mha_mhs08_additional_prep
 WHERE		LegalStatusCode not in ('02','03','07','08','09','10','37','38','15','16','17','18','12', '13', '14') 			
 group by    case when (length(OrgIDProv) = 3 and left(OrgIDProv, 1) in ("R", "T")) then "NHS Trust"
             else "Independent Health Provider" end