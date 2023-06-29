USE [ECDS]


/*** UPDATE ME ***/
DECLARE @STARTDATE DATE
SET @STARTDATE = '2020-04-01'

DECLARE @ENDDATE DATE
SET @ENDDATE = '2021-03-31'

IF OBJECT_ID('tempdb..#SECTION_1') IS NOT NULL DROP TABLE #SECTION_1
IF OBJECT_ID('tempdb..#DISTINCT_CLASS') IS NOT NULL DROP TABLE #DISTINCT_CLASS


----------------------------------------------------------
-- Produces unsuppressed counts for each specified measure grouping by provider and filtering on where classification code
-- is not null
-----------------------------------------------------------
/******* 1. Formal admission to hospital *******/
SELECT * INTO #SECTION_1
FROM(
SELECT 
	CASE WHEN [PROVIDER_SUBMITTED] LIKE '[A-Za-z][0-9][A-Za-z][0-9][A-Za-z]' THEN [PROVIDER_SUBMITTED]
		 WHEN ([PROVIDER_SUBMITTED] LIKE 'R%' OR [PROVIDER_SUBMITTED] LIKE 'T%') THEN LEFT([PROVIDER_SUBMITTED],3)
		ELSE [PROVIDER_SUBMITTED]
	END AS 'Organisation_Code'
	,t7.[NAME] as 'Org Name'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 02 THEN 1 ELSE 0 END) AS '1#1_Section_2'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 03 THEN 1 ELSE 0 END) AS '1#2_Section_3'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 20 THEN 1 ELSE 0 END) AS '1#3_Section_136'
	--,SUM(CASE WHEN T3.[CLASSIFICATION] NOT IN (01,02,03,20,98,99) THEN 1 ELSE 0 END) AS '1#4_Any_Other'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 04 THEN 1 ELSE 0 END) AS '1#4_Section_4'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 05 THEN 1 ELSE 0 END) AS '1#5_Section_5(2)'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 06 THEN 1 ELSE 0 END) AS '1#6_Section_5(4)'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 07 THEN 1 ELSE 0 END) AS '1#7_Section_35'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 08 THEN 1 ELSE 0 END) AS '1#8_Section_36'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 09 THEN 1 ELSE 0 END) AS '1#9_Section_37_with_section_41_restrictions'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 10 THEN 1 ELSE 0 END) AS '1#10_Section_37'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 12 THEN 1 ELSE 0 END) AS '1#11_Section_38'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 13 THEN 1 ELSE 0 END) AS '1#12_Section_44'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 14 THEN 1 ELSE 0 END) AS '1#13_Section_46'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 15 THEN 1 ELSE 0 END) AS '1#14_Section_47_with_section_49_restrictions'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 16 THEN 1 ELSE 0 END) AS '1#15_Section_47'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 17 THEN 1 ELSE 0 END) AS '1#16_Section_48 with_section_49_restrictions'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 18 THEN 1 ELSE 0 END) AS '1#17_Section_48'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 19 THEN 1 ELSE 0 END) AS '1#19_Section_135'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 31 THEN 1 ELSE 0 END) AS '1#20_Criminal_Procedure(Insanity)_Act_1964'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 32 THEN 1 ELSE 0 END) AS '1#21_Formally_detained_under_other_acts'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 35 THEN 1 ELSE 0 END) AS '1#22_Section_7'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 36 THEN 1 ELSE 0 END) AS '1#23_Section_37'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 37 THEN 1 ELSE 0 END) AS '1#24_Section_45A#-(Limited direction in force)'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 38 THEN 1 ELSE 0 END) AS '1#25_Section_45A_(Limitation direction ended)'
	---------------------------------------------------------------------------------------------------------
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 06 THEN 1 ELSE 0 END) AS '2#1_Informal_to_5_(4)'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 05 THEN 1 ELSE 0 END) AS '2#2_Informal_to_5_(2)'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 02 THEN 1 ELSE 0 END) AS '2#3_Informal_to_2'
	,SUM(CASE WHEN T3.[CLASSIFICATION] = 03 THEN 1 ELSE 0 END) AS '2#6_Informal_to_3'

FROM DQ.[DQ_VW_ATT_ATTENDANCE] AS T1 
	LEFT JOIN DQ.[DQ_VW_SYS_SYSTEM] AS T2 
	ON T1.[RECORD_IDENTIFIER] = T2.[RECORD_IDENTIFIER] 
		AND T1.[FILE_LOAD_ID] = t2.[FILE_LOAD_ID]
	LEFT JOIN DQ.[DQ_VW_PAT_MHA] AS T3 
	ON T1.[RECORD_IDENTIFIER] = T3.[RECORD_IDENTIFIER] 
		AND T1.[FILE_LOAD_ID] = t3.[FILE_LOAD_ID]
LEFT JOIN ECDS.REF.ORG_DAILY as t7
		ON CASE WHEN [PROVIDER_SUBMITTED] LIKE '?#?#?' THEN [PROVIDER_SUBMITTED]
				WHEN ([PROVIDER_SUBMITTED] LIKE 'R%' OR [PROVIDER_SUBMITTED] LIKE 'T%') THEN LEFT([PROVIDER_SUBMITTED],3)
			ELSE [PROVIDER_SUBMITTED] END = T7.ORG_CODE AND BUSINESS_END_DATE IS NULL AND (
		(ORG_CLOSE_DATE IS NULL AND ORG_OPEN_DATE <= ARRIVAL_DATE) OR (ORG_CLOSE_DATE >= ARRIVAL_DATE AND ORG_OPEN_DATE <= ARRIVAL_DATE))

WHERE 
	T2.[IS_MONTHLY_PUB] = 1 AND 
	T1.[ARRIVAL_DATE] BETWEEN @STARTDATE AND @ENDDATE 
	AND T3.[CLASSIFICATION] IS NOT NULL
	AND DEPARTMENT_TYPE != '05'

GROUP BY 
CASE WHEN [PROVIDER_SUBMITTED] LIKE '[A-Za-z][0-9][A-Za-z][0-9][A-Za-z]' THEN [PROVIDER_SUBMITTED]
		 WHEN ([PROVIDER_SUBMITTED] LIKE 'R%' OR [PROVIDER_SUBMITTED] LIKE 'T%') THEN LEFT([PROVIDER_SUBMITTED],3)
		ELSE [PROVIDER_SUBMITTED]
	END,
t7.[NAME]

--ORDER BY
	--[PROVIDER_SUBMITTED]
)_ 

SELECT * FROM #SECTION_1
ORDER BY [Organisation_Code]


------------------------------------------------------------------------------------
-- Suppression of small numbers from temp table we used to store unsuppressed counts
------------------------------------------------------------------------------------
SELECT 
	t1.[Organisation_Code]
	,[Org Name]
	,CASE WHEN cast(SUM(t1.[1#1_Section_2]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((SUM(t1.[1#1_Section_2])*1.0)/5,0)*5 AS int) as varchar) end as '1#1_Section_2'

	,CASE WHEN cast(sum(t1.[1#2_Section_3]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#2_Section_3])*1.0)/5,0)*5 AS int) as varchar) end as '1#2_Section_3'

	,CASE WHEN cast(sum(t1.[1#3_Section_136]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#3_Section_136])*1.0)/5,0)*5 AS int) as varchar) end as '1#3_Section_136'

	,CASE WHEN cast(sum(t1.[1#4_Section_4]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#4_Section_4])*1.0)/5,0)*5 AS int) as varchar) end as '1#4_Section_4'

	,CASE WHEN cast(sum(t1.[1#5_Section_5(2)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#5_Section_5(2)])*1.0)/5,0)*5 AS int) as varchar) end as '1#5_Section_5(2)'

	,CASE WHEN cast(sum(t1.[1#6_Section_5(4)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#6_Section_5(4)])*1.0)/5,0)*5 AS int) as varchar) end as '1#6_Section_5(4)'

	,CASE WHEN cast(sum(t1.[1#7_Section_35]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#7_Section_35])*1.0)/5,0)*5 AS int) as varchar) end as '1#7_Section_35'

	,CASE WHEN cast(sum(t1.[1#8_Section_36]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#8_Section_36])*1.0)/5,0)*5 AS int) as varchar) end as '1#8_Section_36'

	,CASE WHEN cast(sum(t1.[1#9_Section_37_with_section_41_restrictions]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#9_Section_37_with_section_41_restrictions])*1.0)/5,0)*5 AS int) as varchar) end as '1#9_Section_37_with_section_41_restrictions'

	,CASE WHEN cast(sum(t1.[1#10_Section_37]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#10_Section_37])*1.0)/5,0)*5 AS int) as varchar) end as '1#10_Section_37'

	,CASE WHEN cast(sum(t1.[1#11_Section_38]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#11_Section_38])*1.0)/5,0)*5 AS int) as varchar) end as '1#11_Section_38'

	,CASE WHEN cast(sum(t1.[1#12_Section_44]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#12_Section_44])*1.0)/5,0)*5 AS int) as varchar) end as '1#12_Section_44'

	,CASE WHEN cast(sum(t1.[1#13_Section_46]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#13_Section_46])*1.0)/5,0)*5 AS int) as varchar) end as '1#13_Section_46'

	,CASE WHEN cast(sum(t1.[1#14_Section_47_with_section_49_restrictions]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#14_Section_47_with_section_49_restrictions])*1.0)/5,0)*5 AS int) as varchar) end as '1#14_Section_47_with_section_49_restrictions'

	,CASE WHEN cast(sum(t1.[1#15_Section_47]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#15_Section_47])*1.0)/5,0)*5 AS int) as varchar) end as '1#15_Section_47'

	,CASE WHEN cast(sum(t1.[1#16_Section_48 with_section_49_restrictions]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#16_Section_48 with_section_49_restrictions])*1.0)/5,0)*5 AS int) as varchar) end as '1#16_Section_48 with_section_49_restrictions'

	,CASE WHEN cast(sum(t1.[1#17_Section_48]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#17_Section_48])*1.0)/5,0)*5 AS int) as varchar) end as '1#17_Section_48'

	,CASE WHEN cast(sum(t1.[1#19_Section_135]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#19_Section_135])*1.0)/5,0)*5 AS int) as varchar) end as '1#19_Section_135'

	,CASE WHEN cast(sum(t1.[1#20_Criminal_Procedure(Insanity)_Act_1964]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#20_Criminal_Procedure(Insanity)_Act_1964])*1.0)/5,0)*5 AS int) as varchar) end as '1#20_Criminal_Procedure(Insanity)_Act_1964'

	,CASE WHEN cast(sum(t1.[1#21_Formally_detained_under_other_acts]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#21_Formally_detained_under_other_acts])*1.0)/5,0)*5 AS int) as varchar) end as '1#21_Formally_detained_under_other_acts'

	,CASE WHEN cast(sum(t1.[1#22_Section_7]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#22_Section_7])*1.0)/5,0)*5 AS int) as varchar) end as '1#22_Section_7'

	,CASE WHEN cast(sum(t1.[1#23_Section_37]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#23_Section_37])*1.0)/5,0)*5 AS int) as varchar) end as '1#23_Section_3'

	,CASE WHEN cast(SUM(t1.[1#24_Section_45A#-(Limited direction in force)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((SUM(t1.[1#24_Section_45A#-(Limited direction in force)])*1.0)/5,0)*5 AS int) as varchar) end as '1#24_Section_45A#-(Limited direction in force)'

	,CASE WHEN cast(sum(t1.[1#25_Section_45A_(Limitation direction ended)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[1#25_Section_45A_(Limitation direction ended)])*1.0)/5,0)*5 AS int) as varchar) end as '1#25_Section_45A_(Limitation direction ended)'

	,CASE WHEN cast(sum(t1.[2#1_Informal_to_5_(4)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[2#1_Informal_to_5_(4)])*1.0)/5,0)*5 AS int) as varchar) end as '2#1_Informal_to_5_(4)'

	,CASE WHEN cast(sum(t1.[2#2_Informal_to_5_(2)]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[2#2_Informal_to_5_(2)])*1.0)/5,0)*5 AS int) as varchar) end as '2#2_Informal_to_5_(2)'

	,CASE WHEN cast(sum(t1.[2#3_Informal_to_2]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[2#3_Informal_to_2])*1.0)/5,0)*5 AS int) as varchar) end as '2#3_Informal_to_2'

	,CASE WHEN cast(sum(t1.[2#6_Informal_to_3]) as varchar) BETWEEN 1 AND 7 THEN '*' 
		  ELSE CAST(CAST(ROUND((sum(t1.[2#6_Informal_to_3])*1.0)/5,0)*5 AS int) as varchar) end as '2#6_Informal_to_3'


FROM #SECTION_1 AS t1

GROUP BY
	 [Organisation_Code], [Org Name]

ORDER BY [Organisation_Code]

------------------------------------------------------------------------------------------
/**** CODE TO CHECK HOW MANY ID's HAVE DIFFERENT CLASSIFICATION CODE****/

SELECT * INTO #DISTINCT_CLASS
FROM(
SELECT 
	CASE WHEN [PROVIDER_SUBMITTED] LIKE '[A-Za-z][0-9][A-Za-z][0-9][A-Za-z]' THEN [PROVIDER_SUBMITTED]
		 WHEN ([PROVIDER_SUBMITTED] LIKE 'R%' OR [PROVIDER_SUBMITTED] LIKE 'T%') THEN LEFT([PROVIDER_SUBMITTED],3)
		ELSE [PROVIDER_SUBMITTED]
	END AS 'PROVIDER_SUBMITTED',
	T1.[RECORD_IDENTIFIER],
	--[CLASSIFICATION], 
	--[START_DATE], 
	--[START_TIME]
	COUNT(DISTINCT [CLASSIFICATION]) AS 'DISTINCT CLASS'
FROM DQ.[DQ_VW_ATT_ATTENDANCE] AS T1 
	INNER JOIN DQ.[DQ_VW_SYS_SYSTEM] AS T2 
	ON T1.[RECORD_IDENTIFIER] = T2.[RECORD_IDENTIFIER] and t1.FILE_LOAD_ID = t2.[FILE_LOAD_ID]
	INNER JOIN DQ.[DQ_VW_PAT_MHA] AS T3 
	ON T1.[RECORD_IDENTIFIER] = T3.[RECORD_IDENTIFIER] and t1.[FILE_LOAD_ID] = t2.[FILE_LOAD_ID]

WHERE T1.[ARRIVAL_DATE] BETWEEN @STARTDATE AND @ENDDATE 
AND [IS_MONTHLY_PUB] = 1 AND [CLASSIFICATION] IN ('01','02','03','04','05','06','20') 
AND DEPARTMENT_TYPE != '05'


GROUP BY 
	CASE WHEN [PROVIDER_SUBMITTED] LIKE '[A-Za-z][0-9][A-Za-z][0-9][A-Za-z]' THEN [PROVIDER_SUBMITTED]
		 WHEN ([PROVIDER_SUBMITTED] LIKE 'R%' OR [PROVIDER_SUBMITTED] LIKE 'T%') THEN LEFT([PROVIDER_SUBMITTED],3)
		ELSE [PROVIDER_SUBMITTED]
	END,
	T1.[RECORD_IDENTIFIER]
HAVING
	COUNT(DISTINCT [CLASSIFICATION]) > 1
)_ 

SELECT 
	PROVIDER_SUBMITTED,
	T1.[RECORD_IDENTIFIER], 
	[CLASSIFICATION], 
	[START_DATE], 
	[START_TIME]

FROM DQ.DQ_VW_PAT_MHA AS T1
INNER JOIN #DISTINCT_CLASS AS T2
	ON T1.[RECORD_IDENTIFIER] = T2.[RECORD_IDENTIFIER]

ORDER BY [PROVIDER_SUBMITTED], [RECORD_IDENTIFIER]