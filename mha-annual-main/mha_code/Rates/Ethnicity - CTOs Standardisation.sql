-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW TOT_POP AS

select 
SUM(population) as population
from 
$db_output.POP_HEALTH
where 
Der_Gender in ('1','2')
-- and 
-- ethnic_group_formatted in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
									

-- COMMAND ----------

 %python spark.conf.set('tot.pop',spark.sql("SELECT population FROM TOT_POP").collect()[0]['population'])

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW POP_DATA_a AS

SELECT 
ethnic_group_formatted,
Ethnic_group,
Der_Gender,
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
	end AS AGE_GROUP,
sum(Population) as Population

from $db_output.POP_HEALTH
WHERE
Der_Gender in ('1','2')
and 
ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
GROUP BY 
ethnic_group_formatted,
Ethnic_group,
Der_Gender,
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
	end

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Data_a AS 

select 
distinct
CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
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
    END as Ethnicity,
    Der_Gender,
case 
    when AgeRepPeriodEnd between 0 and 15 then '15 and under'
    when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
    when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
    when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
    when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
    when AgeRepPeriodEnd >= 65 then '65 and over'
    else 'Unknown'
    end as age_group,
COUNT(DISTINCT CTO_UniqMHActEpisodeID) as count

FROM 
$db_output.CTO
WHERE
EthnicCategory is not NULL and EthnicCategory not in ('Z','99')
AND AgeRepPeriodEnd is not null
AND Der_Gender in ('1','2')

group by 
case 
    when AgeRepPeriodEnd between 0 and 15 then '15 and under'
    when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
    when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
    when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
    when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
    when AgeRepPeriodEnd >= 65 then '65 and over'
    else 'Unknown'
    end,
CASE 
    WHEN EthnicCategory = 'A' THEN 'British'
    WHEN EthnicCategory = 'B' THEN 'Irish'
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
Der_Gender

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW RATE_a AS

select 
a.ethnic_group_formatted as Ethnicity,
a.Der_Gender,
a.age_group,
b.count,
a.population,
cast(b.count as FLOAT)/cast(a.population as FLOAT) as rate
from  POP_DATA_a a
left join  Data_a b on b.Ethnicity = a.ethnic_group_formatted and a.Der_Gender = b.Der_Gender and a.age_group = b.age_group
where
a.Der_Gender in ('1','2')


-- COMMAND ----------

--Get population rates
CREATE OR REPLACE TEMPORARY VIEW POP_RATES_a AS

Select
a.Der_Gender,
a.age_group,
cast(sum(population) as FLOAT)/cast(${tot.pop} as FLOAT) as pop_rates
from pop_data_A a
where 
Der_Gender in ('1','2')
and 
ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
group 
by a.Der_Gender,
a.age_group

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW S_RATES_a AS 

Select
a.age_group,
a.Der_Gender,
a.ethnicity,
rate,
pop_rates,
cast(rate*pop_rates as FLOAT) as standardised_rate

from RATE_a a
left join POP_RATES_a b on a.Der_Gender = b.Der_Gender and a.age_group = b.age_group

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW standardised_rate_a AS 
--standardised rate output per 100,000
select 
ethnicity,
SUM(standardised_rate)*100000 as standardised_rate

from S_RATES_a
group by ethnicity


-- COMMAND ----------


CREATE OR REPLACE TEMPORARY VIEW SU_CI_a AS

select 
a.Ethnicity
,a.Der_Gender
,a.age_group
,b.pop_totals
,a.rate
,c.population
,((cast(b.pop_totals as FLOAT)*cast(b.pop_totals as FLOAT))*a.rate*(1-a.rate))/cast(sum(c.population) as FLOAT) as CI_rate
from rate_a a
left join 
(Select
a.Der_Gender,
a.age_group,
sum(population) as pop_totals
from pop_data_A a
where 
age_group not in ('0 to 4','10 to 14','16 to 17','5 to 7','8 to 9','15') 
	and 
	Der_Gender in ('1','2')
	and 
    ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
	group by 
	Der_Gender,
	age_group
	) b on a.Der_Gender = b.Der_Gender and a.age_group = b.age_group
left join pop_data_a c on a.Ethnicity = c.ethnic_group_formatted and a.Der_Gender = c.Der_Gender and a.age_group = c.age_group
group by
a.Ethnicity
,a.Der_Gender
,a.age_group
,b.pop_totals
,a.rate
,c.population

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Confidence_interval_a AS
select 
Ethnicity
,SUM(CI_rate) as CI_rate
,1.96*SQRT(cast(1.0/(cast(${tot.pop} as FLOAT)*cast(${tot.pop} as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as Confidence_interval
from SU_CI_a
group by Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW a_rates AS 
select
a.Ethnicity,
sum(c.count) as count,
sum(c.population) as population,
avg(a.standardised_rate) as standardised_rate_per_100000,
avg(b.Confidence_interval) as CI
from standardised_rate_a a
left join Confidence_interval_a b on a.Ethnicity = b.Ethnicity
left join RATE_a c on a.Ethnicity = c.Ethnicity
group by a.Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW a_rates_final AS

--Final output, combingin standardised rate and confidence interval
select 
"Ethnicity" as demographic_breakdown,
"All" as organisation_breakdown,
Ethnicity as primary_level,
Ethnicity as primary_level_desc,
"NONE" as secondary_level,
"NONE" as secondary_level_desc,
"Uses of CTOs" as count_of,
"All" as sub_measure,
count,
coalesce(population, 0) as population,
coalesce(round((count / population) * 100000, 1), 0) as crude_rate_per_100000,
coalesce(round(standardised_rate_per_100000, 1), 0) as standardised_rate_per_100000,
coalesce(round(CI, 1), 0) as CI
from a_rates
order by Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW POP_DATA_b AS

SELECT 
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
    ELSE NULL
END as ethnic_group_formatted,
Der_Gender,
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
	end AS AGE_GROUP,
sum(Population) as Population

from $db_output.POP_HEALTH
WHERE
Der_Gender in ('1','2')
and 
ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
GROUP BY 
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
    ELSE NULL
END,
Der_Gender,
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
	end

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Data_b AS 

select 
distinct
CASE
    WHEN EthnicCategory IN ('A','B','C') THEN 'White'
    WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
    WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
    WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
    WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
END as Ethnicity,
    Der_Gender,
case 
    when AgeRepPeriodEnd between 0 and 15 then '15 and under'
    when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
    when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
    when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
    when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
    when AgeRepPeriodEnd >= 65 then '65 and over'
    else 'Unknown'
    end as age_group,
COUNT(DISTINCT CTO_UniqMHActEpisodeID) as count

FROM 
$db_output.CTO
WHERE
EthnicCategory is not NULL and EthnicCategory not in ('Z','99')
AND AgeRepPeriodEnd is not null
AND Der_Gender in ('1','2')
---as per standardisation methodology, counts to be standardised needs to be where all demographics are known and have values that can be standardised (such as Male and Female only)
group by 
case 
    when AgeRepPeriodEnd between 0 and 15 then '15 and under'
    when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
    when AgeRepPeriodEnd between 18 and 34 then '18 to 34'
    when AgeRepPeriodEnd between 35 and 49 then '35 to 49'
    when AgeRepPeriodEnd between 50 and 64 then '50 to 64'
    when AgeRepPeriodEnd >= 65 then '65 and over'
    else 'Unknown'
    end,
CASE
    WHEN EthnicCategory IN ('A','B','C') THEN 'White'
    WHEN EthnicCategory IN ('D','E','F','G') THEN 'Mixed'
    WHEN EthnicCategory IN ('H','J','K','L') THEN 'Asian or Asian British'
    WHEN EthnicCategory IN ('M','N','P') THEN 'Black or Black British'
    WHEN EthnicCategory IN ('R','S') THEN 'Other Ethnic Groups'
END,
Der_Gender

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW RATE_b AS

select 
a.ethnic_group_formatted as Ethnicity,
a.Der_Gender,
a.age_group,
b.count,
a.population,
cast(b.count as FLOAT)/cast(a.population as FLOAT) as rate
from  POP_DATA_b a
left join  Data_b b on b.Ethnicity = a.ethnic_group_formatted and a.Der_Gender = b.Der_Gender and a.age_group = b.age_group
where
a.Der_Gender in ('1','2')

-- COMMAND ----------

--Get population rates
CREATE OR REPLACE TEMPORARY VIEW POP_RATES_b AS

Select
a.Der_Gender,
a.age_group,
cast(sum(population) as FLOAT)/cast(${tot.pop} as FLOAT) as pop_rates
from pop_data_A a
where 
Der_Gender in ('1','2')
and 
ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
group 
by a.Der_Gender,
a.age_group

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW S_RATES_b AS 

Select
a.age_group,
a.Der_Gender,
a.ethnicity,
rate,
pop_rates,
cast(rate*pop_rates as FLOAT) as standardised_rate

from RATE_b a
left join POP_RATES_b b on a.Der_Gender = b.Der_Gender and a.age_group = b.age_group

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW standardised_rate_b AS 
--standardised rate output per 100,000
select 
ethnicity,
SUM(standardised_rate)*100000 as standardised_rate

from S_RATES_b
group by ethnicity


-- COMMAND ----------


CREATE OR REPLACE TEMPORARY VIEW SU_CI_b AS

select 
a.Ethnicity
,a.Der_Gender
,a.age_group
,b.pop_totals
,a.rate
,c.population
,((cast(b.pop_totals as FLOAT)*cast(b.pop_totals as FLOAT))*a.rate*(1-a.rate))/cast(sum(c.population) as FLOAT) as CI_rate
from rate_b a
left join 
(Select
a.Der_Gender,
a.age_group,
sum(population) as pop_totals
from pop_data_A a
where 
age_group not in ('0 to 4','10 to 14','16 to 17','5 to 7','8 to 9','15') 
	and 
	Der_Gender in ('1','2')
	and 
    ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
	group by 
	Der_Gender,
	age_group
	) b on a.Der_Gender = b.Der_Gender and a.age_group = b.age_group
left join pop_data_B c on a.Ethnicity = c.ethnic_group_formatted and a.Der_Gender = c.Der_Gender and a.age_group = c.age_group
group by
a.Ethnicity
,a.Der_Gender
,a.age_group
,b.pop_totals
,a.rate
,c.population

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Confidence_interval_b AS
select 
Ethnicity
,SUM(CI_rate) as CI_rate
,1.96*SQRT(cast(1.0/(cast(${tot.pop} as FLOAT)*cast(${tot.pop} as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as Confidence_interval
from SU_CI_b
group by Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW b_rates AS 
select
a.Ethnicity,
sum(c.count) as count,
sum(c.population) as population,
avg(a.standardised_rate) as standardised_rate_per_100000,
avg(b.Confidence_interval) as CI
from standardised_rate_b a
left join Confidence_interval_b b on a.Ethnicity = b.Ethnicity
left join RATE_b c on a.Ethnicity = c.Ethnicity
group by a.Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW b_rates_final AS

--Final output, combingin standardised rate and confidence interval
select 
"Ethnicity" as demographic_breakdown,
"All" as organisation_breakdown,
Ethnicity as primary_level,
Ethnicity as primary_level_desc,
"NONE" as secondary_level,
"NONE" as secondary_level_desc,
"Uses of CTOs" as count_of,
"All" as sub_measure,
count,
coalesce(population, 0) as population,
coalesce(round((count / population) * 100000, 1), 0) as crude_rate_per_100000,
coalesce(round(standardised_rate_per_100000, 1), 0) as standardised_rate_per_100000,
coalesce(round(CI, 1), 0) as CI
from b_rates
order by Ethnicity

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW c_rates_final AS

--Final output, combingin standardised rate and confidence interval
select 
"Ethnicity" as demographic_breakdown,
"All" as organisation_breakdown,
"All" as primary_level,
"All" as primary_level_desc,
"NONE" as secondary_level,
"NONE" as secondary_level_desc,
"Uses of CTOs" as count_of,
"All" as sub_measure,
sum(count) as count,
coalesce(sum(population), 0) as population,
coalesce(round((sum(count) / sum(population)) * 100000, 1), 0) as crude_rate_per_100000,
0 as standardised_rate_per_100000,
0 as CI
from b_rates

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.eth_ctos_rates;
CREATE TABLE IF NOT EXISTS $db_output.eth_ctos_rates AS
-- CREATE OR REPLACE GLOBAL TEMPORARY VIEW eth_ctos_rates AS 

SELECT * FROM a_rates_final

UNION ALL

SELECT * FROM b_rates_final

UNION ALL

SELECT * FROM c_rates_final