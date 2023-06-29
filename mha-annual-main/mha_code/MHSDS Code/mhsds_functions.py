# Databricks notebook source
from dataclasses import dataclass, field
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta

# COMMAND ----------

def str2dt(date):
  '''
  This function converts a string into a datetime in the format "YYYY-MM-DD"
  '''  
  date_format = '%Y-%m-%d'
  date_conv = datetime.strptime(date, date_format)
  
  return date_conv

def dt2str(date):
  '''
  This function converts a datetime to a string in the format "YYYY-MM-DD"
  '''  
  date_format = '%Y-%m-%d'
  date_conv = date.strftime(date_format)
  
  return date_conv
  
def get_end_month_id(rp_enddate, status):
  '''
  This function gets the relevant MHSDS UniqMonthID using the header table
  '''  
  tab = spark.sql(f"select distinct UNIQMONTHID from $db_source.mhs000header where reportingperiodenddate = '{rp_enddate}'")
  end_month_id = tab.first()["UNIQMONTHID"]
  
  return end_month_id

def get_start_month_id(rp_enddate, status):
  '''
  This function gets the relevant MHSDS UniqMonthID using the header table
  '''
  end_month_id = get_end_month_id(rp_enddate, status)
  start_month_id = int(end_month_id) - 11
  
  return start_month_id

def get_1m_startdate(rp_enddate, status):
  '''
  This function gets the 1-month start date according to the pub month and submission window
  ''' 
  #expected quarter start date from rp_enddate
  rp_enddate_dt = str2dt(rp_enddate)
  exp_1m_startdate = rp_enddate_dt.replace(day=1) ##first day   
  exp_1m_startdate = dt2str(exp_1m_startdate)
  
  return exp_1m_startdate

def get_qtr_startdate(rp_enddate, status):
  '''
  This function gets the quarter start date according to the pub month and submission window
  '''  
  #expected quarter start date from rp_enddate
  rp_enddate_dt = str2dt(rp_enddate)
  exp_qtr_enddate = rp_enddate_dt - relativedelta(months=2) ##minus 2 months
  exp_qtr_startdate = exp_qtr_enddate.replace(day=1) ##first day 
  exp_qtr_startdate = dt2str(exp_qtr_startdate)
  
  return exp_qtr_startdate

def get_12m_startdate(rp_enddate, status):
  '''
  This function gets the quarter start date according to the pub month and submission window
  '''  
  #expected 12m start date from rp_enddate
  rp_enddate_dt = str2dt(rp_enddate)
  exp_12m_enddate = rp_enddate_dt - relativedelta(months=11) ##minus 11 months
  exp_12m_startdate = exp_12m_enddate.replace(day=1) ##first day 
  exp_12m_startdate = dt2str(exp_12m_startdate)
  
  return exp_12m_startdate

def get_financial_yr_start(rp_enddate, status):
  '''
  This function gets the financial year start date according to the pub month and submission window 
  it uses the $ref_database.calendar_financial_year to get expected financial year
  '''
  #gets expected 12m start date from rp_enddate
  rp_startdate = get_12m_startdate(rp_enddate, status)
  #searches financial year start from rp_startdate and rp_enddate
  fy = spark.sql(f"select START_DATE from $ref_database.calendar_financial_year where START_DATE between '{rp_startdate}' and '{rp_enddate}'")
  fy_value = fy.first()["START_DATE"]
  financial_year_start = dt2str(fy_value)
  
  return financial_year_start

def get_df_name(df):
  name = [x for x in globals() if globals()[x] is df][0]
  return name


# COMMAND ----------

@dataclass
class MHRunParameters:
  db_output: str
  db_source: str
  status: str  
  rp_enddate: DateType()  
  end_month_id: int = field(init=False)
  start_month_id: int = field(init=False)  
  rp_startdate_1m: DateType() = field(init=False)  
  rp_startdate_qtr: DateType() = field(init=False)  
  rp_startdate_12m: DateType() = field(init=False)
  financial_year_start: DateType() = field(init=False)
  $ref_database: str = "$ref_database"
   
  def __post_init__(self):
    self.end_month_id = get_end_month_id(self.rp_enddate, self.status)
    self.start_month_id = get_start_month_id(self.rp_enddate, self.status)
    self.rp_startdate_1m = get_1m_startdate(self.rp_enddate, self.status)
    self.rp_startdate_qtr = get_qtr_startdate(self.rp_enddate, self.status)
    self.rp_startdate_12m = get_12m_startdate(self.rp_enddate, self.status)
    self.financial_year_start = get_financial_yr_start(self.rp_enddate, self.status)
  
  def as_dict(self):
    json_dump = json.dumps(self, sort_keys=False, default=lambda o: o.__dict__)
    return json.loads(json_dump)  