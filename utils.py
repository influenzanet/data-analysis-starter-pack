import numpy as np
import geopandas as gpd
import pgeocode
import pandas as pd
from collections import Counter, defaultdict
import os, sys, glob, re, datetime
from datetime import date, timedelta

#Convert weeks with formats like 2014-2 to proper format 2014-02
def fix_yearweek(date_string):
    year, week =  map(lambda x: int(x), date_string.split('-'))
    fixed = "{0}-{1:02d}".format(year, week)
    
    assert re.match("\d{4}-\d{2}$", fixed)
    return fixed

def weekMinus(week, minusweek):
    #week is a string 'yyyy-w'
    yr, wk = map(lambda x: int(x), week.split('-'))
    mid_date = getMiddleDayOfWeek(yr, wk)
    res_date = mid_date - timedelta(days=7*minusweek)
    isoyear, isoweek, isoday = res_date.isocalendar()
    return str(isoyear) + '-' + str(isoweek)

def getMiddleDayOfWeek(year, week):
    d = datetime.date(year,1,1)
    if(d.weekday()>3):
        d = d+timedelta(7-d.weekday())
    else:
        d = d - timedelta(d.weekday())
    dlt = timedelta(days = (week-1)*7)
    return d + dlt + timedelta(days=3)

def weekPlusOne(week):
    #week is a string 'yyyy-w'
    yr, wk = map(lambda x: int(x), week.split('-'))
    mid_date = getMiddleDayOfWeek(yr, wk)
    res_date = mid_date + timedelta(days=7)
    isoyear, isoweek, isoday = res_date.isocalendar()
    return str(isoyear) + '-' + str(isoweek)

def days_difference(date1, date2):
    y1,m1,d1 = map(lambda x: int(x), date1.split('-'))
    y2,m2,d2 = map(lambda x: int(x), date2.split('-'))
    delta_days = (datetime.date(y1,m1,d1) - datetime.date(y2,m2,d2)).days
    return delta_days

def get_onset_date( submission_date, symptoms_date, fever_date ):
    onset_date = submission_date
    if pd.notnull(symptoms_date): 
        if 0 <= days_difference(submission_date, symptoms_date) <= 15:
            onset_date = symptoms_date
    elif pd.notnull(submission_date) and pd.notnull(fever_date):
         if 0 <= days_difference(submission_date, fever_date) <= 15:
            onset_date = fever_date    
    return onset_date

def get_week_of_activity(global_id, submission_weeks):
    activity_weeks = []
    for week in submission_weeks: 
        wk_start, wk_end = fix_yearweek(weekMinus(week, 2)), fix_yearweek(weekMinus(week, -2))
        wk = wk_start
        while(wk <= wk_end):
            activity_weeks.append(wk)
            wk = fix_yearweek(weekPlusOne(wk))
    return sorted(set(activity_weeks))

def yearweek_to_ts(x):
    year = x.split('-')[0]
    week = x.split('-')[1]
    date = "{}-{}-1".format(year, week)
    dt = datetime.datetime.strptime(date, "%Y-%W-%w")
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

def get_ILI_ECDC(row):
    ILI=False
    if row.symptoms==True:
        if row['Sudden onset']==True or row['Sudden fever']==True: #0 = sudden onset
            if row.Fever==True or row.Chills==True or row['Malaise']==True or row.Headache==True or row['Muscle/joint pain']==True:
                if row['Sore throat']==True or row.Cough==True or row['Shortness of breath']==True:
                    ILI=True  
    return ILI

def get_ARI(row):
    ARI=False
    if row.symptoms==True:
        if row['Sudden onset']==0.0: #0 = sudden onset
            if row.Cough==True or row['Sore throat']==True or row['Shortness of breath']==True or row['Runny or blocked nose']==True:
                ARI=True  
    return ARI

###
# Return previous week (es: lastweek(datetime.datetime.strptime('18112019', "%d%m%Y").date()) -> 2019-46)
###
def lastweek(today=date.today()):
    for i in range(7):
        x = str((today - timedelta(days=i)).isocalendar()[0])+'-'+str((today - timedelta(days=i)).isocalendar()[1])
        d1 = str(today.isocalendar()[0])+'-'+str(today.isocalendar()[1])
        if x!=d1:
              return fix_yearweek(x)

def unite(x):
    #print(x)
    aa=np.where(x)[0]
    if len(aa)>0:
        return int(aa[0])
        print(aa)
    else: return np.nan
    
    
def translate(entry):
    if entry=='f':
        entry=False
    elif entry=='FALSE':
        entry=False
    elif entry=='t':
        entry=True
    elif entry=='TRUE':
        entry=True
    return entry

def get_age(x):
    subm=int(x.intake_submission[:4])
    if any([x.version=='22-12-2', x.version=='21-11-1', x.version=='23-10-1']):
        new_year= int(datetime.datetime.fromtimestamp(int(x.Q2)).strftime('%Y'))
        if subm - new_year < 0:
            print(x.Q2)
        return subm - new_year
    
    else:
        if '/' in x.Q2 or '-' in x.Q2:
            year=int(x.Q2[:4])
            if subm - year < 0:
                print(x.Q2)
            return subm - year
        
def get_age_class(x):
    if x<18 and x>0:
        return '<18'
    elif x>=18 and x<=40:
        return '18-40'
    elif x>40 and x<=65:
        return '41-65'
    elif x>65:
        return '>65'
    else: return 'nan'

def occupation(x):
    if str(x)!='nan':
        x=int(x)
        if x==0:
            return 'full_time'
        elif x==1:
            return 'part_time'
        elif x==2:
            return 'self-employed'
        elif x==3:
            return 'student'
        elif x==4:
            return 'homemaker'
        elif x==5:
            return 'unemployed'
        elif x==6:
            return 'on leave'
        elif x==7:
            return 'retired'
        elif x==8:
            return 'other'
    
    
def schooling(x):
    if x==0:
        return 'none'
    elif x==1:
        return 'int_school'
    elif x==2:
        return 'high_school'
    elif x==3:
        return 'bachelor'
    elif x==4:
        return 'master_phd'
    elif x==5:
        return 'student'
    
def get_edu(x):
    if str(x)=='None' or str(x)=='none' or x==np.nan or x=='student':
        return 'elementary'
    elif x=='int_school' or x=='high_school':
        return 'secondary'
    elif x=='master_phd' or x=='bachelor':
        return 'higher'

def change_regname(x):
    if x=='Emilia-Romagna':
        return 'Emilia-Romagna' 
    elif x=="Valle d'Aosta/Vallée d'Aoste":
        return "Valle d'Aosta"
    elif x=="Valle d'Aosta / Vallée d'Aoste":
        return "Valle d'Aosta"
    elif x=='Trentino-Alto Adige/Südtirol':
        return 'Trentino-Alto Adige'
    elif x=='Trentino-Alto Adige / Südtirol':
        return 'Trentino-Alto Adige'
    elif x=='Trentino Alto Adige / Südtirol':
        return 'Trentino-Alto Adige'
    elif x=='Abruzzi':
        return 'Abruzzo'
    elif x=="Valle D'Aosta":
        return "Valle d'Aosta"
    else: return x

