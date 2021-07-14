#!/usr/bin/env python
# coding: utf-8

# Step 1: Configuration

import pandas as pd
import os
import numpy as np
import glob
from multiprocessing import Pool
from datetime import datetime

def getPgEngine(pgLogin=None):
    """
    returns an engine object that connects to postgres
    From the docs: You only need to create the engine once
                   per database you are connecting to
    """
    from sqlalchemy import create_engine
    if pgLogin is None:
        pgLogin = getPgLogin()
    thehost = '' if 'host' not in pgLogin else pgLogin['host']+':5432'
    if 'pw' not in pgLogin:
        if 'requirePassword' in pgLogin and pgLogin['requirePassword']:
            import getpass
            pw = getpass.getpass('Enter postgres password for {}: '.format((pgLogin['user'])))
        else:
            pw = ''
        pgLogin.update({'pw': pw})

    engine = create_engine('postgresql://%s:%s@%s/%s' % (pgLogin['user'], pgLogin['pw'], thehost, pgLogin['db']))

    return engine

def avg_age_cal(agelst,df,newcol):
    i = 0
    age = pd.DataFrame()
    while i <= len(agelst) - 1:
        age[str(i)] = agelst[i] * df.iloc[:, i+3] / df.iloc[:, 2]
        i += 1
    df[newcol] = age.sum(axis=1)
    return df

def sql_process (eng, schema, trips, gis, census):
    cursor = eng.connect()
    cursor.execute(
    '''
    DROP TABLE IF EXISTS ''' + schema + '''.temp1;
    CREATE TABLE ''' +  schema + '''.temp1 AS
    SELECT endhour, weekday, timeperiod, bg, agg_clazz, agg_edge_id, n,
    mean_resolution * n AS pingtime_mean_SUM,
    sum_weights,
    n_cruise,
    n_cruise_weighted,
    mean_cruise_dist * n AS cruise_dist_SUM,
    mean_cruise_dist_weighted * n AS cruise_dist_weighted_SUM,
    mean_cruise_time * n AS cruise_time_SUM,
    mean_cruise_time_weighted * n AS cruise_time_weighted_SUM
    FROM ''' + schema + '''.''' + '"' + trips + '"' + 
    ''' ORDER BY bg;

    DROP TABLE IF EXISTS ''' + schema + '''.temp2;
    CREATE TABLE ''' + schema + '''.temp2 AS
    SELECT bg, endhour, weekday, timeperiod,
            SUM(n) AS n,
            SUM(pingtime_mean_SUM * n) / SUM(n) AS mean_resolution,
            SUM(sum_weights) AS sum_weights,
            SUM(n_cruise * n) / SUM(n) AS cruise_rate,
            SUM(n_cruise_weighted * n) / SUM(n) AS cruise_rate_weighted,
            SUM(cruise_dist_SUM * n) / SUM(n) AS mean_cruise_dist,
            SUM(cruise_dist_weighted_SUM * n) / SUM(n) AS mean_cruise_dist_weighted,
            SUM(cruise_time_SUM * n) / SUM(n) AS mean_cruise_time,
            SUM(cruise_time_weighted_SUM * n) / SUM(n) AS mean_cruise_time_weighted
    FROM ''' + schema + '''.temp1
    GROUP BY bg, endhour, weekday, timeperiod;
    DROP TABLE IF EXISTS ''' +  schema + '''.temp1;

    DROP TABLE IF EXISTS ''' + schema + '.' + trips + '''_variable;
    CREATE TABLE ''' +  schema + '.' + trips + '''_variable AS
    SELECT t1.*, t2.r_den, t2.j_den, t2.p_den, t2."mean_AADT", t2."D2A_EPHHM", t2."D2C_TRPMX1", t3.avg_age_weighted
    FROM ''' +  schema + '''.temp2 t1
    INNER JOIN ''' +  schema + '.' + '"' + gis + '"' + '''t2
    ON t1.bg = t2.''' + '"GEOID10"' 
    '''INNER JOIN ''' + schema + '.' + '"' + census + '"' + ''' t3
    ON t1.bg = t3.geoid10;
    DROP TABLE IF EXISTS ''' + schema + '''.temp2;
    ''')
    cursor.close()
    return


# Step 2: Get Connection and Input Data

## db = "mm_test"
## schema = "parking"
db = input ("Enter database name: ")
schm = input ("Enter schema name: ")

pgInfo = {'db': db,
          'schema': schm,    # schema with GPS traces and streets table
          'user': 'postgres',
          'host': 'localhost',
          'requirePassword': False  # Prompt for password? Normally, False for localhost
          }
engine = getPgEngine(pgInfo)


basepath = input ("Enter input directory: ")
input_trip = input ("Enter aggregated trip files folder: ")
input_city = input ("Enter the city name: ")
input_gis = input_city + "_gis_variables"
input_census_var = input_city + "_census_variables"
input_gis_sld = input ("Enter SLD file name with extension: ")
input_gis_aadt = input ("Enter AADT file name with extension: ")
input_gis_p = input ("Enter Parking Meters file name with extension: ")
input_census = input ("Enter Census file name with extension: ")


# Step 3: Prepare GIS Variables
## prepare gis variables in ArcGIS and exported as xlsx file, then use the code below to import GIS variables into the pgadmin DB
## SLD: https://www.epa.gov/smartgrowth/smart-location-mapping#SLD
## AADT: https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles.cfm (cannot download, have to use ArcGIS to operate geoprocessing to get avg_DDOT per BG)
## Parking meters download link (optional), according to each city's data availability

## 3.1 process mean_AADT and p_counts per bg in ArcGIS Pro
### - for p_count, use spatial join intersect
### - for mean_AADT, use spatial join intersect, and set merge rule as mean for aadt attribute

## 3.2 use code to process SLD, AADT, and p_den

gis_sld = os.path.join (basepath,input_gis_sld)
gis_aadt = os.path.join (basepath,input_gis_aadt)
gis_pcnt = os.path.join (basepath,input_gis_p)

## get geoid10 list of the target city
df_geoid10 = pd.read_csv(gis_aadt, usecols = ['GEOID'])
## slice target city's useful data from SLD dataset
df_sld = pd.read_csv(gis_sld, usecols = ['GEOID10','Ac_Unpr', 'D1A', 'D1C', 'D2A_EPHHM', 'D2C_TRPMX1'])
df_sld = df_sld[df_sld['GEOID10'].isin(df_geoid10['GEOID'])]
df_aadt = pd.read_csv(gis_aadt, usecols = ['GEOID','aadt'])
df_pcnt = pd.read_csv(gis_pcnt, usecols = ['GEOID','Join_Count'])

##  merge three gis df together
df_gis = df_sld.merge(df_aadt, how = "inner", left_on = ["GEOID10"] , right_on = ["GEOID"]).merge(df_pcnt, how = "inner", left_on = ["GEOID10"] , right_on = ["GEOID"])
## compute p_den and clean attributes
df_gis['p_den'] = df_gis['Join_Count']/df_gis['Ac_Unpr']
df_gis = df_gis.rename(columns={'D1A': 'r_den', 'D1C': 'j_den', 'aadt': 'mean_AADT'}, errors="raise")
df_gis = df_gis.drop(['GEOID_x','Join_Count','GEOID_y','Ac_Unpr'], axis = 1)
df_gis.to_sql(input_gis, engine, schema = schm, if_exists = 'replace', index = False)


# Step 4: Perpare Census Variables
## 2019 Census B25034 link: https://data.census.gov/cedsci/table?q=B25034&tid=ACSDT1Y2019.B25034
## Use the code below to prepare Census Data as additional variables, for now only one attributes which is building average ages

# read and process Hsg Year Built data
path_census = os.path.join (basepath,input_census)
df_census = pd.read_csv(path_census, header = 1, usecols = [0,1,2,4,6,8,10,12,14,16,18,20,22], dtype = {'id' : pd.StringDtype()})
df_geoid = pd.to_numeric(df_census['id'].str[-12:]) # convert geoid back to int64 type for future join
df_revised_total = df_census.iloc[:, 2] - df_census.iloc[:, -1] # calculate revised total by substracting the number of units in 1939 or earlier column
df_census.update(df_geoid)
df_census = df_census[df_census['id'].isin(df_geoid10['GEOID'])]
df_census['Estimate!!Total:'].update(df_revised_total) # update the total number of unit column
df_census = df_census.drop(columns = ['Estimate!!Total:!!Built 1939 or earlier']) # drop the 1939 or earlier column


## calculate average age values as a list for next step
headers = df_census.columns
baseyear = 2019
ls = []
ls_yr = []
for s in headers[2:]:
    s = s.split()
    for i in s:
        ls.append(i)
for i in ls:
    if i.isnumeric():
        ls_yr.append(int(i))
ls_yr.insert(1,baseyear)
i = 0
i_max = len(ls_yr) - 1
ls_age = []
while i < i_max:
    age = baseyear - (ls_yr[i] + ls_yr[i+1])/2
    ls_age.append(age)
    i += 2

df_census = avg_age_cal(ls_age, df_census, 'avg_age_weighted')
df_census = df_census.rename(columns={'id': 'geoid10'}, errors="raise")
df_census = df_census[['geoid10','avg_age_weighted']]
df_census.to_sql(input_census_var, engine, schema = schm, if_exists = 'replace', index = False)


# Main Process
path_trip = os.path.join (basepath,input_trip)

for fn in os.listdir(path_trip):
    path_fn = os.path.join (path_trip,fn)
    pd.read_csv(path_fn).to_sql(fn[:-4], engine, schema = schm, if_exists = 'replace', index = True)
    sql_process(engine, schm, fn[:-4], input_gis, input_census_var)
    pd.read_sql_query('select * from ' + schm +'.' + fn[:-4] +'_variable',con=engine).to_csv (os.path.join (basepath,fn[:-4] + '_variable.csv'), index = False)