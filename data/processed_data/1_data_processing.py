# -*- coding: utf-8 -*-
#%%
import pandas as pd
import datetime
import requests
#import json
import csv

#%%
"""GET WEATHER DATA USING API (already done)"""

#%%
"""
fips_list = [str(x).zfill(2) for x in range(1,61)]
for x in ['03', '07', '14', '43', '52', '57', '58', '59']:
    fips_list.remove(x)
fips_list  

#%%

#csv_file_path = r'C:\Users\virig\OneDrive\Documents\AGH\ED\dane\meteo\stations.csv'
csv_file_path = 'stations.csv'

#column_headers = list(data[0].keys())
column_headers = [
    'elevation',
    'mindate',
    'maxdate',
    'latitude',
    'name',
    'datacoverage',
    'id',
    'elevationUnit',
    'longitude']

with open(csv_file_path, 'w', newline='') as csv_file:
    # Create a CSV writer object
    csv_writer = csv.DictWriter(csv_file, fieldnames=column_headers)
    
    # Write the column headers to the CSV file
    csv_writer.writeheader()
    
#%%
for fips in fips_list:
    
    offset=0
    empty_response = False
    
    while not empty_response:
        url=f'https://www.ncei.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:{fips}&datacategoryid=TEMP&startdate=2010-01-01&enddate=2022-12-31&limit=1000&offset={offset}'
        headers = {'token': 'geenLBYxrnMaVJmsIXldbsCqTQNHfSpH'}
    
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            json_response = response.json()  # Convert response to JSON
            
            if not json_response:
                empty_response = True
            else:
                data = json_response['results']

                with open(csv_file_path, 'a', newline='') as csv_file:
                    # Create a CSV writer object
                    csv_writer = csv.DictWriter(csv_file, fieldnames=column_headers)

                    # Write each row of data to the CSV file
                    for row in data:
                        csv_writer.writerow(row)
                
                offset += 1000

    print(f'Data exported for FIPS {fips}')

#%%
# create file
#csv_file_path = r'C:\Users\virig\OneDrive\Documents\AGH\ED\dane\meteo\data_gsom.csv'
csv_file_path = 'data_gsom.csv'

column_headers = ['date', 'datatype', 'station', 'attributes', 'value']

with open(csv_file_path, 'w', newline='') as csv_file:
    # Create a CSV writer object
    csv_writer = csv.DictWriter(csv_file, fieldnames=column_headers)
    
    # Write the column headers to the CSV file
    csv_writer.writeheader()
    
#%%
for year in range(2010, 2023):
    for fips in fips_list:
        
        offset=0
        empty_response = False
        
        while not empty_response:
            url=f'https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&locationid=FIPS:{fips}&datatypeid=TMAX,TMIN,TAVG&startdate={year}-01-01&enddate={year}-12-31&limit=1000&offset={offset}'
            headers = {'token': 'geenLBYxrnMaVJmsIXldbsCqTQNHfSpH'}
        
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                json_response = response.json()  # Convert response to JSON
                
                if not json_response:
                    empty_response = True
                else:
                    data = json_response['results']
    
                    with open(csv_file_path, 'a', newline='') as csv_file:
                        # Create a CSV writer object
                        csv_writer = csv.DictWriter(csv_file, fieldnames=column_headers)
    
                        # Write each row of data to the CSV file
                        for row in data:
                            csv_writer.writerow(row)
                    
                    offset += 1000
    
        print(f'Data exported for year {year}, FIPS {fips}')

"""
#%%
"""FILTER STATIONS WITH FULL DATA COVERAGE ONLY"""

#%%
df_stations = pd.read_csv('stations.csv')
df_stations.head()

#%%
#stations_cols = df_stations.columns.tolist()
stations_cols = [
    'id', 
    'name',    
    'latitude', 
    'longitude', 
    'elevation',
    'elevationUnit',    
    'mindate',
    'maxdate',
    'datacoverage',
 ]

#%%
df_stations = df_stations[stations_cols]

#%%
# rename some columns
df_stations.rename(columns={'id': 'station_id', 
                            'name': 'station_name',
                            'latitude': 'station_lat', 
                            'longitude': 'station_lon',
                            'elevation': 'station_el'}, inplace=True)

#%%
df_stations_full = df_stations.loc[
                            (df_stations['mindate'] <= '2009-12-01')
                            & (df_stations['maxdate'] >= '2023-01-01')]
# miesiąc wczesniej

#%%
df_stations_full.to_csv('stations_full_coverage.csv', index=True, index_label='station_idx')

#%%
"""PROCESS WEATHER DATA """

#%% merge data
# meteo data
cols_weather = ['field_1', 
              'LOCAL_DATE', 
              'LOCATION_L', 
              'LOCATION_1', 
              'station_id', 
              'station_la', 
              'station_lo', 
              'station_el', 
              'zones']

#%%
df_data_gsom = pd.read_csv('data_gsom.csv', header=0)
df_data_gsom.shape    

#%%
df_data_gsom['YYYY-mm'] = pd.to_datetime(df_data_gsom['date']).dt.strftime('%Y-%m')

#%%
# rename some columns
df_data_gsom.rename(columns={'station': 'station_id'}, inplace=True)
#%%
df_weather = df_data_gsom.pivot_table(index=['YYYY-mm', 'station_id'], columns='datatype', values='value')

df_weather.reset_index(inplace=True)

#%%
# add station data to weather data
df_weather = pd.merge(df_weather, df_stations[['station_lat', 'station_lon', 'station_id']], how='left', on='station_id')


#%%
"""PROCESS ACCIDENT AND INCIDENT DATA"""

#%%
paths = [
    "accident_gravity_reporting_regulated_jul2020_present.txt",
    "accident_hazardous_liquid_jan2010_present.txt",
    "incident_gas_distribution_jan2010_present.txt",
    "incident_gas_transmission_gathering_jan2010_present.txt",
    "incident_liquefied_natural_gas_jan2011_present.txt",
    "incident_type_r_reporting_regulated_gas_gathering_may2022_present.txt"
]

data_sources = [
    'gravity',
    'hazardous_liquid',
    'gas_distribution',
    'gas_transmission',
    'liquefied_natural_gas',
    'gas_gathering'
]
#%%
# concatenate tables to one df

df_temp1 = pd.read_csv(
    paths[0]
    , header=0
    , encoding='unicode_escape' 
    , sep='\t'
    #, index_col=['REPORT_NUMBER', 'SUPPLEMENTAL_NUMBER'] 
    , engine='python'
    )
df_temp1['data_source'] = data_sources[0]

df_temp2 = pd.read_csv(
    paths[1]
    , header=0
    , encoding='unicode_escape' 
    , sep='\t'
    #, index_col=['REPORT_NUMBER', 'SUPPLEMENTAL_NUMBER'] 
    , engine='python'
    )
df_temp2['data_source'] = data_sources[1]

df = pd.concat([df_temp1, df_temp2])

for path, data_source in zip(paths[2:], data_sources[2:]):
    df_temp = pd.read_csv(
        path
        , header=0
        , encoding='unicode_escape' 
        , sep='\t'
        #, index_col=['REPORT_NUMBER', 'SUPPLEMENTAL_NUMBER'] 
        , engine='python'
        )
    df_temp['data_source'] = data_sources
    # convert mft to bbls
    df_temp['INTENTIONAL_RELEASE_BBLS'] =df_temp['INTENTIONAL_RELEASE'] * 178.10760668
    df_temp['UNINTENTIONAL_RELEASE_BBLS'] =df_temp['UNINTENTIONAL_RELEASE'] * 178.10760668  
    # drop columns
    df_temp.drop(labels=['INTENTIONAL_RELEASE', 'UNINTENTIONAL_RELEASE'], axis=1, inplace=True)    
    df = pd.concat([df, df_temp])

print(df.shape)

#%%
# create columns 'case_date', 'YYYY-mm', 'YYYY-mm_prev'
df2 = df.copy()
df2['LOCAL_DATETIME'] = pd.to_datetime(df2['LOCAL_DATETIME'])
df2['case_date'] = df2['LOCAL_DATETIME'].dt.date
df2['YYYY-mm'] = pd.to_datetime(df2['case_date']).dt.strftime('%Y-%m')
df2['YYYY-mm_prev'] = (pd.to_datetime(df2['YYYY-mm']) - pd.DateOffset(months=1)).dt.strftime('%Y-%m')

df = df2.copy()
#%%
# drop unnamed columns
columns_to_drop = df.columns[df.columns.str.contains('Unnamed')]
df = df.loc[:, ~df.columns.isin(columns_to_drop)]
#%%
# rename some columns
df.rename(columns={'LOCATION_LATITUDE': 'case_lat', 'LOCATION_LONGITUDE': 'case_lon'}, inplace=True)

#%%
# filter out dates outside of our range
# and records with no locations
df = df[
    (df['case_date'] >= datetime.date(2010, 1, 1)) 
    & (df['case_date'] <= datetime.date(2022, 12, 31))
    & (df['case_lat'].notnull())
    & (df['case_lon'].notnull())
    ]

#%%
# reset_index
df.reset_index(inplace=True)
df.rename(columns={'index': 'case_idx'}, inplace=True)
#%%
# export to qgis for further processing
cols_qgis = [
             'case_idx'
             , 'case_date'
             , 'case_lat'
             , 'case_lon'
             , 'rlsd_type'
]

df_qgis = df[cols_qgis]

df_qgis.rename(columns={'COMMODITY_RELEASED_TYPE': 'rlsd_type'}, inplace=True)
df_qgis.to_csv('df_qgis.csv', index=False)

"""
PROCESSING IN QGIS:
    - assign nearest station to each observation
    - assign climate zone to each observation
    """
#%%
# import processed data
df_qgis_stations_zones = pd.read_csv('df_qgis_stations_zones.csv', header=0)

df_qgis_stations_zones.drop(['zones1_BA_Climate', 'zones2_BA_Climate', 'zones3_BA_Climate'], axis=1, inplace=True)#%%

#%%
# add station and zone data to df

df = pd.merge(df, df_qgis_stations_zones[['case_idx', 'station_id', 'zone']], how='left', on='case_idx')

#%%
# add weather data to df
df = pd.merge(df, df_weather[['YYYY-mm', 'station_id', 'TAVG', 'TMAX', 'TMIN']], how='left', on=['YYYY-mm', 'station_id'])
df = pd.merge(df, df_weather[['YYYY-mm', 'station_id', 'TAVG', 'TMAX', 'TMIN']], how='left', left_on=['YYYY-mm_prev','station_id'], right_on=['YYYY-mm', 'station_id'])

df.drop_duplicates(inplace=True)

#%%
df.rename(columns={
    'YYYY-mm_x': 'YYYY-mm', 
    'TAVG_x': 'TAVG', 
    'TMAX_x': 'TMAX', 
    'TMIN_x': 'TMIN', 
    'YYYY-mm_y': 'YYYY-mm_2', 
    'TAVG_y': 'TAVG_prev', 
    'TMAX_y': 'TMAX_prev', 
    'TMIN_y': 'TMIN_prev'}, 
    inplace=True)

#%%
df_columns = df.columns

#%%
# drop extreme locations
df = df[(df['case_lon'] <= -70) & (df['case_lon'] >= -152)]


#%%
# calculate nans per column and filter out those with more than 0.2 nans

nan_share_per_column = df.isna().sum()/df.shape[0]

cols_20prcnt_nan = nan_share_per_column[nan_share_per_column <= 0.2].index.tolist()
df_20prcnt_nan = df.loc[:, cols_20prcnt_nan]

#%%
# manually selected columns
selected_cols = [
    #'case_idx',
    'data_source',    
    'case_lat',
    'case_lon',
    'case_date',
    #'YYYY-mm',
    #'YYYY-mm_prev', 
    'COMMODITY_RELEASED_TYPE',
    'INTENTIONAL_RELEASE_BBLS',
    'UNINTENTIONAL_RELEASE_BBLS',    
    #'FATALITY_IND',
    'FATAL',
    #'INJURY_IND',
    'INJURE',
    'ON_OFF_SHORE',
    'IGNITE_IND',
    'EXPLODE_IND',
    'NUM_PUB_EVACUATED',
    'FEDERAL',
    'LOCATION_TYPE',
    'CROSSING',
    'ITEM_INVOLVED',
    'MATERIAL_INVOLVED',
    'EST_COST_OPER_PAID',
    'EST_COST_PROP_DAMAGE',
    'EST_COST_EMERGENCY',
    'EST_COST_OTHER',
    'CAUSE',
    'CAUSE_DETAILS',
    'NARRATIVE',
    'SYSTEM_PART_INVOLVED',
    'INCIDENT_AREA_TYPE',
    'PIPE_FACILITY_TYPE',
    'INSTALLATION_YEAR',
    'RELEASE_TYPE',
    'COULD_BE_HCA',
    'ACCIDENT_PSIG',
    'MOP_PSIG',
    #'ACCIDENT_PRESSURE',
    'PIPELINE_FUNCTION',
    'SCADA_IN_PLACE_IND',
    'INVESTIGATION_STATUS',
    'EMPLOYEE_DRUG_TEST_IND',
    'CONTRACTOR_DRUG_TEST_IND',
    'zone',
    'TAVG',
    #'TMAX',
    #'TMIN',
    'TAVG_prev',
    #'TMAX_prev',
    #'TMIN_prev'
]

#%%
df2 = df.loc[:, selected_cols].reset_index(drop=True)

#%%
mapping = {'YES': 1, 'NO': 0}

cols_binary = [
    'IGNITE_IND',
    'EXPLODE_IND',
    'FEDERAL',
    'CROSSING',
    'COULD_BE_HCA',
    'SCADA_IN_PLACE_IND',
    'EMPLOYEE_DRUG_TEST_IND',
    'CONTRACTOR_DRUG_TEST_IND',]

#%%
df2[cols_binary] = df2[cols_binary].map(lambda x: mapping.get(x,x))

#%%
df2['case_date'] = pd.to_datetime(df2['case_date'])

#%%
# calculate installation age
df2['INSTALLATION_YEAR'].replace('UNKNOWN', np.nan, inplace=True)
df2['INSTALLATION_YEAR'] = pd.to_datetime(df2['INSTALLATION_YEAR'])

df2['inst_age_in_days'] = (df2['case_date'] - df2['INSTALLATION_YEAR']).dt.days
df2.drop(labels='INSTALLATION_YEAR', axis=1, inplace=True)

#%%
df2['TAVG'] = (df2['TAVG'] + df2['TAVG_prev']) / 2
df2.drop(labels='TAVG_prev', axis=1, inplace=True)

#%%
df2['accident_pressure_as_%_mop_psig'] = df2['ACCIDENT_PSIG'] / df2['MOP_PSIG'] * 100
df2.drop(labels='ACCIDENT_PSIG', axis=1, inplace=True)

#%%
df2.to_csv('processed_data.csv', index=True, index_label='case_idx')

#%%
