import pandas as pd
from airflow.decorators import task

@task
def transform_aqi_csv_task(ti=None):
    date = ti.execution_date
    year = date.year
    source_path = f'data/source_files/daily_aqi_by_county_{year}.csv'
    data = pd.read_csv(source_path)

    
    # Change the header names
    data.columns = ['state_name', 'county_name', 'state_code', 'county_code', 'date', 'aqi', 'category', 'defining_parameter', 'defining_site', 'number_of_sites_reporting']

    data['created'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    data['last_updated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # rewrite the csv file
    data.to_csv(source_path, index=False)
