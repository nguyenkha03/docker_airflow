from airflow.decorators import task
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

@task
def load_to_stage_uscounties_task():
    # read uscounties.csv
    data = pd.read_csv('data/source_files/uscounties.csv')

    hook = MySqlHook(mysql_conn_id='mysql_conn_id')
    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute('TRUNCATE TABLE datawarehouse.uscounties_stage;')

    insert_data = []
    for _, row in data.iterrows():
        insert_data.append((
            row['county_fips'], row['county'], row['county_ascii'], row['county_full'], 
            row['state_id'], row['state_name'], row['lat'], row['lng'], row['population']
        ))

    insert_query = """
        INSERT INTO datawarehouse.uscounties_stage (county_fips, county, county_ascii, county_full, state_id, state_name, lat, lng, population)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(insert_query, insert_data)

    connection.commit()
    cursor.close()
    connection.close()

@task
def load_to_stage_aqi_task(ti=None):
    date = ti.execution_date
    year = date.year
    source_path = f'data/source_files/daily_aqi_by_county_{year}.csv'
    data = pd.read_csv(source_path)

    hook = MySqlHook(mysql_conn_id='mysql_conn_id')
    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute('TRUNCATE TABLE datawarehouse.state_aqi_stage;')

    insert_data = []
    for _, row in data.iterrows():
        insert_data.append((
            row['state_name'], row['county_name'], row['state_code'], row['county_code'],
            row['date'], row['aqi'], row['category'], row['defining_parameter'],
            row['defining_site'], row['number_of_sites_reporting'], row['created'], row['last_updated']
        ))

    insert_query = """
        INSERT INTO datawarehouse.state_aqi_stage (state_name,county_name,state_code,county_code,date,aqi,category,defining_parameter,defining_site,number_of_sites_reporting,created,last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(insert_query, insert_data)

    connection.commit()
    cursor.close()
    connection.close()