from datetime import datetime
from airflow.decorators import task
import requests
import zipfile
import os

@task
def extract_data_task(ti=None):
        date = ti.execution_date
        year = date.year
        url = f'https://aqs.epa.gov/aqsweb/airdata/daily_aqi_by_county_{year}.zip'
        response = requests.get(url)
        local_zip_path = f'data/zip_files/daily_aqi_by_county_{year}.zip'
        source_path = 'data/source_files'
        
        if response.status_code == 200:
            # Save the downloaded ZIP file to the local path
            with open(local_zip_path, 'wb') as f:
                f.write(response.content)
            print(f"ZIP file downloaded to {local_zip_path}")

            # Make sure the extraction directory exists
            os.makedirs(source_path, exist_ok=True)
            print(f"Extraction directory is {source_path}")

            # Open the local ZIP file and extract using zipfile
            with zipfile.ZipFile(local_zip_path, 'r') as archive:
                archive.extractall(source_path)
                print(f"Extracted ZIP file to {source_path}")

            # Delete the local ZIP file
            os.remove(local_zip_path)
        else:
            raise Exception(f"Failed to download ZIP file. Status code: {response.status_code}")