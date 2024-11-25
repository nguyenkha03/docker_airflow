from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from datetime import datetime

from extract import extract_data_task
from transform import transform_aqi_csv_task
from source_to_stage import load_to_stage_uscounties_task, load_to_stage_aqi_task

@dag(
    dag_id = 'etl',
    schedule_interval = '@yearly',
    start_date = datetime(2023, 1, 1),
    catchup = False
)
def etl():
    start_dummy = DummyOperator(task_id='start')
    end_dummy = DummyOperator(task_id='end')

    with TaskGroup('load') as load:
        with TaskGroup('source_to_stage') as source_to_stage:

            uscounties_file_sensor_task = FileSensor(
                task_id='wait_for_uscounties_file',
                fs_conn_id='fs_default',
                filepath='data/source_files/uscounties.csv',
                poke_interval=10,  # Check every 10 seconds
                timeout=600,       # Timeout after 10 minutes
                mode='poke',       # Poke mode for regular checking
            )

            state_aqi_file_sensor_task = FileSensor(
                task_id='wait_for_state_aqi_file',
                fs_conn_id='fs_default',
                filepath='data/source_files/daily_aqi_by_county_2024.csv',
                poke_interval=10,  # Check every 10 seconds
                timeout=600,       # Timeout after 10 minutes
                mode='poke',       # Poke mode for regular checking
            )

            [uscounties_file_sensor_task, state_aqi_file_sensor_task] >> load_to_stage_uscounties_task() >> load_to_stage_aqi_task()

        source_to_stage

    start_dummy >> extract_data_task()  >> transform_aqi_csv_task() >> load >> end_dummy

etl()