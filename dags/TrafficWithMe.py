from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import numpy as np
import requests 
import csv
from pathlib import Path
from mlxtend.frequent_patterns import apriori
from datetime import datetime, timedelta
import glob
import psycopg2 as pg


default_args ={
    'owner': 'Michael',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

csv_path = Path("/opt/airflow/data/traffic.csv")

def ExtractTraffic(**kwargs):
    response = requests.get('http://www.mapquestapi.com/traffic/v2/incidents?key=A4Id5QmHAbiVZUPGkfMG82ZpAz6T86nt&boundingBox=-6.37477,106.9804,-6.05441,106.6651')
    json_dict = response.json()
    return json_dict
    
def CleanTheTraffic(**kwargs):
    ti = kwargs['ti']
    json_dict = ti.xcom_pull(task_ids='Extract')

    df_traffic = pd.json_normalize(json_dict, 'incidents')
    df_traffic = df_traffic.loc[(df_traffic['shortDesc'] == 'Incident') | (df_traffic['shortDesc'] == 'Accident')]
    df_traffic['startTime'] = pd.to_datetime(df_traffic['startTime'])
    df_traffic['date_time'] = df_traffic['startTime'].apply(lambda x: x.replace(minute=0, second=0))
    df_traffic.drop(['type','startTime', 'eventCode', 'lat', 'lng', 'impacting', 'delayFromFreeFlow', 'delayFromTypical', 'iconURL', 'distance', 'endTime'], axis=1, inplace=True)

    try:
        print(df_traffic)
        df_traffic.to_csv(csv_path, mode='a', index=False, header=False)
        return True
    except OSError as e:
        print(e)
        return False

def LoadToDB():
    conn = None
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='airflow_docker-postgres-1' password='airflow'"
        )
        path = "/opt/airflow/data/traffic.csv"
        glob.glob(path)
        for fname in glob.glob(path):
            fname = fname.split('/')
            csvname = fname[-1]
            csvname = csvname.split('.')
            tablename = str(csvname[0])

            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS traffic_""" + tablename + """ (
                    id varchar(50),
                    severity varchar(50),
                    shortDesc varchar(50),
                    fullDesc varchar(255),
                    date_time varchar(255)
                );
                """
            )
            conn.commit()
            
        # insert each csv row as a record in our database
        with open('/opt/airflow/data/traffic.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO traffic_traffic VALUES (%s, %s, %s, %s, %s)",
                    row
                )
        conn.commit()

    except Exception as error:
        print(error)

    

with DAG(
    dag_id = 'TrafficGet',
    default_args=default_args,
    description = 'TrafficGet',
    start_date = datetime.now(),
    schedule_interval = '@hourly'

) as dag:
    task1 = PythonOperator(
        task_id = 'Extract',
        python_callable=ExtractTraffic
        # op_kwargs={
        #     'name': 'akjsdf'
        # }
    )

    task2 = PythonOperator(
        task_id = 'Transform',
        python_callable=CleanTheTraffic
    )

    task3 = PythonOperator(
        task_id = 'Load',
        python_callable=LoadToDB
    )

task1 >> task2 >> task3