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

csv_path = Path("/opt/airflow/data/weather.csv")

def GetDate():
    global startDate
    global endDate
    start = datetime.date(datetime.now())
    end = datetime.date(datetime.now()) + timedelta(days=1)
    startDate = start.strftime("%d-%m-%Y")
    endDate = end.strftime("%d-%m-%Y")
    print(startDate)
    print(endDate)

def ExtractWeather(**kwargs):
    GetDate()
    response = requests.get('http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=a30d18f3420248b19a973523223011&q=Jakarta&format=json&date=' + startDate + '&enddate=' + endDate + '&tp=1')
    json_dict = response.json()
    return json_dict
    
def CleanTheWeather(**kwargs):
    ti = kwargs['ti']
    json_dict = ti.xcom_pull(task_ids='Extract')
    df_weather = pd.DataFrame(json_dict['data']['weather'][0]['hourly'])
    df_weather['date'] = json_dict['data']['weather'][0]['date']

    for item in range(1,2):
        df_weather['weatherValue'] = df_weather['weatherDesc'].apply(lambda x: x[0]['value'])
        temp = pd.DataFrame(json_dict['data']['weather'][item]['hourly'])
        temp['date'] = json_dict['data']['weather'][item]['date']
        df_weather = pd.concat([df_weather, temp], ignore_index=True)
    df_weather['time'] = df_weather['time'].apply(lambda x: x.zfill(4))
    # keep only first 2 digit (00-24 hr) 
    df_weather['time'] = df_weather['time'].str[:2]
    # convert to pandas datetime
    df_weather['date_time'] = pd.to_datetime(df_weather['date'] + ' ' + df_weather['time'])
    # keep only interested columns
    col_to_keep = ['date_time','weatherValue']
    df_weather = df_weather[col_to_keep]
    df_weather = df_weather[df_weather['weatherValue'].notna()]

    try:
        print(df_weather)
        df_weather.to_csv(csv_path, mode='a', index=False, header=False)
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
        path = "/opt/airflow/data/weather.csv"
        glob.glob(path)
        for fname in glob.glob(path):
            fname = fname.split('/')
            csvname = fname[-1]
            csvname = csvname.split('.')
            tablename = str(csvname[0])

            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_""" + tablename + """ (
                    date_time varchar(50),
                    weatherValue varchar(50)
                );
                """
            )
            conn.commit()
            
        # insert each csv row as a record in our database
        with open('/opt/airflow/data/weather.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO weather_weather VALUES (%s, %s)",
                    row
                )
        conn.commit()

    except Exception as error:
        print(error)

    

with DAG(
    dag_id = 'WeatherGet',
    default_args=default_args,
    description = 'WeatherGet',
    start_date = datetime.now(),
    schedule_interval = '@daily'

) as dag:
    task1 = PythonOperator(
        task_id = 'Extract',
        python_callable=ExtractWeather
        # op_kwargs={
        #     'name': 'akjsdf'
        # }
    )

    task2 = PythonOperator(
        task_id = 'Transform',
        python_callable=CleanTheWeather
    )

    task3 = PythonOperator(
        task_id = 'Load',
        python_callable=LoadToDB
    )

task1 >> task2 >> task3