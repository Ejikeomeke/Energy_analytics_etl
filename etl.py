import pandas as pd
import requests
import json
import yaml
from datetime import datetime
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

def connection():
    with open('key.yaml', 'r') as file:
        config = yaml.safe_load(file)

        key = config.get('key')
    return key

def get_data(ti):
  lati = 9.072264
  longi = 7.491302
  API_key = connection()

  source = f"https://api.openweathermap.org/data/2.5/weather?lat={lati}&lon={longi}&appid={API_key}"
  response = requests.get(url=source)
  status = response.status_code
  print(status)
  data = response.json()
  ti.xcom_push(key='extracted_data', value=data)
  return data

def transformation(ti):
  weather_data = []
  raw_data = ti.xcom_pull(key='extracted_data', task_ids='get_data')
  country = raw_data['sys']['country']
  latitude = raw_data['coord']['lat']
  longitude = raw_data['coord']['lon']
  city_name = raw_data['name']
  time_zone = raw_data['timezone']


  date = datetime.fromtimestamp(raw_data['dt']).date().strftime("%Y-%m-%d %H:%M:%S")
  time_of_calculation = datetime.fromtimestamp(raw_data['dt']).time().strftime("%Y-%m-%d %H:%M:%S")
  sunrise_time = datetime.fromtimestamp(raw_data['sys']['sunrise']).time().strftime("%Y-%m-%d %H:%M:%S")
  sunset_time = datetime.fromtimestamp(raw_data['sys']['sunset']).time().strftime("%Y-%m-%d %H:%M:%S")

  cloud = raw_data['clouds']
  temperature_in_degree = raw_data['main']['temp']-273.15
  min_temp_in_degree =  raw_data['main']['temp_min']-273.15
  max_temp_in_degree = raw_data['main']['temp_max']-273.15

  wind_speed = raw_data['wind']['speed']
  wind_degree = raw_data['wind']['deg']
  wind_gust = raw_data['wind']['gust']
  
  weather_data.append({
      "country":country,
      "latitude":latitude,
      "longitude":longitude,
      "city_name":city_name,
      "time_zone":time_zone,
      "date":date,
      "time_of_calculation":time_of_calculation,
      "sunrise_time":sunrise_time,
      "sunset_time":sunset_time,
      "cloud":cloud,
      "temperature_in_degree":temperature_in_degree,
      "min_temp_in_degree":min_temp_in_degree,
      "max_temp_in_degree":max_temp_in_degree,
      "wind_speed":wind_speed,
      "wind_degree":wind_degree,
      "wind_gust":wind_gust

  })
  print(weather_data)
  return weather_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'email': 'engromeke@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'energy_etl',
    default_args =default_args,
    description='energy weather etl data pipeline'
)


extraction = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag,
)
transformation = PythonOperator(
    task_id='data_transformation',
    python_callable=transformation,
    dag=dag,
)

extraction >> transformation