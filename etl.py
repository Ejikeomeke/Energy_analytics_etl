import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from airflow import DAG
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from airflow.operators.python import PythonOperator

load_dotenv()

def get_data(ti):
  lati = 9.072264
  longi = 7.491302
  API_key = os.getenv("WEATHER_API")

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
  transformed_data = weather_data
  ti.xcom_push(key='transformed_data', value=transformed_data)
  return transformed_data


def stage_data(ti):
  extract = ti.xcom_pull(key='transformed_data', task_ids='data_transformation')
  extracted = pd.DataFrame(extract)
  extracted_df=pd.DataFrame(extracted)
  existing_csv=pd.read_csv('weather.csv')
  existing_df=pd.DataFrame(existing_csv)
  df = pd.concat([existing_df, extracted_df], ignore_index=True)
  df.to_csv('weather.csv', index=False)

def load_data(ti):
  weather_data = pd.read_csv('weather.csv')
  df = pd.DataFrame(weather_data)
  connection_str = os.getenv("CONTAINER_STRING")
  container_name = os.getenv("CONTAINER_NAME")
    
    # create a blobserviceClient
  blob_service_client = BlobServiceClient.from_connection_string(connection_str)
  container_client = blob_service_client.get_container_client(container_name)

  files = [(df, 'raw/transformed_weather_data.csv')]

  for file, blob_name in files:
    blob_client = container_client.get_blob_client(blob_name)
    output = file.to_csv(index=False)
    blob_client.upload_blob(output, overwrite=True)
    print(f"{blob_name} loaded into Azure succesfully")
    



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
    'weather_energy_etl',
    default_args =default_args,
    description='weather etl data pipeline'
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

staging = PythonOperator(
  task_id='stage_data',
  python_callable=stage_data,
  dag=dag,
)


loading = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)


extraction >> transformation >> staging >> loading
