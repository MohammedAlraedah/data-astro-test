from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2

conn_params = {
    "dbname": "your_db",
    "user": "your_user",
    "password": "your_password",
    "host": "localhost",
    "port": 5432
}

"""
on this script we will learn how to create ETL and wrap it under dag.
ETL stands for: Extract - transform - load.
So with that we will create extract function and transform function and loading function.
"""

# Extrct will extract data from source whick here in our case API
def extract_transform_load():
    response = requests.get("https://api.coindesk.com/v1/bpi/currentprice.json")
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
    data = response.json()

    transformed_data = pd.json_normalize(data)

    # conn = psycopg2.connect(**conn_params)
    # cursor = conn.cursor()
    
    # # Convert DataFrame to a list of tuples
    # records = [tuple(x) for x in data.to_records(index=False)]

    # query = f"INSERT INTO scranton_office (id, empolyee, dept) VALUES (%s, %s, %s)"
    
    # try:
    #     cursor.executemany(query, values)
    #     conn.commit()
    # except Exception as e:
    #     print(f"Error: {e}")
    #     conn.rollback()
    # finally:
    #     cursor.close()
    #     conn.close()
    print(data)
        
    return "ETL finished successfully"
    


# Define the DAG
with DAG(
    "scranton_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # Runs daily
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="dwight_sales",
        python_callable=extract_transform_load
    )

etl_task
