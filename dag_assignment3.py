import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

########################################################
#
#   DAG Settings
#
#########################################################
dag_default_args = {
    'owner': 'bde',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment_3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data" 
conn_ps = PostgresHook(postgres_conn_id='postgres').get_conn()
#########################################################
#
#   Custom Logics for Operator
#
#########################################################
def load_csv_to_postgres(filename, table):
    df = pd.read_csv(os.path.join(AIRFLOW_DATA, filename))
    
    on_conflict_clause = ""
    if table == 'raw.listings':
        # Handle NaN values in HOST_SINCE column
        if 'HOST_SINCE' in df.columns:
            df['HOST_SINCE'] = df['HOST_SINCE'].fillna('2000-01-01')  
            df['HOST_SINCE'] = pd.to_datetime(df['HOST_SINCE'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Handle NaN values in HOST_IS_SUPERHOST column
        if 'HOST_IS_SUPERHOST' in df.columns:
            df['HOST_IS_SUPERHOST'] = df['HOST_IS_SUPERHOST'].fillna('n')
        
        # Check if the values for HAS_AVAILABILITY are 1 character long
        col = 'HAS_AVAILABILITY'
        if col in df.columns and not all(df[col].astype(str).str.len() == 1):
            raise ValueError(f"Column {col} has values longer than 1 character.")
        
        on_conflict_clause = " ON CONFLICT (LISTING_ID) DO NOTHING"

    # Convert the dataframe to a list of tuples
    values = list(df.itertuples(index=False, name=None))

    # Create the SQL insert statement with individual placeholders for each column
    columns = ', '.join(df.columns)
    placeholders = '(' + ', '.join(['%s'] * len(df.columns)) + ')'
    insert_sql = f"INSERT INTO {table} ({columns}) VALUES %s {on_conflict_clause}"

    conn_ps = PostgresHook(postgres_conn_id='postgres').get_conn()
    with conn_ps.cursor() as cursor:
        execute_values(cursor, insert_sql, values)
    conn_ps.commit()


def load_NSW_LGA_files(**kwargs):
    load_csv_to_postgres('NSW_LGA_CODE.csv', 'raw.nsw_lga_code')
    load_csv_to_postgres('NSW_LGA_SUBURB.csv', 'raw.nsw_lga_name')

def load_listings_files(**kwargs):
    for year in [2020, 2021]:
        for month in range(1, 13):
            if year == 2020 and month < 5:
                continue
            if year == 2021 and month > 4:
                break
            filename = f"{month:02}_{year}.csv"
            load_csv_to_postgres(filename, 'raw.listings')

def load_census_files(**kwargs):
    load_csv_to_postgres('2016Census_G01_NSW_LGA.csv', 'raw.census_g01')
    load_csv_to_postgres('2016Census_G02_NSW_LGA.csv', 'raw.census_g02')

#########################################################
#
#   DAG Operator Setup
#
#########################################################
load_NSW_LGA_task = PythonOperator(
    task_id="load_NSW_LGA_files",
    python_callable=load_NSW_LGA_files,
    provide_context=True,
    dag=dag
)

load_listings_task = PythonOperator(
    task_id="load_listings_files",
    python_callable=load_listings_files,
    provide_context=True,
    dag=dag
)

load_census_task = PythonOperator(
    task_id="load_census_files",
    python_callable=load_census_files,
    provide_context=True,
    dag=dag
)

# Task Dependencies
[load_NSW_LGA_task, load_listings_task, load_census_task]


