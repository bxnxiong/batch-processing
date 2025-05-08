from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd
import io
from sqlalchemy import create_engine
from utils.constants import MINIO_BUCKET_NAME, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from sqlalchemy import create_engine

# MinIO connection settings
MINIO_ENDPOINT = 'minio:9000'  # Changed from localhost:9000 to use container name
FILE_PREFIX = 'raw'


# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres2',  # This is correct as it matches the container name
    'port': '5432',       # Changed from 5434 to 5432 as this is the internal container port
    'database': 'reddit',
    'user': 'postgres',
    'password': 'password'
}

# Initialize MinIO client
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )


def process_csv_to_postgres(**context):
    client = get_minio_client()
    
    # PostgreSQL connection using the POSTGRES_CONN dictionary
    conn_string = f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    engine = create_engine(conn_string)
    
    # List all CSV objects in the bucket
    objects = client.list_objects(MINIO_BUCKET_NAME, prefix=FILE_PREFIX, recursive=True)
    
    with engine.connect() as conn:
        for obj in objects:
            if not obj.object_name.endswith('.csv'):
                continue
                
            # Read CSV from MinIO
            response = client.get_object(MINIO_BUCKET_NAME, obj.object_name)
            df = pd.read_csv(io.BytesIO(response.read()))

            # Generate table name from file name
            table_name = obj.object_name.split('/')[-1].replace('.csv', '').lower()
            # Write DataFrame to PostgreSQL
            conn_string = f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
            engine = create_engine(conn_string)
            
            
            df.to_sql(
                table_name,
                con=conn,
                # schema='reddit',
                if_exists='replace',
                index=False,
                method='multi'
            )
            print(f"Loaded {obj.object_name} to PostgreSQL table {table_name}")


default_args = {
    'owner': 'Beixian Xiong',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'minio_processing_dag',
    default_args=default_args,
    description='Process CSV files from MinIO to Postgres stg tables for Superset dashboards',
    catchup=False,
    schedule_interval='@daily',
    tags=['reddit', 'etl', 'pipeline']
) as dag:

    process_files = PythonOperator(
        task_id='process_csv_to_postgres',
        python_callable=process_csv_to_postgres,
        provide_context=True,
    )

    process_files
