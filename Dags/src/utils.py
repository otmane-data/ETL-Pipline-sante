import psycopg2
from psycopg2 import extras
from minio import Minio
import random
import sys
import polars as pl
import io
import logging
from datetime import datetime, timedelta
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('airflow_logs.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# MinIO configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_RAW = "sante-data-raw"
MINIO_BUCKET_CLEAN = "sante-data-clean"
MINIO_BUCKET_AGGREGATED = "sante-data-aggregated"

def get_minio_client():
    connection = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    return connection

def extract_table(table_name: str, **kwargs):
    execution_date = kwargs['execution_date']
    logging.info(f"Extracting data from {table_name} on {execution_date}")
    date_column_mapping = {
        'dim_temps': 'date',
        'dim_patient': 'date_naissance',
        'dim_medecin': 'date_creation',
        'dim_etablissement': 'date_creation',
        'dim_diagnostic': 'date_creation',
        'dim_medicament': 'date_creation',
        'fact_consultation': 'date_consultation',
        'fact_traitement': 'date_traitement',
        'fact_analyse': 'date_analyse',
        'fact_occupation_etablissement': 'date_occupation'
    }
    
    # Get the appropriate date column for the table
    date_column = date_column_mapping.get(table_name)
    logging.info(f"Date column for {table_name}: {date_column}")
    
    # Create PostgreSQL connection
    postgres_connection = psycopg2.connect(
        dbname="sante_database",
        user="postgres",
        password="postgres",
        host="postgres-prod",
        port="5432"
    )
    
    # Construct query based on whether table has date column
    if date_column:
        query = f"""
            SELECT * FROM {table_name} 
            WHERE DATE({date_column}) = DATE('{execution_date}')
        """
    else:
        query = f"SELECT * FROM {table_name}"

    # Execute query and get data as Polars DataFrame
    df = pl.read_database(query=query, connection=postgres_connection)
    logging.info(f"Extracted {len(df)} records from {table_name}")
    logging.info(f"Extracted data: {df.head()}")
    
    if len(df) > 0:
        # Convert to parquet bytes
        parquet_buffer = io.BytesIO()
        logging.info(f"Writing {len(df)} records to parquet")
        df.write_parquet(parquet_buffer)
        logging.info(f"Finished writing {len(df)} records to parquet")
        parquet_buffer.seek(0)
        
        # Save to MinIO
        minio_client = get_minio_client()
        logging.info(f"Uploading {len(df)} records to MinIO")
        
        # Create bucket if it doesn't exist
        if not minio_client.bucket_exists(MINIO_BUCKET_RAW):
            logging.info(f"Creating bucket {MINIO_BUCKET_RAW}")
            minio_client.make_bucket(MINIO_BUCKET_RAW)
        
        # Define the object path in MinIO
        object_path = f"{table_name}/{table_name}_{execution_date.strftime('%Y-%m-%d')}.parquet"
        logging.info(f"Uploading to {object_path}")
        
        # Upload to MinIO
        try:
            minio_client.put_object(
                bucket_name=MINIO_BUCKET_RAW,
                object_name=object_path,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
        except Exception as e:
            time.sleep(5)
            logging.error(f"Connection failed: {e}. Retrying...")
            minio_client = get_minio_client()
            minio_client.put_object(
                bucket_name=MINIO_BUCKET_RAW,
                object_name=object_path,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
        logging.info(f"Uploaded {len(df)} records to MinIO")
        
        return f"Extracted and saved {len(df)} records from {table_name}"
    else:
        return f"No data found for {table_name} on {execution_date}"

def load_data_from_minio(table: str, **kwargs):
    execution_date = kwargs['execution_date']
    logging.info(f"Loading data from MinIO for {execution_date}")
    minio_client = get_minio_client()
    object_path = f"{table}/{table}_{execution_date.strftime('%Y-%m-%d')}.parquet"
    logging.info(f"Loading data from {object_path}")
    try:
        df = pl.read_parquet(minio_client.get_object(MINIO_BUCKET_RAW, object_path))
    except Exception as e:
        logging.error(f"Error while loading data from MinIO: {e}")
        df = pl.DataFrame()
        return df.is_empty()
    logging.info(f"Loaded table: {table},\n execution_date = {execution_date},\n Data: {len(df.head())} records from MinIO")
    return df

def clean_data(table_name: str, **kwargs):
    execution_date = kwargs['execution_date']
    logging.info(f"Starting data cleaning for {table_name}")
    df = load_data_from_minio(table=table_name, execution_date=execution_date)
    
    if type(df) == bool:
        return f"No data found for {table_name} on {execution_date}"
    
    # Specific cleaning rules for each table
    if table_name == 'dim_patient':
        df = df.with_columns([
            pl.col('date_naissance').cast(pl.Date),
            pl.col('age').cast(pl.Int32)
        ])
    elif table_name == 'dim_medecin':
        df = df.with_columns([
            pl.col('experience_annees').cast(pl.Int32)
        ])
    elif table_name == 'dim_medicament':
        df = df.with_columns([
            pl.col('prix').cast(pl.Float64)
        ])
    elif table_name == 'fact_consultation':
        df = df.with_columns([
            pl.col('date_consultation').cast(pl.Date),
            pl.col('duree_minutes').cast(pl.Int32)
        ])
    
    # Common cleaning operations
    df = df.drop_nulls()
    df = df.unique()
    
    # Save cleaned data to MinIO
    if len(df) > 0:
        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        minio_client = get_minio_client()
        if not minio_client.bucket_exists(MINIO_BUCKET_CLEAN):
            minio_client.make_bucket(MINIO_BUCKET_CLEAN)
        
        object_path = f"{table_name}/{table_name}_{execution_date.strftime('%Y-%m-%d')}_clean.parquet"
        
        try:
            minio_client.put_object(
                bucket_name=MINIO_BUCKET_CLEAN,
                object_name=object_path,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            logging.info(f"Cleaned data saved for {table_name}")
            return f"Cleaned and saved {len(df)} records from {table_name}"
        except Exception as e:
            logging.error(f"Error saving cleaned data: {e}")
            return f"Error cleaning data for {table_name}: {str(e)}"

def dimension_pipeline(**kwargs):
    logging.info("Starting dimension tables pipeline")
    #def dimension_pipeline(**kwargs):
    logging.info("Starting dimension tables pipeline")
    dimension_tables = [
        'dim_temps',
        'dim_patient',
        'dim_medecin',
        'dim_etablissement',
        'dim_diagnostic',
        'dim_medicament'
    ]
    for table in dimension_tables:
        extract_result = extract_table(table, **kwargs)
        logging.info(extract_result)
        clean_result = clean_data(table, **kwargs)
        logging.info(clean_result)

    

def aggregate_daily_data(**kwargs):
    logging.info("Starting daily data aggregation")
    execution_date = kwargs['execution_date']
    logging.info("Starting daily data aggregation")

    df = load_data_from_minio("fact_consultation", execution_date=execution_date)
    if type(df) == bool:
        return "No consultation data to aggregate"

    if "date_consultation" not in df.columns:
        logging.warning("date_consultation column not found in fact_consultation")
        return "Missing column"

    agg_df = df.groupby("date_consultation").agg(
        [pl.count().alias("nb_consultations")]
    )

    parquet_buffer = io.BytesIO()
    agg_df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    minio_client = get_minio_client()
    if not minio_client.bucket_exists(MINIO_BUCKET_AGGREGATED):
        minio_client.make_bucket(MINIO_BUCKET_AGGREGATED)

    object_path = f"consultation_daily/consultation_{execution_date.strftime('%Y-%m-%d')}_agg.parquet"

    minio_client.put_object(
        bucket_name=MINIO_BUCKET_AGGREGATED,
        object_name=object_path,
        data=parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    logging.info("Aggregation completed and saved")
    return f"Aggregated data saved for date {execution_date}"

    

def insert_data_in_dim_tables(**kwargs):
    execution_date = kwargs['execution_date']
    logging.info("Starting insertion into dimension tables")

    minio_client = get_minio_client()
    table_names = [
        'dim_temps',
        'dim_patient',
        'dim_medecin',
        'dim_etablissement',
        'dim_diagnostic',
        'dim_medicament'
    ]

    conn = psycopg2.connect(
        dbname="sante_database",
        user="postgres",
        password="postgres",
        host="postgres-prod",
        port="5432"
    )
    cursor = conn.cursor()

    for table in table_names:
        object_path = f"{table}/{table}_{execution_date.strftime('%Y-%m-%d')}_clean.parquet"
        try:
            parquet_data = minio_client.get_object(MINIO_BUCKET_CLEAN, object_path)
            df = pl.read_parquet(parquet_data)

            # Insertion par ligne (à adapter à ton schéma)
            rows = df.to_dicts()
            for row in rows:
                columns = ','.join(row.keys())
                placeholders = ','.join(['%s'] * len(row))
                values = list(row.values())
                insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)

            conn.commit()
            logging.info(f"Inserted cleaned data into {table}")
        except Exception as e:
            logging.error(f"Error inserting data into {table}: {e}")
            conn.rollback()

    cursor.close()
    conn.close()


def fact_pipeline(**kwargs):
    logging.info("Starting fact tables pipeline")
    fact_tables = [
        'fact_consultation',
        'fact_traitement',
        'fact_analyse',
        'fact_occupation_etablissement'
    ]
    for table in fact_tables:
        extract_result = extract_table(table, **kwargs)
        logging.info(extract_result)
        clean_result = clean_data(table, **kwargs)
        logging.info(clean_result)
