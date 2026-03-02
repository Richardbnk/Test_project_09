import os
from google.cloud import bigquery

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('GCP_DATASET_ID', 'taxi_analytics')


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def load_parquet_to_bq(parquet_path, table_id, partition_field=None):
    """
    Load a parquet file into BigQuery. Partitions by date field if provided. Truncates on reload.
    """
    client = get_bq_client()
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    # create partitioned table if needed
    if partition_field:
        table = bigquery.Table(full_table_id)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        try:
            client.create_table(table, exists_ok=True)
        except:
            pass
    
    with open(parquet_path, 'rb') as f:
        job = client.load_table_from_file(f, full_table_id, job_config=job_config)
    
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")


def load_csv_to_bq(csv_path, table_id):
    """
    Load a CSV file into BigQuery. Auto-detects schema. Truncates on reload.
    """
    client = get_bq_client()
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    with open(csv_path, 'rb') as f:
        job = client.load_table_from_file(f, full_table_id, job_config=job_config)
    
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")


def run_query(query):
    """
    Run a SQL query on BigQuery.
    """
    client = get_bq_client()
    job = client.query(query)
    job.result()
    return job


def query_to_dataframe(query):
    """
    Run a query and return results as a DataFrame.
    """
    client = get_bq_client()
    return client.query(query).to_dataframe()
