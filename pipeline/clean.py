import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import run_query, PROJECT_ID, DATASET_ID


def clean_data():
    """
    Clean and enrich taxi data with zone info. Rebuilds clean_trips from raw_trips each run.
    """
    project_id = PROJECT_ID
    dataset_id = DATASET_ID
    
    # note: for production with billing enabled, would use MERGE statement for true incremental processing
    # using create or replace for free tier compatibility
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.clean_trips` AS
    SELECT
        CONCAT(CAST(t.VendorID AS STRING), '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', t.tpep_pickup_datetime)) as trip_id,
        t.tpep_pickup_datetime as pickup_datetime,
        t.tpep_dropoff_datetime as dropoff_datetime,
        EXTRACT(HOUR FROM t.tpep_pickup_datetime) as pickup_hour,
        CAST(t.tpep_pickup_datetime AS DATE) as pickup_date,
        pz.Zone as pickup_zone,
        dz.Zone as dropoff_zone,
        pz.Borough as pickup_borough,
        dz.Borough as dropoff_borough,
        t.passenger_count,
        t.trip_distance,
        TIMESTAMP_DIFF(t.tpep_dropoff_datetime, t.tpep_pickup_datetime, MINUTE) as trip_duration_minutes,
        t.fare_amount,
        t.tip_amount,
        t.total_amount as total_earnings,
        t.payment_type
    FROM `{project_id}.{dataset_id}.raw_trips` t
    LEFT JOIN `{project_id}.{dataset_id}.zones` pz ON t.PULocationID = pz.LocationID
    LEFT JOIN `{project_id}.{dataset_id}.zones` dz ON t.DOLocationID = dz.LocationID
    WHERE 
        t.tpep_pickup_datetime IS NOT NULL
        AND t.tpep_dropoff_datetime IS NOT NULL
        AND t.trip_distance > 0
        AND t.fare_amount > 0
    """
    
    print("Cleaning data...")
    run_query(query)
    print("Clean data ready.")

if __name__ == '__main__':
    clean_data()
