import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import load_parquet_to_bq, load_csv_to_bq


def ingest_data():
    """
    Load raw taxi and zone data into bigquery.
    Processes all parquet files in data/ folder - each file is one month.
    Safe to re-run: truncates and reloads each time.
    """
    data_dir = Path("data")
    
    # load all taxi trip files - handles multiple months
    parquet_files = sorted(data_dir.glob("yellow_tripdata_*.parquet"))
    for parquet_file in parquet_files:
        print(f"Loading {parquet_file.name}...")
        load_parquet_to_bq(str(parquet_file), "raw_trips", partition_field="tpep_pickup_datetime")
    
    # load zone lookup - small reference table
    csv_file = data_dir / "taxi_zone_lookup.csv"
    print(f"Loading {csv_file.name}...")
    load_csv_to_bq(str(csv_file), "zones")
    
    print("\nData loaded.")

if __name__ == '__main__':
    ingest_data()
