from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta
from week2.flows.gcp.etl_gcs_to_bq import etl_gcs_to_bq



@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"week2/data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-dce")
    gcs_block.upload_from_path(from_path, to_path)
    return

@task()
def write_bq(df: pd.DataFrame, color) -> None:
  """Write DataFrame to BiqQuery"""

  gcp_credentials_block = GcpCredentials.load("gcs-dce-creds")

  df.to_gbq(
      destination_table=f"trips_data_all.{color}_tripdata",
      project_id="swift-arcadia-387709",
      credentials=gcp_credentials_block.get_credentials_from_service_account(),
      chunksize=500_000,
      if_exists="append",
  )

@flow()
def etl_web_to_gcs_bq(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    #df_clean = clean(df)
    df_clean = df    
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, Path(f"data/{color}/{dataset_file}.parquet"))
    write_bq(df_clean, color)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):  
    months = list(range(1, 8))
    color = "green"
    for month in months:
        etl_web_to_gcs_bq(year, month, color)
        #etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = list(range(1, 2))
    year = 2021
    etl_parent_flow(months, year, color)
