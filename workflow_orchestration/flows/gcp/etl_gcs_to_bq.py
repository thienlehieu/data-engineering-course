from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
  """Download trip data from GCS"""
  gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
  gcs_block = GcsBucket.load("gcs-dce")
  gcs_block.get_directory(from_path=gcs_path, local_path=f"week2/")
  return Path(f"week2/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
  """Data cleaning example"""
  df = pd.read_parquet(path)
  print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
  df["passenger_count"].fillna(0, inplace=True)
  print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
  return df


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
def etl_gcs_to_bq(year: int, month: int, color: str):
  """Main ETL flow to load data into Big Query"""
  path = extract_from_gcs(color, year, month)
  df = transform(path)
  write_bq(df, color)


if __name__ == "__main__":
  etl_gcs_to_bq()