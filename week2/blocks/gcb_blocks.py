from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


credentials_block = GcpCredentials(
    service_account_info={}
)
credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="prefect-de-zoomcamp",  # insert your  GCS bucket name
)

bucket_block.save("zoom-gcs", overwrite=True)