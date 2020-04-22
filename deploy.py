
import os

from prefect import Client
from prefect.environments.storage import Docker

from etl_moc import etl_moc_flow




# Paths
working_dir_path = os.getcwd()
print(f"Working Dir: {working_dir_path}")

docker_flpth = os.path.join(working_dir_path, "Dockerfile")
print(f"Docker flpth: {docker_flpth}")

# Build a docker container
etl_moc_flow.storage = Docker(
    #registry_url="https://417497546600.dkr.ecr.us-east-2.amazonaws.com/get-tsx-moc-ecr",
    python_dependencies=[
        "pandas", "sqlalchemy", "psycopg2", "s3fs",
        "lxml", "boto3", "pyhumps", "requests", "yfinance"],
    dockerfile=docker_flpth,
    image_name="etl-moc-img",
    image_tag="latest"
    )


#etl_moc_flow.storage = storage.build()

# 1. Go to the UI client and create a project name

# 2. Register flow
## How to update flow? Without that error message?
etl_moc_flow.register(project_name="market-on-close")