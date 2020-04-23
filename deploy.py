
import os

from prefect import Client
from prefect.environments.storage import Docker

import boto3
import base64

from etl_moc import etl_moc_flow




def retrieve_ecr_login(region):
    ecr_client = boto3.client('ecr', region_name=region)
    ecr_token = ecr_client.get_authorization_token()

    return ecr_token


def get_ecr_url(ecr_token, aws_ecr_repo_name):

    # 1. Decode the aws token
    username, password = base64.b64decode(ecr_token['authorizationData'][0]['authorizationToken']).decode().split(':')
    ecr_url = ecr_token['authorizationData'][0]['proxyEndpoint']

    return ecr_url


# Paths
working_dir_path = os.getcwd()
print(f"Working Dir: {working_dir_path}")

# 1. Set the path to the docker
docker_flpth = os.path.join(working_dir_path, "Dockerfile")
print(f"Docker flpth: {docker_flpth}")

# 2. Get the ECR URL (This is where the docker (image?) is stored)
# a. retrieve_ecr_login
ecr_token  = retrieve_ecr_login(region="us-east-2")

# b. Deserialize and get url
ecr_url = get_ecr_url(ecr_token, aws_ecr_repo_name="get-tsx-moc-ecr")

# Build a docker container
etl_moc_flow.storage = Docker(
    #registry_url=ecr_url,
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