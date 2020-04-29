
import os

from prefect import Client
from prefect.environments.storage import Docker

import boto3
import docker
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

def push_to_ecr(ecr_token, aws_ecr_repo_name):
    
    # 1. Decode the aws token
    username, password = base64.b64decode(ecr_token['authorizationData'][0]['authorizationToken']).decode().split(':')
    ecr_url = ecr_token['authorizationData'][0]['proxyEndpoint']
    
    # 2. Connect to docker deamon and pull image
    docker_client = docker.from_env()
    docker_client.login(username, password, registry=ecr_url)
    
    # 3. Tag image for AWS ECR
    ecr_repo_name = f"{ecr_url.replace('https://', '')}/{aws_ecr_repo_name}:latest"
    #image.tag(ecr_repo_name, tag='latest')
    
    # 4. Push to ECR
    push_log = docker_client.images.push(ecr_repo_name, tag='latest')
    return push_log




# prefect
# cloud_agent_token = "xyz"
project_name =  "market-on-close"

# Docker
dckr_image_name = "etl-moc-img"
dckr_tag = "latest"

working_dir_path = os.getcwd()
print(f"Working Dir: {working_dir_path}")
docker_flpth = os.path.join(working_dir_path, "Dockerfile")
print(f"Docker flpth: {docker_flpth}")

# aws
aws_ecr_repo_name = "etl-moc-img"
aws_region = "us-east-2"

# ############ Script #######



ecr_client = boto3.client('ecr', region_name=aws_region)
ecr_token = ecr_client.get_authorization_token()

# # Decode the aws token
username, password = base64.b64decode(ecr_token['authorizationData'][0]['authorizationToken']).decode().split(':')
ecr_url = ecr_token['authorizationData'][0]['proxyEndpoint']

# # # Registry URL for prefect or docker push
ecr_repo_name = f"{ecr_url.replace('https://', '')}"#/{aws_ecr_repo_name}" #:latest"

# 5. Add Docker push to docker repo
etl_moc_flow.storage = Docker(
    #registry_url=ecr_repo_name,
    python_dependencies=[
        "pandas", "sqlalchemy", "psycopg2", "s3fs", "html5lib",
        "beautifulsoup4","lxml", "boto3", "pyhumps", "requests", 
        "yfinance"],
    dockerfile=docker_flpth,
    image_name="etl-moc-img",
    image_tag=f"latest",
    local_image=True
    )

# # # 6. Register 
pushlog = etl_moc_flow.register(project_name="market-on-close", build=True)

print(pushlog)
