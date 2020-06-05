
import os
from prefect import Flow
from prefect.environments.storage import Docker
from prefect.tasks.shell import ShellTask
from prefect.tasks.secrets.base import PrefectSecret
from dask_cloudprovider import FargateCluster
from prefect.environments.execution.dask.cloud_provider import DaskCloudProviderEnvironment


from prefect.schedules import clocks, filters, Schedule
from prefect.schedules import IntervalSchedule


import boto3
import docker
import base64

from datetime import timedelta
import pendulum

from moc_data_flow import tsx_imb_fl


# prefect
# cloud_agent_token = "xyz"
project_name =  "market-on-close"

# Docker
dckr_image_name = "pretest"
dckr_tag = "latest"

working_dir_path = os.getcwd()
print(f"Working Dir: {working_dir_path}")
docker_flpth = os.path.join(working_dir_path, "Dockerfile")
print(f"Docker flpth: {docker_flpth}")

# aws
aws_ecr_repo_name = dckr_image_name
aws_region = "us-east-2"


############## Schedule when to run the script ##############
schedule = Schedule(
    # fire every day
    clocks=[clocks.IntervalClock(
        start_date=pendulum.datetime(2020, 4, 22, 17, 30, tz="America/Toronto"),
        interval=timedelta(days=1)
        )],
    # but only on weekdays
    filters=[filters.is_weekday],

    # and not in January TODO: Add TSX Holidays
    not_filters=[filters.between_dates(1, 1, 1, 31)]
)

#tsx_imb_fl.schedule = schedule

############## Storage ecr docker flow ##############
p = PrefectSecret("docker_ecr_login")
dkr_ecr_scrt = p.run()

get_ecr_auth_token = ShellTask(helper_script="cd ~")
with Flow("deploy to aws") as deploy_fl:
    ecr_auth_tocken = get_ecr_auth_token(command=dkr_ecr_scrt)

st = deploy_fl.run()
deploy_fl.visualize(flow_state=st)

ecr_client = boto3.client('ecr', region_name=aws_region)
ecr_token = ecr_client.get_authorization_token()

# # Decode the aws token
username, password = base64.b64decode(ecr_token['authorizationData'][0]['authorizationToken']).decode().split(':')
ecr_url = ecr_token['authorizationData'][0]['proxyEndpoint']

############################################################

# # # Registry URL for prefect or docker push
ecr_repo_name = f"{ecr_url.replace('https://', '')}"#/{aws_ecr_repo_name}" #:latest"

# 5. Add Docker push to docker repo
tsx_imb_fl.storage = Docker(
    registry_url=ecr_repo_name,
    python_dependencies=[
        "pandas", "sqlalchemy", "psycopg2",
        "beautifulsoup4","lxml", "boto3","requests", 
        "dask_cloudprovider"],
    dockerfile=docker_flpth,
    image_name=dckr_image_name,
    image_tag=f"latest",
    local_image=True
    )




############## Define the enviroment for a fargate cluster

from prefect.environments import FargateTaskEnvironment
environment=FargateTaskEnvironment(
        launch_type="FARGATE",
        #aws_session_token="MY_AWS_SESSION_TOKEN",
        region="us-east-2",
        cpu="1024",
        memory="2048",
        # networkConfiguration={
        #     "awsvpcConfiguration": {
        #         "assignPublicIp": "ENABLED",
        #         "subnets": ["MY_SUBNET_ID"],
        #         "securityGroups": ["MY_SECURITY_GROUP"],
        #     }
        # },
        family="tsx-moc",
        # taskRoleArn="MY_TASK_ROLE_ARN",
        # executionRoleArn="MY_EXECUTION_ROLE_ARN",
        containerDefinitions={
            "name": "flow-container",
            "image": ecr_repo_name,
            "command": [],
            "environment": [],
            "essential": True,
        }
    )



tsx_imb_fl.environment = environment



# # # 6. Register 
pushlog = tsx_imb_fl.register(project_name="market-on-close", build=True)

print(pushlog)
