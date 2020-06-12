import prefect 

print(prefect.__version__)

from prefect import task, Flow
from prefect.engine.results import LocalResult, S3Result
from prefect.tasks.secrets.base import PrefectSecret
from prefect.engine.executors import DaskExecutor, LocalExecutor
from prefect.environments.storage import Docker
from prefect.environments import RemoteEnvironment

template = '{flow_name}-{today}/{task_name}/{map_index}.prefect'

s3_result = S3Result(
    bucket="results-prefect-tst",
)

@task()
def gen_list():
    return [x for x in range(10)]


@task(
    target=template
)
def add(x, y):
    return x + y


@task(
    target=template
)
def multiply(x, y):
    return x * y

with Flow("Map-Test", result=s3_result) as flow:
    x = gen_list()
    y = gen_list()
    added = add.map(x, y)
    multiply.map(added, added)

flow.environment=RemoteEnvironment(executor="prefect.engine.executors.LocalExecutor")
flow.storage=Docker(
        #registry_url=registry_url,
        image_name="pretest",
        image_tag="latest",
        python_dependencies=[
            'boto3',
        ]
    )

flow.visualize()

st = flow.run()

flow.visualize(flow_state=st)