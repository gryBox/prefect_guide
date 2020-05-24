from dask_cloudprovider import FargateCluster

from prefect import Flow, Parameter, task
from prefect.engine.executors import DaskExecutor

cluster = FargateCluster(
    image="prefecthq/prefect:latest",
    #task_role_arn="arn:aws:iam::<your-aws-account-number>:role/<your-aws-iam-role-name>",
    execution_role_arn="arn:aws:iam::417497546600:role/ecsTaskExecutionRole",
    n_workers=1,
    scheduler_cpu=256,
    scheduler_mem=512,
    worker_cpu=256,
    worker_mem=512,
    scheduler_timeout="15 minutes",
)
# Be aware of scheduler_timeout. In this case, if no Dask client (e.g. Prefect
# Dask Executor) has connected to the Dask scheduler in 15 minutes, the Dask
# cluster will terminate. For development, you may want to increase this timeout.


@task
def times_two(x):
    return x * 2


@task
def get_sum(x_list):
    return sum(x_list)


with Flow("Dask Cloud Provider Test") as flow:
    x = Parameter("x", default=[1, 2, 3])
    y = times_two.map(x)
    results = get_sum(y)

flow.run(executor=DaskExecutor(cluster.scheduler.address),
         parameters={"x": list(range(10))})

# Tear down the Dask cluster. If you're developing and testing your flow you would
# not do this after each Flow run, but when you're done developing and testing.
cluster.close()
