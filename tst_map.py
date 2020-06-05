import prefect 

print(prefect.__version__)

from prefect import task, Flow
from prefect.engine.results import LocalResult
from prefect.tasks.secrets.base import PrefectSecret

lcl_res = LocalResult(dir="~/prefect_guide/results/{flow_name}")

@task(target="{task_name}")
def return_list():
    return [1, 2, 3]

@task(target="{task_name}/{map_index}.prefect")
def mapped_task(x):
    return x + 1
    

with Flow("blah", result=lcl_res) as flow:
    mapped_task.map(return_list)


st = flow.run()
flow.visualize(flow_state=st)