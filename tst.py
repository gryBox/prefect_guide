from prefect import task, Flow
from prefect.engine.results import LocalResult

lcl_res = LocalResult(location="/home/ilivni/prefect_guide/results/{flow_name}")

@task(target="a_specific_file.txt",
    resul
)
def return_list():
    return [1, 2, 3]
@task(target="{task_name}/{filename}/{map_index}.prefect")
def mapped_task(x):
    return x + 1
with Flow("blah", result=lcl_res) as flow:
    mapped_task.map(return_list)


st = flow.run()
flow.visualize(flow_state=st)