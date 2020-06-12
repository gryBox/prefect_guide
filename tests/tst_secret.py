import prefect 

print(prefect.__version__)

from prefect import task, Flow
from prefect.tasks.secrets.base import PrefectSecret


@task
def load_db(s):
    return print("Got Secret")

with Flow("blah") as flow:
  
    s = PrefectSecret("AWS_CREDENTIALS")
    load_db(s)

st = flow.run()
flow.visualize(flow_state=st)