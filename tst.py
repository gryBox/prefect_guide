from prefect import task, Flow
from prefect.engine.result_handlers import LocalResultHandler, S3ResultHandler

import datetime

s3_handler = S3ResultHandler(bucket='tsx-moc-bcp')  
lcl_handler = LocalResultHandler()

# configure on the task decorator
@task(result_handler=lcl_handler)
def add(x, y=1):
    return x + y




with Flow("Result Handler Test") as fl:
    first_result = add(1, y=2)
    second_result = add(x=first_result, y=100)

fl_state = fl.run()
print(fl_state.result[first_result].result)
print(fl_state.result[first_result]._result.safe_value)