from prefect import task, Flow, Parameter

## initialize the Parameter outside of any
## Flow context

add_num = Parameter("add_num", default=10)

@task
def add_one(x):
    return x+1

with Flow("Flow 1") as flow_1:
    new_num1 = add_one(add_num)

@task
def add_two(y):
    return y+1

with Flow("Flow 2") as flow_2:
   new_num2 = add_one(add_num)

combo_fl = Flow("Add Numbers")

combo_fl.update(flow_1)
combo_fl.update(flow_2, validate=True)


combo_fl.visualize()


# @task
# def return_list():
#     return 1
# @task
# def add_one(x):
#     return x + 1

# @task 
# def divide_num(x):
#     return x/2

# @task
# def mul_num

# with Flow("blah", result=lcl_res) as flow:
#     mapped_task.map(return_list)


# st = flow.run()
# flow.visualize(flow_state=st)