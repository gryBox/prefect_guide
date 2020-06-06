import prefect
from prefect import Parameter, Task, Flow, task

print (prefect.__version__)


## Test deep copy

@task
def add_one(a_number: int): 
    return a_number + 1

@task(log_stdout=True)
def print_result(the_result:int): 
    print(the_result)

with Flow("Add") as add_fl:
    a_number = Parameter("a_number", default=1)
    
    the_result = add_one(a_number)
    
    print_result(the_result)

    
add_fl.visualize()

# Copy
new_flow  = add_fl.copy()
for task in new_flow:
    task.name = f"new-flow-{task.name}"

# new flow with copy
new_flow.visualize()

# original flow gone bad
add_fl.visualize()



# Deep copy
new_flow  = add_fl.deep_copy()
for task in new_flow:
    task.name = f"new-flow-{task.name}"

# new flow with copy
new_flow.visualize()


# original flow gone bad
add_fl.visualize()

origin  https://github.com/gryBox/prefect.git (fetch)
origin  https://github.com/gryBox/prefect.git (push)
upstream        https://github.com/PrefectHQ/prefect.git (fetch)
upstream        https://github.com/PrefectHQ/prefect.git (push)


    def deep_copy(self) -> "Flow":
        """
        Create a new deep copy of the current flow
        """
        new = copy.deepcopy(self)
        # create a new cache
        new._cache = dict()

        # TODO Create deep copies of tasks and edges
        new.tasks = self.tasks.copy()
        new.edges = self.edges.copy()
        new.set_reference_tasks(self._reference_tasks)

        return new