import json
import os
import urllib.parse
import urllib.request


print("Loading function")


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    ## prep the data
    create_mutation = """
    mutation($input: createFlowRunInput!){
        createFlowRun(input: $input){
            flow_run{
                id
            }
        }
    }
    """
    inputs = dict(flowId=os.getenv("PREFECT__FLOW_ID"))

    variables = dict(input=inputs)
    data = json.dumps(
        dict(query=create_mutation, variables=json.dumps(variables))
    ).encode("utf-8")

    ## prep the request
    req = urllib.request.Request(os.getenv("PREFECT__CLOUD__API"), data=data)
    req.add_header("Content-Type", "application/json")
    req.add_header(
        "Authorization", "Bearer {}".format(os.getenv("PREFECT__CLOUD__AUTH_TOKEN"))
    )

    ## send the request and return the response
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode())