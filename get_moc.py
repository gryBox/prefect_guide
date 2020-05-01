import pandas as pd
import requests

from datetime import timedelta

from prefect import task, Flow, Parameter, tags
from prefect.engine.result_handlers import LocalResultHandler, S3ResultHandler
from prefect.engine.state import Success, Failed, Skipped
from prefect.client.secrets import Secret


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_handler = S3ResultHandler(bucket='tsx-moc-bcp')  
lcl_handler = LocalResultHandler()

def error_notifcation_handler(obj, old_state, new_state):
    # Hamdle an empty dataframe to return a fail message.  
    # The result of a succesfull 
    if new_state.is_failed():
        s = Secret("system_errors") # create a secret object
        slack_web_hook_url = s.get() # retrieve its value

        msg = "Task '{0}' finished in state {1}".format(obj.name, new_state.message)
        # replace URL with your Slack webhook URL
        requests.post(slack_web_hook_url, json={"text": msg})
                
    else:
        return_state = new_state   
        
    return return_state



def imb_handler(obj, old_state, new_state):
    # Hamdle an empty dataframe to return a fail message.  
    # The result of a succesfull 
    if isinstance(new_state, Success) and new_state.result.empty:
        return_state = Failed(
            message=f"No tsx imbalance data: No trading or Data was not published to {new_state.cached_inputs['url']}", 
            result=new_state.result
            )        
    else:
        return_state = new_state   

    return return_state

@task(
    max_retries=2, 
    retry_delay=timedelta(seconds=1),
    result_handler=s3_handler,
    state_handlers=[imb_handler, error_notifcation_handler])
def get_tsx_moc_imb(url: str):
    """
    Scrape the TSX website Market on close website. Data only available weekdays after 15:40 pm Toronto time
    until 12 am.
    
    Use archived url for testing.       
    "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    """
    assert 1/0
    
    # 1, Get the html content
    html = requests.get(url).content
    
    # 2. Read all the tables
    df_list = pd.read_html(html, header=[0], na_values=[''], keep_default_na=False)
    
    tsx_imb_df = df_list[-1]
    
    logger.info(f"MOC download shape {tsx_imb_df.shape}")

    return tsx_imb_df.head(0)


with Flow(name="Get-TSX-MOC-Imbalances") as tsx_imb_fl:
    
    tsx_url = Parameter("tsx_url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    
    tsx_imb_df = get_tsx_moc_imb(tsx_url)
    


if __name__ == "__main__":

    # Inputs
    tsx_url = 'https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html'
    backup_url = "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"

    # Script
    from prefect.engine.executors import LocalExecutor

    #from prefect.environments import RemoteEnvironment
    #tsx_imb_fl.environment=RemoteEnvironment(executor="prefect.engine.executors.LocalExecutor")

    fl_state = tsx_imb_fl.run(
        parameters=dict(
            tsx_url=tsx_url
        ), 
        executor=LocalExecutor()

    )
    tsx_imb_fl.visualize(flow_state=fl_state)