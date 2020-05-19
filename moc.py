import requests
import pandas as pd
import numpy as np
import sqlalchemy as sa

from prefect.client import Secret

from prefect import Flow, Parameter, task

from prefect.engine.state import Success, Failed, Skipped
from prefect.engine import signals
from prefect.engine.results import S3Result, LocalResult

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)





# Initialize where to store task results
#tsx_imb_res = S3Result(bucket="tsx-moc-bcp")
tsx_imb_res = LocalResult(dir="~/MOC/moc_res")
def error_handler(obj, old_state, new_state):
    
    if new_state.is_failed():
        # # Sends a slack notification to system errors in the tapnotion slack channel
        #         s = Secret("system_errors") # create a secret object
        #         slack_web_hook_url = s.get() # retrieve its value

        #         msg = f"Task '{obj.name}' finished in state {new_state.message}"
        # replace URL with your Slack webhook URL
        # requests.post(slack_web_hook_url, json={"text": msg})
        
        raise signals.SKIP(message='skipping!')
        return_state = Failed("Failed")
                              
        
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
    max_retries=3,
    retry_delay=timedelta(seconds=1), # In production this will be change to 20 minutes
    target="{task_name}-{today}.prefect",
    result=tsx_imb_res, # S3Result(bucket="tsx-moc-bcp/{task_name}-{today}"), 
    state_handlers=[imb_handler, error_handler]
)
def get_tsx_moc_imb(url: str):
    
    # Pefect logger added.  See https://docs.prefect.io/core/idioms/logging.html#logging-from-within-tasks
    #logger = prefect.context.get("logger") # (Error: jupyter notebook?)
    
    raise 1/0
    
    # 1, Get the html content
    html = requests.get(url).content
    
    # 2. Read all the tables
    df_list = pd.read_html(html, header=[0], na_values=[''], keep_default_na=False)
    
    tsx_imb_df = df_list[-1]
    
    logger.info(f"MOC download shape {tsx_imb_df.shape}")

    return tsx_imb_df

@task
def add_imb_features(tsx_imb_df):
    # rename columns
    tsx_imb_df.rename(columns=lambda clm_nm: clm_nm.replace(" ", "_").lower(), inplace=True)
    
    # Convert string to
    tsx_imb_df["imb_side_enc"] = np.where(tsx_imb_df['imbalance_side']=="BUY", 1, 0)

    return tsx_imb_df

@task
def df_to_db(df, tbl_name, conn_str):

    engine = sa.create_engine(conn_str)
    
    df.to_sql(
        name=tbl_name,
        con=engine,
        if_exists="append",
        index=True,
        method="multi"
        )
    
    engine.dispose()
  
    return df.shape





with Flow("Our first flow") as tst_flow:
    
    tsx_url = Parameter("tsx_url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    
    tsx_imb_df = get_tsx_moc_imb(tsx_url)

if __name__ == "__main__":
    
    tsx_url = "https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    put_dir = "s3://tsx-moc/"
    
    fl_states = tst_flow.run(
        parameters={"tsx_url":backup_url}
    )
    tst_flow.visualize(flow_state=fl_states)

    tst_fl.visualize(flow_state=fl_state)
    #print(fl_state.result[df]._result.safe_value)

    pass
    #etl_moc_flow.visualize()
    # etl_state = etl_moc_flow.run(
    #     # parameters=dict(
    #     #     tsx_url="https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    #     # )
    #     #executor=DaskExecutor()
    # )
    #etl_moc_flow.visualize(flow_state=etl_state) 

    # s = Secret("moc_pgdb_conn") # create a secret object
    # print(s.exists()) # retrieve its value

# (base) ilivni@ilivni-UX430UAR:~$ cd ~
# (base) ilivni@ilivni-UX430UAR:~$ cd .prefect
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ touch config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  config.toml  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ nano config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ 
