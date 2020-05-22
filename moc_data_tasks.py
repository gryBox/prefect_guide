import pandas as pd
import numpy as np
import requests
import sqlalchemy as sa
from datetime import timedelta

from prefect import task

# Error Handling
from error_handling import imb_handler, error_notifcation_handler


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from prefect.engine.results import S3Result, LocalResult
s3_handler = S3Result(bucket="results-prefect-tst", location="{flow_name}")

@task(
    max_retries=2, 
    retry_delay=timedelta(seconds=1),
    target="{flow_name}/{task_name}",
    state_handlers=[imb_handler, error_notifcation_handler]
    )
def get_tsx_moc_imb(url: str):
    """
    Scrape the TSX website Market on close website. Data only available weekdays after 15:40 pm Toronto time
    until 12 am.
    
    Use archived url for testing.       
    "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    """

    #raise Exception
    
    # 1, Get the html content
    html = requests.get(url).content
    
    # 2. Read all the tables
    df_list = pd.read_html(html, header=[0], na_values=[''], keep_default_na=False)
    
    tsx_imb_df = df_list[-1]
    
    logger.info(f"MOC download shape {tsx_imb_df.shape}")

    tsx_imb_df = tsx_imb_df.set_index('Symbol')

    return tsx_imb_df#.head(0)

@task(
    target="{flow_name}/{task_name}",
    state_handlers=[error_notifcation_handler])
def partition_df(df, n_conn=1):
    # raise Exception
    df_lst = np.array_split(df, n_conn)
    return df_lst


@task(
    target="{flow_name}/{task_name}/{filename}/{map_index}",
    state_handlers=[error_notifcation_handler])
def df_to_db(df, tbl_name, conn_str):
    #raise Exception
    engine = sa.create_engine(conn_str)
    
    # Changer "if_exist" to "append" when done devolpment
    df.to_sql(name=tbl_name, con=engine, if_exists="append", index=True, method="multi")
    
    engine.dispose()
  
    return df.shape