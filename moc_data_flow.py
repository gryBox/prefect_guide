import pandas as pd
import numpy as np
import requests
import sqlalchemy as sa
from datetime import timedelta

from prefect import task, Flow, Parameter, unmapped
from prefect.engine.results import S3Result, LocalResult

from prefect.tasks.secrets.base import PrefectSecret

from moc_data_tasks import get_tsx_moc_imb, partition_df, df_to_db


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# For other template options https://docs.prefect.io/api/latest/utilities/context.html#context-2  
s3_handler = S3Result(bucket="results-prefect-tst", location="{flow_name}")

# A flow has no particular order unless the data is bound (shown) or explicitly set (not shown).
with Flow(name="Get-Imbalances", result=s3_handler) as tsx_imb_fl:
    
    tsx_url = Parameter("tsx_url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    imb_tbl_nm = Parameter("imb_tbl_nm", default="moc_tst")
    n_conn = Parameter("n_conn", default=1) 
    
    tsx_imb_df = get_tsx_moc_imb(tsx_url)

    conn_str = PrefectSecret("moc_pgdb_conn")
    
    tsx_imb_df_lst = partition_df(tsx_imb_df, n_conn)

    df_shape = df_to_db.map(tsx_imb_df_lst, tbl_name=unmapped(imb_tbl_nm), conn_str=unmapped(conn_str))

if __name__ == "__main__":

    # Inputs
    tsx_url = 'https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html'
    backup_url = "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"

    # Script
    from prefect.engine.executors import LocalExecutor

    #tsx_imb_fl.visualize()
    fl_state = tsx_imb_fl.run(
        parameters=dict(
            tsx_url=backup_url,
            n_conn=15
        ), 
        executor=LocalExecutor()

    )
    tsx_imb_fl.visualize(flow_state=fl_state)