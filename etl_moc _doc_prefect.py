from prefect import Flow, task, Task, Parameter
from prefect.tasks.control_flow import ifelse, merge
from prefect.tasks.secrets.base import PrefectSecret
from prefect.engine.executors import DaskExecutor
#from prefect.engine.result_handlers import LocalResultHandler, S3ResultHandler

from prefect.schedules import Schedule
import pendulum

import boto3

import pandas as pd
import sqlalchemy as sa

from datetime import timedelta
import humps
import requests

from extractMOCData.moc_data import TsxMocData 
from normalize.ticker_symbols import MapTickerSymbols
from addFeatures.daily import DailyData 

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Schedule when to run the script
schedule = Schedule(
    # fire every day
    clocks=[clocks.IntervalClock(
        start_date=pendulum.datetime(2020, 4, 20, tz="America/Toronto"),
        interval=timedelta(days=1)
        )],
    # but only on weekdays
    filters=[filters.is_weekday],
    # and only at 5:15
    or_filters=[
        filters.between_times(pendulum.time(17,15), pendulum.time(17,15)),
    ],
    # and not in January TODO: Add TSX Holidays
    not_filters=[filters.between_dates(1, 1, 1, 31)]
)



@task(max_retries=3, retry_delay=timedelta(seconds=10), result=)
def scrape_tsx_moc(url, put_dir):
    """
    Scrape the TSX website Market on close website. Data only available after 15:40 pm Toronto time
    until 12 am.
    
    Use archived url for testing.       
    "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    """
    mocData = TsxMocData(url, put_dir)
    tsx_moc_df = mocData.scrape_moc_data()
    logger.info("TSX MOC - Number of Symbols' {tsx_moc_df.shape}")
    return tsx_moc_df#.head(3)

@task
def get_1min_ohlc(moc_key_df):
    
    dailyData =  DailyData()
    intraday_df = dailyData.get_yahoo_ohlc_data(moc_key_df, interval='1m')

    return intraday_df.round(3)

@task
def get_eod_price_data(moc_key_df):

    dailyData =  DailyData()

    # 2. Get daily price data 
    eod_price_df = dailyData.get_yahoo_ohlc_data(moc_key_df, interval="1d")
    
    # 3. Filter out duplicates that can arise from AH/adjusted volumes
    eod_price_df = eod_price_df.drop_duplicates(
        [dailyData.date_clmn_nm, dailyData.yhoo_sym_clmn_nm], 
        ignore_index=True
        )
    
    # Set datetime to date
    eod_price_df[dailyData.date_clmn_nm] = eod_price_df[dailyData.date_clmn_nm].dt.date

    
    # 3. Add tsx price data (moc_key_df)
    eod_price_df = moc_key_df.merge(
        eod_price_df,
        how="left", 
        left_on=[dailyData.date_clmn_nm, dailyData.yhoo_sym_clmn_nm], 
        right_on=[dailyData.date_clmn_nm, dailyData.yhoo_sym_clmn_nm]
    ).copy()


    return eod_price_df

@task
def get_sym_info(moc_key_df):
    """
    Input any df that has a list of yahoo symbols
    """
    
    # 1. get ticker info
    dailyData =  DailyData()
    info_df = dailyData.get_sym_info_data(moc_key_df)

    sub_moc_key_df = moc_key_df[[
        dailyData.date_clmn_nm,
        dailyData.yhoo_sym_clmn_nm,
        dailyData.tsx_symbol_clmn_nm
    ]]

    # 3. Add keys
    eod_info_df = sub_moc_key_df.merge(
        info_df,
        how="left", 
        left_on=[dailyData.yhoo_sym_clmn_nm], 
        right_on=[dailyData.yhoo_sym_clmn_nm]
    )
        
    return eod_info_df

@task
def build_moc_data( intraday_df, eod_df, eod_info_df):
    dailyData =  DailyData()
    moc_df = dailyData.prepare_moc_data(intraday_df, eod_df, eod_info_df)

    return moc_df


@task
def df_to_db(df, tbl_name, idx_clmn_lst, conn_str=None):

    # if conn_str is None:
    #     db_creds = get_db_creds()

    df = df.set_index(idx_clmn_lst)

    engine = sa.create_engine(conn_str)
    df.to_sql(
        name=tbl_name,
        con=engine,
        if_exists="append",
        index=True,
        method="multi",
        chunksize=5000
        )
    
    # TODO: Return rows inserted
    return df.shape

with Flow("Extract Transform TSX MOC", schedule=schedule) as etl_moc_flow:


    tsx_url = Parameter("tsx_url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    #pg_conn_str_sec = Parameter("conn_str", default="moc_pgdb_conn")
    
    put_dir = Parameter("put_dir", default="s3://tsx-moc/")
    index_clmn_lst = Parameter("index_clmn_lst", default=["moc_date", "yahoo_symbol"])

    # 1. Scrape the tsx website for the moc
    tsx_moc_df = scrape_tsx_moc(tsx_url, put_dir)

    # # 2. Map tsx symbols to yhoo
    yhooMap =  MapTickerSymbols()
    moc_key_df = yhooMap(tsx_moc_df)

    # # 3. Get Open High _Low Close Data from yahoo
    intraday_df =  get_1min_ohlc(moc_key_df)

    # # 4. Get EOD ohlc and attributes
    eod_price_df = get_eod_price_data(moc_key_df)

    # # 5. Get share short, float and other attributes for a ticker
    eod_info_df = get_sym_info(moc_key_df)
    

    # 6. Craete daily moc table (used for training)
    moc_df = build_moc_data(intraday_df, eod_price_df, eod_info_df)

    conn_str = PrefectSecret("moc_pgdb_conn")

    # 7. Write to db
    num_rows_ins = df_to_db(intraday_df, tbl_name="intraday_prices", idx_clmn_lst=index_clmn_lst,conn_str=conn_str)

    # 8. Write to db
    num_rows_ins = df_to_db(eod_price_df, tbl_name="eod_prices", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)

    # 9. Write to db
    num_rows_ins = df_to_db(moc_df, tbl_name="daily_moc", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)

    # 10. Write to db
    num_rows_ins = df_to_db(eod_info_df, tbl_name="eod_sym_info", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)


if __name__ == "__main__":
    #etl_moc_flow.visualize()
    etl_state = etl_moc_flow.run(
        # parameters=dict(
        #     tsx_url="https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
        # )
        executor=DaskExecutor()
    )
    #etl_moc_flow.visualize(flow_state=etl_state) 

    # s = Secret("moc_pgdb_conn") # create a secret object
    # print(s.exists()) # retrieve its value

#     (base) ilivni@ilivni-UX430UAR:~$ cd ~
# (base) ilivni@ilivni-UX430UAR:~$ cd .prefect
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ touch config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  config.toml  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ nano config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ 
