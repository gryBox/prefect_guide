from prefect import Flow, task, Task, Parameter, unmapped
from prefect.tasks.control_flow import ifelse, merge
from prefect.client import Secret

import sqlalchemy as sa
from datetime import timedelta
import humps

from extractMOCData.moc_data import TsxMocData 
from normalize.ticker_symbols import MapTickerSymbols
from addFeatures.daily import DailyData 

dbConn = Secret("moc_pg_db_conn_str")
conn_str = dbConn.get()

@task(max_retries=3, retry_delay=timedelta(seconds=10))
def scrape_tsx_moc(tsx_url, put_dir):
    tsxMoc = TsxMocData(url=tsx_url, put_dir=put_dir)
    moc_df = tsxMoc.scrape_moc_data()
    return moc_df


@task
def get_1min_ohlc(moc_key_df):
    dailyData =  DailyData()
    intraday_df = dailyData.get_intraday_data(moc_key_df)

    
    return intraday_df.round(3)

@task
def get_eod_data(moc_key_df):
    dailyData =  DailyData()
    eod_df = dailyData.get_eod_data(moc_key_df)

    return eod_df

@task
def build_moc_data(intraday_df, eod_df):
    dailyData =  DailyData()
    moc_df = dailyData.prepare_moc_data(intraday_df, eod_df)

    return moc_df


@task
def df_to_db(df, tbl_name, idx_clmn_lst, conn_str=conn_str):

    df = df.set_index(idx_clmn_lst)

    engine = sa.create_engine(conn_str)
    df.to_sql(
        name=tbl_name,
        con=engine,
        if_exists="append",
        index=True,
        method="multi"
        )
    
    # TODO: Return rows inserted
    return df.shape

with Flow("Prepare load db data") as etl_moc_flow:
    
    conn_str = Parameter("conn_str", default=None)
    
    tsx_url = Parameter("url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    put_dir = Parameter("put_dir", default="s3://tsx-moc/")
    index_clmn_lst = Parameter("index_clmn_lst", default=["moc_date", "yahoo_symbol"])

    # 1. Scrape the tsx website for the moc
    tsx_moc_df = scrape_tsx_moc(tsx_url, put_dir)

    # 2. Map tsx symbols to yhoo
    yhooMap =  MapTickerSymbols()
    moc_key_df = yhooMap(tsx_moc_df)

    # 3. Download 1min day bars
    intraday_df = get_1min_ohlc(moc_key_df)

    # 4. Get EOD ohlc and attributes
    eod_df = get_eod_data(moc_key_df)

    # 5. Craete daily moc table (used for training)
    moc_df = build_moc_data(intraday_df, eod_df)

    # 5. Write to db
    num_rows_ins = df_to_db(intraday_df, tbl_name="intraday_prices", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)

    # 6. Write to db
    num_rows_ins = df_to_db(eod_df, tbl_name="eod", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)

    # 7. Write to db
    num_rows_ins = df_to_db(moc_df, tbl_name="moc_daily", idx_clmn_lst=index_clmn_lst, conn_str=conn_str)


if __name__ == "__main__":
    etl_moc_flow.visualize()
    etl_state = etl_moc_flow.run(
        # parameters=dict(
        #     tsx_url="https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
        # )
    )
    etl_moc_flow.visualize(flow_state=etl_state) 