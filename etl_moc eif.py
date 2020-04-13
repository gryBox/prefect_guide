from prefect import Flow, task, Task, Parameter, unmapped
from prefect.tasks.control_flow import ifelse, merge

import sqlalchemy as sa
import humps

from extractMOCData.moc_data import TsxMocData 
from normalize.ticker_symbols import TsxToYhoo
from addFeatures.daily import DailyData 

engine = sa.create_engine("postgresql+psycopg2://dbmasteruser:mayal1vn1$@ls-ff3a819f9545d450aca1b66a4ee15e343fc84280.cenjiqfifwt6.us-east-2.rds.amazonaws.com/mocdb")

@task
def load_tsx_moc_data(file_to_get, parse_dates=["moc_date"]):
    return  pd.read_csv(file_to_get, parse_dates=parse_dates, na_filter=False)

@task
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
def df_to_db(df, tbl_name, idx_clmn_lst, engine=engine):

    df = df.set_index(idx_clmn_lst)

    df.to_sql(
        name=tbl_name,
        con=engine,
        if_exists="append",
        index=True,
        method="multi"
        )
    
    
    return df.shape

with Flow("Prepare load db data") as etl_moc_flow:
    
    files_to_get_lst = Parameter("files_to_get_lst", default=None)
    tsx_url = Parameter("url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    put_dir = Parameter("put_dir", default="s3://tsx-moc/")
    index_clmn_lst = Parameter("index_clmn_lst", default=["moc_date", "yahoo_symbol"])

    tsx_moc_df = scrape_tsx_moc(tsx_url, put_dir)
    
    df_lst = load_tsx_moc_data.map(files_to_get_lst)
    
    ifelse(
        files_to_get_lst.is_equal(None),
        tsx_moc_df,
        df_lst
    )

    mod_df_lst = merge([tsx_moc_df], df_lst)

    # 2. Map tsx symbols to yhoo
    yhooMap =  TsxToYhoo()
    moc_key_df_lst = yhooMap.map(mod_df_lst)

    # 3. Download 1min day bars
    intraday_df_lst = get_1min_ohlc.map(moc_key_df_lst)

    # 4. Get EOD ohlc and attributes
    eod_df_lst = get_eod_features.map(moc_key_df_lst)

    # 5. Write to db
    num_rows_ins = df_to_db.map(intraday_df_lst, tbl_name="intraday_prices", idx_clmn_lst=index_clmn_lst, engine=engine)

    # 6. Write to db
    num_rows_ins = df_to_db.map(eod_df_lst, tbl_name="eod", idx_clmn_lst=index_clmn_lst, engine=engine)


if __name__ == "__main__":
    etl_moc_flow.visualize()