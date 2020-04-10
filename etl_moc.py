from prefect import Flow, task, Task, Parameter

from extractMOCData.moc_data import TsxMocData 
from normalize.ticker_symbols import TsxToYhoo
from addFeatures.daily import DailyData 

@task
def get_tsx_moc(tsx_url, put_dir):
    tsxMoc = TsxMocData(url=tsx_url, put_dir=put_dir)
    moc_df = tsxMoc.scrape_moc_data()
    return moc_df


@task
def get_1min_ohlc(moc_key_df):
    dailyData =  DailyData()
    intraday_df = dailyData.get_intraday_data(moc_key_df)
    return intraday_df

@task
def get_eod_features(moc_key_df):
    dailyData =  DailyData()
    eod_df = dailyData.get_eod_data(moc_key_df)


    return eod_df

with Flow("Prepare load db data") as etl_moc_flow:
    tsx_url = Parameter("url", default="https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    put_dir = Parameter("put_dir", default="s3://tsx-moc/")


    # 1. Get Moc data from TSX
    moc_df = get_tsx_moc(tsx_url, put_dir)

    # 2. Map tsx symbols to yhoo
    yhooMap =  TsxToYhoo()
    moc_key_df = yhooMap(moc_df)

    # 3. Download 1min day bars
    intraday_df = get_1min_ohlc(moc_key_df)

    # 4. Get EOD ohlc and attributes
    eod_df = get_eod_features(moc_key_df)


if __name__ == "__main__":
    etl_moc_flow.visualize()