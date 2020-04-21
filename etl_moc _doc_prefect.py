from prefect import Flow, task, Task, Parameter, unmapped
from prefect.tasks.control_flow import ifelse, merge
from prefect.client import Secret

import boto3

import pandas as pd
import sqlalchemy as sa
from datetime import timedelta
import humps
import requests

# from extractMOCData.moc_data import TsxMocData 
# from normalize.ticker_symbols import MapTickerSymbols
# from addFeatures.daily import DailyData 



# dbConn = Secret("moc_pg_db_conn_str")
# conn_str = dbConn.get()



@task(max_retries=3, retry_delay=timedelta(seconds=10))
def scrape_tsx_moc(url, date_clmn_nm="moc_date"):
    """
    Scrape the TSX website Market on close website. Data only available after 15:40 pm Toronto time
    until 12 am.
    
    Use archived url for testing.       
    "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    """

    html = requests.get(url).content

    df_list = pd.read_html(html, header=[0], displayed_only=False)

    moc_df = df_list[-1]
    moc_df[date_clmn_nm] = dt.datetime.today().date()
    
    # One of the symbols is NA 
    moc_df[fillna_clmn] = moc_df[fillna_clmn].fillna("NA")
    
    logger.info(f"MOC download shape {moc_df.shape}")

    # We use 
    return moc_df.head()
    

class MapTickerSymbols(Task):
    def __init__(
        self,
        symbol_clmn_nm="Symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        tsx_sym_clmn_nm="tsx_symbol",
        prfrd_pattern=".PR."
    ):
        super().__init__()

        self.symbol_clmn_nm = symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.tsx_sym_clmn_nm = tsx_sym_clmn_nm
        self.prfrd_pattern = prfrd_pattern

    def map_tsx_to_yhoo_sym(self, tsx_sym):
        #print(tsx_sym)
        # Check for prefereds 
        if tsx_sym.find(self.prfrd_pattern)!=-1:
            #print(tsx_sym)
            pr_parts = tsx_sym.partition(self.prfrd_pattern)
            yhoo_sym = f"{pr_parts[0]}-{pr_parts[1][1]}{pr_parts[2]}.TO"
            #print(yhoo_sym)
        else:
            # Replace equity extensions (i.e. UN, PR)
            yhoo_sym = tsx_sym.replace(".", "-")

            # Add yahoo TSX key
            yhoo_sym = f"{yhoo_sym}.TO"

        return yhoo_sym
    
    def run(self, df):
         # 1. Map TSX symbols to yhoo
        df[self.yhoo_sym_clmn_nm] = df[self.symbol_clmn_nm].apply(self.map_tsx_to_yhoo_sym)
        df.rename(columns={self.symbol_clmn_nm: self.tsx_sym_clmn_nm}, inplace=True)

        # 2. Normalize columns
        df.rename(
            columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), 
            inplace=True
        )

        return df


class GetTickerInfo(Task):
    def __init__(
        self,
        tsx_symbol_clmn_nm="tsx_symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        date_clmn_nm="moc_date"
        ):
        super().__init__()
        self.tsx_symbol_clmn_nm = tsx_symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.date_clmn_nm = date_clmn_nm

    def get_yahoo_sym_info(self, sym_to_get):

        try:
            info_df = pd.DataFrame([sym_to_get.info])
       
            # print(yhoo_eod_df)
            return info_df

        except (IndexError, ValueError, KeyError) as error:
            logging.info(f"Error getting info from yahoo for sym {sym_to_get.ticker}")
            
            return None

    def run(self, moc_key_df):

        # 1.  Get ticker info
        symbol_lst = moc_key_df[self.yhoo_sym_clmn_nm].unique().tolist()
        print(len(symbol_lst))
        tickers = yf.Tickers(symbol_lst)
        
        info_df_lst = [self.get_yahoo_sym_info(sym) for sym in tickers.tickers]

        info_df = pd.concat(info_df_lst, ignore_index=True)
        info_df = info_df.rename(columns={'symbol': self.yhoo_sym_clmn_nm})
        
        info_df.rename(
            columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), 
            inplace=True
            )
        
        return info_df

class GetOhlcData(Task):
    def __init__(
        self,
        tsx_symbol_clmn_nm="tsx_symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        date_clmn_nm="moc_date"
        ):
        super().__init__()
        self.tsx_symbol_clmn_nm = tsx_symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.date_clmn_nm = date_clmn_nm

    def run(self, moc_key_df, interval):
        """
        get_yahoo_ohlc_data
        """
        # 1. Get a list of tickers with ohlc and date as index
        symbol_lst = moc_key_df[self.yhoo_sym_clmn_nm].values.tolist()
        
        df = yf.download(
                tickers=symbol_lst, 
                period="1d", 
                interval=interval
            )
        # 2. Reahape the df and rename some columns
        df = df.stack(dropna=False).reset_index().rename(columns={
            'level_1': self.yhoo_sym_clmn_nm,
            "Datetime": self.date_clmn_nm,
            "Date": self.date_clmn_nm 
        })

        ohlc_df.rename(columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), inplace=True)

        return ohlc_df


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

    engine = sa.create_engine("postgresql://dbmasteruser:mayal1vn1$@ls-ff3a819f9545d450aca1b66a4ee15e343fc84280.cenjiqfifwt6.us-east-2.rds.amazonaws.com/mocdb")
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

with Flow("Prepare load db data") as etl_moc_flow:
    
    conn_str = Parameter("conn_str", default=None)
    
    tsx_url = Parameter("url", default="https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html")
    put_dir = Parameter("put_dir", default="s3://tsx-moc/")
    index_clmn_lst = Parameter("index_clmn_lst", default=["moc_date", "yahoo_symbol"])

    # 1. Scrape the tsx website for the moc
    tsx_moc_df = scrape_tsx_moc(tsx_url, put_dir)

    # 2. Map tsx symbols to yhoo
    yhooMap =  MapTickerSymbols()
    moc_key_df = yhooMap(tsx_moc_df)

    # 3. Get Open High _Low Close Data from yahoo
    yOhlc = GetOhlcData()
    # a. Download 1min day bars
    intraday_df =  get_1min_ohlc(moc_key_df)
    

    # 4. Get EOD ohlc and attributes
    eod_price_df = get_eod_price_data(moc_key_df)

    # 5. Get share short, float and other attributes for a ticker
    tickInfo = GetTickerInfo()
    info_df = tickInfo(moc_key_df)
    
    # 6. 



    # 6. Craete daily moc table (used for training)
    #moc_df = build_moc_data(intraday_df, eod_price_df, eod_info_df)

    # # 7. Write to db
    # num_rows_ins = df_to_db(intraday_df, tbl_name="intraday_prices", idx_clmn_lst=index_clmn_lst)

    # # 8. Write to db
    # num_rows_ins = df_to_db(eod_price_df, tbl_name="eod_prices", idx_clmn_lst=index_clmn_lst)

    # # 9. Write to db
    # num_rows_ins = df_to_db(moc_df, tbl_name="daily_moc", idx_clmn_lst=index_clmn_lst)

    # # 10. Write to db
    # num_rows_ins = df_to_db(eod_info_df, tbl_name="eod_sym_info", idx_clmn_lst=index_clmn_lst)


if __name__ == "__main__":
    # etl_moc_flow.visualize()
    # etl_state = etl_moc_flow.run(
    #     # parameters=dict(
    #     #     tsx_url="https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    #     # )
    # )
    # etl_moc_flow.visualize(flow_state=etl_state) 

    s = Secret("moc_pgdb_conn") # create a secret object
    print(s.exists()) # retrieve its value

#     (base) ilivni@ilivni-UX430UAR:~$ cd ~
# (base) ilivni@ilivni-UX430UAR:~$ cd .prefect
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ touch config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ ls
# client  config.toml  flows  results
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ nano config.toml
# (base) ilivni@ilivni-UX430UAR:~/.prefect$ 
