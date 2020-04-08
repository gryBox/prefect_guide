from prefect import Task

import pandas as pd
from datetime import timedelta, datetime

import yfinance as yf

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DailyData(Task):
    def __init__(
        self,
        symbol_clmn_nm="Symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        date_clmn_nm="moc_date",
        prfrd_pattern=".PR."
        ):
        self.symbol_clmn_nm = symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.prfrd_pattern = prfrd_pattern
        self.date_clmn_nm = date_clmn_nm



    def get_yahoo_data(self, grpd_df, st_date, interval):
        #print(row)
        symbol_lst = grpd_df[self.yhoo_sym_clmn_nm].values.tolist()
        
        end_dt = st_date + timedelta(days=1)
        
        df = yf.download(
            symbol_lst, 
            start=st_dt.strftime('%Y-%m-%d'), 
            end=end_dt.strftime('%Y-%m-%d'),
            interval=interval,
            group_by = 'ticker'
        )
            
        # # Add symbol to ohlc
        # df[self.yhoo_sym_clmn_nm] = row[self.yhoo_sym_clmn_nm]

        # try:
        #     df["sector"] = sym.info["sector"]
        #     df["currency"] = sym.info["currency"]
        #     df["marketCap"] = sym.info["marketCap"]
        #     df["sharesShort"] = sym.info["sharesShort"]
        #     df["floatShares"] = sym.info["floatShares"]
        #     df["enterpriseValue"] = sym.info["enterpriseValue"]
        #     df["exchangeTimezoneName"] = sym.info["exchangeTimezoneName"]
        #     df["forwardPE"] = sym.info["forwardPE"]

        # except IndexError as error:

        #     logging.info(f"Error getting info from yahoo for sym {sym.ticker}")

        return df

    def get_eod_data(self, moc_key_df):
        # 1. Get a list of dfs with ohlc and date as index
        # 1. download EOD yahoo data date
        grpd_eod_dfs = moc_key_df.groupby(by=[self.date_clmn_nm])
        
        df_lst = []
        for grp in grpd_eod_dfs:
            df = self.get_eod_data(grp[1], grp[0], interval="1d")
            df_lst.append(df)
        
        eod_df = pd.concat(df_lst, ignore_index=True)


        return eod_df
    

    def run(self, df):
        
        # 1. Map TSX symbols to yhoo
        df[self.yhoo_sym_clmn_nm] = df[self.symbol_clmn_nm].apply(self.map_tsx_to_yhoo_sym)

        # 2. Add ohlc price data
        ohlc_df = self.add_ohlc(df)
        
        # 3. Munge df 
        # Make daily moc data
        daily_moc_df = moc_key_df.merge(
            ohlc_df,
            how="left",
            left_on=[self.date_clmn_nm, self.yhoo_sym_clmn_nm],
            right_on=["Date", self.yhoo_sym_clmn_nm],
            validate="one_to_one"
        )

        # 4. Normalize i.e. drop cols



        return daily_moc_df

if __name__ == "__main__":
    pass