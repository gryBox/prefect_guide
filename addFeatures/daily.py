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

    def map_tsx_to_yhoo_sym(self, tsx_sym):
        #print(tsx_sym)
        # Check for prefereds 
        if tsx_sym.find(self.prfrd_pattern)!=-1:
            print(tsx_sym)
            pr_parts = tsx_sym.partition(self.prfrd_pattern)
            yhoo_sym = f"{pr_parts[0]}-{pr_parts[1][1]}{pr_parts[2]}.TO"
        else:
            # Replace equity extensions (i.e. UN, PR)
            yhoo_sym = tsx_sym.replace(".", "-")

            # Add yahoo TSX key
            yhoo_sym = f"{yhoo_sym}.TO"

        return yhoo_sym

    def get_yhoo_ohlc(self, row):
        #print(row)
        sym = yf.Ticker(row[self.yhoo_sym_clmn_nm])
        
        st_dt = row[self.date_clmn_nm]
        end_dt = st_dt + timedelta(days=1)
        
        df = sym.history(
                start=st_dt.strftime('%Y-%m-%d'), 
                end=end_dt.strftime('%Y-%m-%d'), 
                auto_adjust=True
            ).head(1)
        
        # Add symbol to ohlc
        df[self.yhoo_sym_clmn_nm] = row[self.yhoo_sym_clmn_nm]

        try:
            df["sector"] = sym.info["sector"]
            df["currency"] = sym.info["currency"]
            df["marketCap"] = sym.info["marketCap"]
            df["sharesShort"] = sym.info["sharesShort"]
            df["floatShares"] = sym.info["floatShares"]
            df["enterpriseValue"] = sym.info["enterpriseValue"]
            df["exchangeTimezoneName"] = sym.info["exchangeTimezoneName"]
            df["forwardPE"] = sym.info["forwardPE"]

        except IndexError as error:

            logging.info(f"Error getting info from yahoo for sym {sym.ticker}")

        return df

    def add_ohlc(self, moc_key_df):
        # 1. Get a list of dfs with ohlc and date as index
        ohlc_df_lst = moc_key_df.apply(self.get_yhoo_ohlc, axis=1)
        df_lst = [df for df in ohlc_df_lst]
        
        # 2. Munge df into one ohlc frame
        ohlc_df = pd.concat(df_lst, axis=0).reset_index()

        return ohlc_df
    
    def normalize_daily(self, df):
        return

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