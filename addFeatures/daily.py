from prefect import Task

import pandas as pd
from datetime import timedelta, datetime

import yfinance as yf

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DailyData(object):
    def __init__(
        self,
        symbol_clmn_nm="Symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        date_clmn_nm="moc_date"
        ):
        self.symbol_clmn_nm = symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.date_clmn_nm = date_clmn_nm

    def yhoo_eod_data(self, sym_to_get, st_dt):

        end_dt = st_dt + timedelta(days=1)

        sym = yf.Ticker(sym_to_get)
        yhoo_price_df = sym.history(
            start=st_dt.strftime('%Y-%m-%d'), 
            end=end_dt.strftime('%Y-%m-%d'), 
            auto_adjust=True,
            interval="1d"
        ).head(1)

        # Add symbol to ohlc
        yhoo_price_df[self.yhoo_sym_clmn_nm] = sym_to_get
        yhoo_price_df = yhoo_price_df.reset_index()    
        #print
        try:
            info_df = pd.DataFrame([sym.info])
            # print(info_df["sector"])
            yhoo_eod_df = yhoo_price_df.join(info_df, how="left")
            # print(yhoo_eod_df)
            return yhoo_eod_df

        except IndexError as error:
            logging.info(f"Error getting info from yahoo for sym {sym.ticker}")
            

            return yhoo_price_df

    def get_eod_data(self, moc_key_df):
        grpd_dates = moc_key_df.groupby(by=self.date_clmn_nm)

        df_lst = []
        for date_grp in grpd_dates:
            
            # 1. List of symbols to get
            tickers = date_grp[1][ self.yhoo_sym_clmn_nm]
            
            yhoo_eod_df_lst = [self.yhoo_eod_data(sym, date_grp[0]) for sym in tickers]
            
            yhoo_eod_df = pd.concat(yhoo_eod_df_lst, ignore_index=True)
            
            df_lst.append(yhoo_eod_df)

        eod_df = pd.concat(df_lst, ignore_index=True)

        eod_moc_df = moc_key_df.merge(
            eod_df, 
            how="left", 
            left_on=[self.yhoo_sym_clmn_nm, self.date_clmn_nm],
            right_on=[self.yhoo_sym_clmn_nm, "Date"]
        )

        return eod_moc_df

    def get_intraday_data(self, moc_key_df):
        # 1. Get a list of dfs with ohlc and date as index
        # 1. download EOD yahoo data date
        grpd_intraday_dfs = moc_key_df.groupby(by=[self.date_clmn_nm])
        
        df_lst = []
        for grp in grpd_intraday_dfs:
            # 1. Ge
            symbol_lst = grp[1][self.yhoo_sym_clmn_nm].values.tolist()
            st_date = grp[0]
            
            end_dt = st_date + timedelta(days=1)
            
            df = yf.download(
                symbol_lst, 
                start=st_date.strftime('%Y-%m-%d'), 
                end=end_dt.strftime('%Y-%m-%d'),
                interval='1m',
                group_by = 'ticker'
            )

            df_lst.append(df)
        
        ohlc_1min_df = pd.concat(df_lst, ignore_index=True)


        return ohlc_1min_df

if __name__ == "__main__":
    pass