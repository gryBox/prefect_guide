from prefect import Task

import pandas as pd
from datetime import timedelta, datetime
import humps

import yfinance as yf

from addFeatures import moc as mocft


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DailyData(object):
    def __init__(
        self,
        tsx_symbol_clmn_nm="tsx_symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        date_clmn_nm="moc_date"
        ):
        self.tsx_symbol_clmn_nm = tsx_symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.date_clmn_nm = date_clmn_nm

    def yhoo_eod_data(self, sym_to_get, st_day):

        # Get historical data
        yhoo_price_df = sym_to_get.history(
            start=st_day.date(), 
            end=st_day.date() + timedelta(days=1), 
            auto_adjust=True,
            interval="1d"
        ).head(1)

        # Add symbol to ohlc
        yhoo_price_df[self.yhoo_sym_clmn_nm] = sym_to_get.ticker
        yhoo_price_df = yhoo_price_df.reset_index()    
        #print
        try:
            info_df = pd.DataFrame([sym_to_get.info])
            # print(info_df["sector"])
            yhoo_eod_df = yhoo_price_df.join(info_df, how="left")
            # print(yhoo_eod_df)
            return yhoo_eod_df

        except (IndexError, ValueError) as error:
            logging.info(f"Error getting info from yahoo for sym {sym_to_get.ticker}")
            

            return yhoo_price_df

    def get_eod_data(self, moc_key_df):
        grpd_dates = moc_key_df.groupby(by=self.date_clmn_nm)

        df_lst = []
        for date_grp in grpd_dates:
            
            # a. List of symbols to get
            symbol_lst = date_grp[1][self.yhoo_sym_clmn_nm].values.tolist()
           
            # b. Initialize yahoo symbols 
            tickers = yf.Tickers(symbol_lst)
            
            # c. Get the data for the tickers
            yhoo_eod_df_lst = [self.yhoo_eod_data(sym, date_grp[0]) for sym in tickers.tickers]
            
            # d. Concat all the ohlc and other features for each symbol
            yhoo_eod_df = pd.concat(yhoo_eod_df_lst, ignore_index=True)
            
            df_lst.append(yhoo_eod_df)

        eod_df = pd.concat(df_lst, ignore_index=True)

        eod_moc_df = moc_key_df.merge(
            eod_df, 
            how="left", 
            left_on=[self.yhoo_sym_clmn_nm, self.date_clmn_nm],
            right_on=[self.yhoo_sym_clmn_nm, "Date"]
        )
        
        eod_moc_df.rename(columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), inplace=True)

        
        return eod_moc_df

    def get_intraday_data(self, moc_key_df):
        # 1. Get a list of dfs with ohlc and date as index
        # 1. download EOD yahoo data date
        grpd_intraday_dfs = moc_key_df.groupby(by=[self.date_clmn_nm])
        
        df_lst = []
        for grp in grpd_intraday_dfs:
            # 1. Get  lsit of symbols
            symbol_lst = grp[1][self.yhoo_sym_clmn_nm].values.tolist()

            # 2. Date to get
            st_date = grp[0]
            
            df = yf.download(
                symbol_lst, 
                start=st_date.date(), 
                end=st_date.date() + timedelta(days=1), 
                interval='1m'
                #group_by = 'ticker'
            )
            df = df.stack(dropna=False).reset_index().rename(columns={
                'level_1': self.yhoo_sym_clmn_nm,
                "Datetime": self.date_clmn_nm
            })
            df_lst.append(df.round(4))
        
        ohlc_1min_df = pd.concat(df_lst, ignore_index=True)

        ohlc_1min_df.rename(columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), inplace=True)
        


        #ohlc_1min_df = ohlc_1min_df.astype({'volume': 'int'}).dtypes
        
        return ohlc_1min_df

    def prepare_moc_data(self, intraday_df, eod_df):
        # self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        # self.date_clmn_nm = date_clmn_nm
        
        # 1. Get pre moc volume
        vol_df = mocft.pre_moc_volume(intraday_df, self.date_clmn_nm, self.yhoo_sym_clmn_nm )

        # 2. Filter columns for base moc
        moc_df = eod_df[[
            self.tsx_symbol_clmn_nm, self.date_clmn_nm, 'imbalance_side','imbalance_size',
            'imbalance_reference_price', self.yhoo_sym_clmn_nm, "close", "shares_outstanding",
            "shares_short", "sector", "held_percent_institutions", "book_value"]]
        
        # # 3. Merge 
        # moc_df[self.date_clmn_nm] = pd.to_datetime(moc_df[self.date_clmn_nm]) \
        #     .dt.tz_localize('America/Toronto') \
        #     .dt.tz_convert('America/Toronto')

        # moc_df = moc_df.merge(vol_df, on=[self.date_clmn_nm, self.yhoo_sym_clmn_nm], how="left")
        
        # # 4. drop rows with na 
        # moc_df.dropna(axis=0, how="any", subset=["close", "pre_moc_volume"], inplace=True)
        
        # # 5. Add some basic features
        # #moc_df = mocft.basic_pnls(moc_df)
        # moc_df["pre_moc_mkt_cap"] = moc_df["imbalance_reference_price"]*moc_df["shares_outstanding"]

        
        return moc_df, vol_df

    def prepare_pre_moc_data(self):
        return pre_moc_df


if __name__ == "__main__":
    pass