from prefect import Task

import pandas as pd
from pandas.core.common import SettingWithCopyWarning
import warnings
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

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

    def get_yahoo_sym_info(self, sym_to_get):

        try:
            info_df = pd.DataFrame([sym_to_get.info])
       
            # print(yhoo_eod_df)
            return info_df

        except (IndexError, ValueError, KeyError) as error:
            logging.info(f"Error getting info from yahoo for sym {sym_to_get.ticker}")
            
            return None

    def get_sym_info_data(self, moc_key_df):

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

    def get_yahoo_ohlc_data(self, moc_key_df, interval):
        # 1. Get a list of dfs with ohlc and date as index
        grpd_moc_dts = moc_key_df.groupby(by=[self.date_clmn_nm])
        
        df_lst = []
        for grp in grpd_moc_dts:
            # 1. Get  lsit of symbols
            symbol_lst = grp[1][self.yhoo_sym_clmn_nm].values.tolist()

            # 2. Date to get
            st_date = grp[0]#.date()
            print(st_date)
            df = yf.download(
                    symbol_lst, 
                    start=st_date, 
                    end=st_date + timedelta(days=1), 
                    interval=interval
                    #group_by = 'ticker'
                )
            df = df.stack(dropna=False).reset_index().rename(columns={
                'level_1': self.yhoo_sym_clmn_nm,
                "Datetime": self.date_clmn_nm,
                "Date": self.date_clmn_nm 
            })
            #print(df.columns)
            # Filter out some bad dates
            df = df[df[self.date_clmn_nm].dt.date==st_date]

            df_lst.append(df.round(4))
        
        ohlc_df = pd.concat(df_lst, ignore_index=True)

        ohlc_df.rename(columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), inplace=True)

        #ohlc_1min_df = ohlc_1min_df.astype({'volume': 'int'}).dtypes
        
        return ohlc_df

    def prepare_moc_data(self, intraday_df, eod_price_df, eod_info_df):
        
        # Merge price and info 
        eod_df = eod_price_df.merge(
            eod_info_df, 
            how="left", 
            left_on=[self.yhoo_sym_clmn_nm], 
            right_on=[self.yhoo_sym_clmn_nm],
        )
        
        # 1. Get pre moc volume
        vol_df = mocft.pre_moc_volume(intraday_df, self.date_clmn_nm, self.yhoo_sym_clmn_nm )

        # 2. Filter columns for base moc
        moc_df = eod_df[[
            self.tsx_symbol_clmn_nm, self.date_clmn_nm, 'imbalance_side','imbalance_size',
            'imbalance_reference_price', self.yhoo_sym_clmn_nm, "close", "shares_outstanding",
            "shares_short", "sector", "held_percent_institutions", "book_value"]]
    
        # # 3. Merge 
        moc_df = moc_df.merge(vol_df, on=[self.date_clmn_nm, self.yhoo_sym_clmn_nm], how="left")
        
        # 4. drop rows with na 
        moc_df.dropna(axis=0, how="any", subset=["close", "pre_moc_volume"], inplace=True)
        
        # 5. Add some basic features
        moc_df = mocft.basic_pnls(moc_df)
        moc_df["pre_moc_mkt_cap"] = moc_df["imbalance_reference_price"]*moc_df["shares_outstanding"]

        
        return moc_df

    def prepare_pre_moc_data(self):
        return pre_moc_df


if __name__ == "__main__":
    pass