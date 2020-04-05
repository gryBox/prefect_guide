import pandas as pd
import yfinance as yf



class PriceData(object):
    def __init__(
        self
        ):
        

    def prepare_moc_key_df(self, df, symbol_clmn="symbol"):

        # Create price data _list
        moc_key_df = moc_df moc_df.assign(yahoo_tsx_symbol=moc_df["Symbol"].apply(lambda x: f"{x}.TO"))

        return moc_key_df

     