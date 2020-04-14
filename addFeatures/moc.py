import pandas as pd
import datetime as dt

def pre_moc_volume(intraday_df, date_clmn_nm, yhoo_sym_clmn_nm):

    # 1. Get pre moc volume
    vol_df = intraday_df[intraday_df[date_clmn_nm].dt.time.between(
        dt.time(15,40,0),
        dt.time(16,40,0)
        )].groupby([date_clmn_nm, yhoo_sym_clmn_nm], as_index=False)["volume"].sum()
    
    vol_df.rename(columns = {'volume':'pre_moc_volume'}, inplace = True)
    
    return vol_df


def basic_pnls(df):
    # various returns
    df["price_change"] = df["imbalance_reference_price"]-df["close"]
    df["return"] = df["price_change"]/df["imbalance_reference_price"]

    df["moc_return"] = df.apply(
        lambda row: -1*row["return"] if row['imbalance_side']=="SELL" else row["return"],
        axis=1 )

    df["moc_price_change"] = df.apply(
        lambda row: -1*row["price_change"] if row['imbalance_side']=="SELL" else row["price_change"],
         axis=1 )

    return df

