import pandas as pd
import datetime as dt

def pre_moc_volume(intraday_df, date_clmn_nm, yhoo_sym_clmn_nm):

    # 1. Get pre moc volume
    vol_df = intraday_df[intraday_df[date_clmn_nm].dt.time.between(
        dt.time(9,30,0),
        dt.time(15,39,0)
        )]

    # 2. Convert dt to date only. 
    vol_df[date_clmn_nm] = vol_df[date_clmn_nm].dt.date
    
    vol_df = vol_df.groupby([date_clmn_nm, yhoo_sym_clmn_nm], as_index=False)["volume"].sum().copy()
    
    vol_df.rename(columns = {'volume':'pre_moc_volume'}, inplace = True)
    

    return vol_df


def basic_pnls(df):
    # various returns
    df["moc_price_change"] = df["close"] - df["imbalance_reference_price"]
    df["moc_return"] = df["moc_price_change"]/df["imbalance_reference_price"]

    df["go_moc_return"] = df.apply(
        lambda row: -1*row["moc_return"] if row['imbalance_side']=="SELL" else row["moc_return"],
        axis=1 )

    df["go_moc_price_change"] = df.apply(
        lambda row: -1*row["moc_price_change"] if row['imbalance_side']=="SELL" else row["moc_price_change"],
         axis=1 )

    # df["moc_delta"] = df.apply(
    #     lambda row: -1*row["imbalance_size"] if row['imbalance_side']=="SELL" else row["imbalance_size"],
    #         axis=1 
    # )

    return df

