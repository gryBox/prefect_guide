from prefect import Task

import pandas as pd
import yfinance as yf



class DailyData(Task):
    def __init__(
        self,
        symbol_clmn="Symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        prfrd_pattern=".PR."
        ):
        self.symbol_clmn = symbol_clmn
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.prfrd_pattern = prfrd_pattern


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

    def get_ohlc(self, row):
        sym = yf.Ticker(row["yahoo_symbol"])
        
        st_dt = row["moc_date"]
        end_dt = st_dt + timedelta(days=1)
        
        df = sym.history(
                start=st_dt.strftime('%Y-%m-%d'), 
                end=end_dt.strftime('%Y-%m-%d'), 
                auto_adjust=True
            ).head(1)
        
        # Add symbolto ohlc
        df["yahoo_symbol"] = row["yahoo_symbol"]

        return df

    def run(self, df):
        
        # 1. Map TSX symbols to yhoo
        df[self.yhoo_sym_clmn_nm] = df[self.symbol_clmn].apply(self.map_tsx_to_yhoo_sym)

        return df

