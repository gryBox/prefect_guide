import pandas as pd
import requests
import datetime 





import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_tsx_moc_imb(url: str):
    """
    Scrape the TSX website Market on close website. Data only available weekdays after 15:40 pm Toronto time
    until 12 am.
    
    Use archived url for testing.       
    "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"
    """
    
    # 1, Get the html content
    html = requests.get(url).content
    
    # 2. Read all the tables
    df_list = pd.read_html(html, header=[0], na_values=[''], keep_default_na=False)
    
    tsx_imb_df = df_list[-1]
    
    logger.info(f"MOC download shape {tsx_imb_df.shape}")

    return tsx_imb_df

class PrepareLoad(object):
    """
    A class to prepare loading tsx imbalances to the database
    """
    
    def __init__(
        self,
        symbol_clmn = "symbol" ,
        date_clmn = "moc_date",
        dollar_delta_clmn = "dlr_delta",
        idx_clmn_lst = ["moc_date". "symbol"]
        
    ):
        self.symbol_clmn = symbol_clmn
        self.date_clmn = date_clmn
        self.dollar_delta_clmn = dollar_delta_clmn
        self.idx_clmn_lst = idx_clmn_lst
        
    def clean_data(self, df):
        # snake case df columns
        df.columns = ["_".join(nm.split()).lower() for nm in df.columns]
        
        # Handle the symbol NA  (There ae more robust ways to do this but for clarity this was chosen)
        df.loc[df[self.symbol_clmn].isna()==True, self.symbol_clmn]= "NA"
        
        return df 
    
    def add_features(self, df):
        # The tsx does not supply a date with their moc data
        df[self.date_clmn] = datetime.date.today()
        
        # Dollar delta feature
        df[self.dollar_delta_clmn] = df["imbalance_size"] * df["imbalance_reference_price"]
        df[self.dollar_delta_clmn] = df[self.dollar_delta_clmn].astype(int)
        return df
    
    def run(self, df):
        
        cleaned_df =  self.clean_data(df)
        imb_df = self.add_features(cleaned_df)
        
        # Set df index
        imb_df = imb_df.set_index(idx_clmn_lst, drop=True, verify_integrity=True).copy()
        
        return imb_df

def df_to_db(df, tbl_name, conn_str=None):

    engine = sa.create_engine(conn_str)
    
    df.to_sql(
        name=tbl_name,
        con=engine,
        if_exists="append",
        index=True,
        method="multi",
        chunksize=5000
        )
    
    engine.dispose()
  
    return df.shape

    
 if __name__ == "__main__":

   # Inputs
   tsx_url = 'https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html'
   backup_url = "https://web.archive.org/web/20200414202757/https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html"

   # Script
   tsx_imb_df =  get_tsx_moc_imb(backup_url)