import requests
import pandas as pd
import datetime as dt

import CONFIG as cfg

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



class TsxMocData(object):
    def __init__(
        self,
        url='https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html',
        flnm="moc_tsx",
        put_dir="s3://tsx-moc/",
        fillna_clmn="Symbol"
    ):

    self.url = url
    self.flnm = flnm
    self.put_dir = put_dir
    self.fillna_clmn = fillna_clmn

    self.set_put_flpth()

    def retrieve_url(url):
        """
        Given a URL (string), retrieves html and
        returns the html as a string.
        """

        html = requests.get(url).content
    
        df_list = pd.read_html(html, header=[0], displayed_only=False)

        moc_df = df_list[-1]
        moc_df["moc_date"] = dt.datetime.today().strftime('%Y%m%d')

        logger.info(f"MOC download shape {moc_df.shape}")

        return moc_df

    def set_put_flpth(self):
        
        today = pd.Timestamp('today')
        self.output_filename = f"{self.flnm}_{today:%Y%m%d}.csv"
        
        if artifact_dir is None:
            self.output_filepath = f"{self.output_filename}"
        else:
            self.output_filepath = f"{artifact_dir}{output_filename}"
        

    def clean_tsx_data(moc_df):

        # When the data is scraped the symbol na gets treated as null
        moc_df[replace_na_clmn] =moc_df[replace_na_clmn].fillna("NA")

        return moc_df


    def write_tsx_moc(df, flnm: str, artifact_dir=None):

        logger.info(f"Writing to file: {self.output_filepath}")
        df.to_csv(self.output_filepath, index=False)

        return df.shape

    def scrape_moc_data(self):

        ####  Script
        # 1. Get raw data drom tsx website
        raw_moc_df = retrieve_url(tsx_moc_url)

        # 2. Clean
        moc_df = clean_tsx_data(raw_moc_df, replace_na_clmn)

        return moc_df



if __name__ == "__main__":
    
    pass
