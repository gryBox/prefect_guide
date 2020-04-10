import requests
import pandas as pd
import datetime as dt
import s3fs
import lxml

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

        self.set_output_flnm()
        self.set_output_filepath()

    def set_output_flnm(self):
    # Set file name and flpth 
        today = pd.Timestamp('today')
        self.output_filename = f"{self.flnm}_{today:%Y%m%d}.json"



    def set_output_filepath(self):
        if self.put_dir is None:
            self.output_filepath = f"{self.output_filename}"
        else:
            self.output_filepath = f"{self.put_dir}{self.output_filename}"




    def retrieve_url(self):
        """
        Given a URL (string), retrieves html and
        returns the html as a string.
        """

        html = requests.get(self.url).content
    
        df_list = pd.read_html(html, header=[0], displayed_only=False)

        moc_df = df_list[-1]
        moc_df["moc_date"] = dt.datetime.today().strftime('%Y%m%d')

        logger.info(f"MOC download shape {moc_df.shape}")

        return moc_df
        

    def clean_tsx_data(self, moc_df, fillna_clmn):

        # When the data is scraped the symbol na gets treated as null
        moc_df[fillna_clmn] =moc_df[fillna_clmn].fillna("NA")

        return moc_df


    def write_tsx_moc(self, df):

        logger.info(f"Writing to file: {self.output_filepath}")
        df.to_json(
            self.output_filepath, 
            orient="records",
            double_precision=5,
            date_unit="s"
            )

        return self.output_filepath

    def scrape_moc_data(self):

        ####  Script
        # 1. Get raw data drom tsx website
        raw_moc_df = self.retrieve_url()

        # 2. Clean
        moc_df = self.clean_tsx_data(raw_moc_df, self.fillna_clmn)

        return moc_df


if __name__ == "__main__":
    
    tsxMocData = TsxMocData()

    moc_df = tsxMocData.scrape_moc_data()