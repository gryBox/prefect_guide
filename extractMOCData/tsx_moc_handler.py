import json
import os

from extractMOCData.moc_data import TsxMocData

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
          
        url = event.get("url", cfg.TSX_MOC_DATA 'https://api.tmxmoney.com/mocimbalance/en/TSX/moc.html')
        flnm = event.get("flnm", cfg.TSX_MOC_DATA"moc_tsx")
        put_dir= event.get("put_dir", cfg.TSX_MOC_DATA"s3://tsx-moc/")
        fillna_clmn = event.get("fillna_clmn", cfg.TSX_MOC_DATA"Symbol")

        # Scrape amd dump MOC data to s3 bucket
        tsxMocData = TsxMocData(
          url, flnm, put_dir, fillna_clmn
        )
        # Scrape
        moc_df = tsxMocData.scrape_moc_data()

        # Write

    return json.loads(resp.read().decode())