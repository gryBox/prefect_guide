import json
import os

from extractMOCData.moc_data import TsxMocData

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
          
    url = event.get("url", cfg.TSX_MOC_DATA["EXTRACT_URL"])
    flnm = event.get("flnm", cfg.TSX_MOC_DATA["FLNAME"])
    put_dir= event.get("put_dir", cfg.TSX_MOC_DATA["MOC_DATA_FLPTH"])
    fillna_clmn = event.get("fillna_clmn", cfg.TSX_MOC_DATA["FILL_NA_CLMN"])

    # Scrape amd dump MOC data to s3 bucket
    tsxMocData = TsxMocData(
        url, flnm, put_dir, fillna_clmn
    )
    # Scrape
    moc_df = tsxMocData.scrape_moc_data()

    # Write
    put_flpth = tsxMocData.write_tsx_moc(moc_df)

    return {
        "put_flpth": put_flpth
    }