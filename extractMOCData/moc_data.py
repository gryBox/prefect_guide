import requests
import pandas as pd

import datetime as dt
from datetime import timedelta, datetime
import pendulum

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule, Schedule, filters
from prefect.schedules.clocks import IntervalClock

from extractMOCData import CONFIG as cfg


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)





@task
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


@task
def clean_tsx_data(moc_df, replace_na_clmn):

    # When the data is scraped the symbol na gets treated as null
    moc_df[replace_na_clmn] = moc_df[replace_na_clmn].fillna("NA")

    return moc_df

@task 
def write_tsx_moc(df, flnm: str, artifact_dir=None):
    today = pd.Timestamp('today')
    output_filename = f"{flnm}_{today:%Y%m%d}.csv"

    if artifact_dir is None:
        output_filepath = f"{output_filename}"
    else:
        output_filepath = f"{artifact_dir}{output_filename}"
    
    logger.info(f"Writing to file: {output_filepath}")
    df.to_csv(output_filepath, index=False)

    return output_filepath



schedule = Schedule(
    # fire every Day
    clocks=[
        IntervalClock(
            start_date=pendulum.datetime(2020, 4, 1, 16, 30, tz="America/Toronto"), interval=timedelta(days=1)
        )
    ],
    # but only on weekdays
    filters=[filters.is_weekday],
    # and only at 9am or 3pm
    or_filters=[
        filters.at_time(pendulum.time(16, 30))
    ]
)




with Flow("Scrape-TSX-MOC") as scrape_tsxmoc_fl:

    tsx_moc_url = Parameter("tsx_moc_url", default=cfg.TSX_MOC_DATA["EXTRACT_URL"])
    moc_data_dir = Parameter("moc_data_dir", default=cfg.TSX_MOC_DATA["MOC_DATA_FLPTH"])
    replace_na_clmn = Parameter("replace_na_clmn", default=cfg.TSX_MOC_DATA["FILL_NA_CLMN"])
    flnm = Parameter("flnm", default="moc_tsx")

    ####  Script
    # 1. Scrape
    raw_moc_df = retrieve_url(tsx_moc_url)

    # 2. Clean
    moc_df = clean_tsx_data(raw_moc_df, replace_na_clmn)

    # 3. Load
    flpth = write_tsx_moc(moc_df, flnm, moc_data_dir)

if __name__ == "__main__":
    
    scrape_tsxmoc_fl.visualize()
    scrape_state  = scrape_tsxmoc_fl.run()
    scrape_tsxmoc_fl.visualize(flow_state=scrape_state)
    
    # IntervalClock(
    #     start_date=pendulum.datetime(
    #         2019, 1, 1, tz="America/Toronto"
    #         ), 
    #     interval=timedelta(days=1)
    # )

   