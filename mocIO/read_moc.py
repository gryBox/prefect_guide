from prefect import task

import pandas as pd
import s3fs



import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



@task
def bucket_to_df_lst(bucket_nm):
    fs = s3fs.S3FileSystem(anon=False)
    fs.ls(bucket_nm)

    return moc_df_lst

@task
def read_file(flpth):
    fs = s3fs.S3FileSystem(anon=False)
    fs.ls(bucket_nm)

    return moc_df_lst