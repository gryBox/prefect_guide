from prefect import task

import pandas as pd
import s3fs



import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



@task
def get_moc_file_lst(bucket_nm):
    fs = s3fs.S3FileSystem(anon=False)
    moc_file_lst = fs.ls(bucket_nm)

    return moc_file_lst

@task
def read_file(flpth):
    raise NotImplemented

    return moc_df_lst