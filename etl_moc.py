from prefect import Flow, task, Task, Parameter

from extractMOCData.moc_data import TsxMocData 


@task
def get_tsx_moc():
    tsxMoc = TsxMocData()
    moc_df = tsxMoc.scrape_moc_data()
    return moc_df



with Flow("Prepare load db data"):
    moc_df = get_tsx_moc()



if __name__ == "__main__":
    pass