from prefect import Flow, task, Task, Parameter

from extractMOCData.moc_data import TsxMocData 

with Flow("Prepare load db data"):
    pass