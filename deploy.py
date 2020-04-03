from prefect import Client
from prefect.environments.storage import Docker

from extractMOCData.moc_data import scrape_tsxmoc_fl


project_nm = "MOC"

scrape_tsxmoc_fl.storage = Docker(
    dockerfile="/home/ilivni/MOC/Dockerfile",
    image_name="scrape-moc",
    image_tag="latest"
    )
scrape_tsxmoc_fl.register(project_name=project_nm)