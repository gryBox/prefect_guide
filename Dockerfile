FROM python:3.7 


COPY . /extractMOCData
WORKDIR /extractMOCData


RUN apt-get update && apt-get install -y 


RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install -U boto3
# RUN pip install -U prefect