
FROM prefecthq/prefect:0.10.4-python3.7

COPY ./ /MOC/
WORKDIR /MOC

ENV PYTHONPATH /MOC

RUN apt update && apt install build-essential -y build-essential libpq-dev postgresql-client postgresql-client-common && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install curl -y



# Expose the PostgreSQL port
EXPOSE 5432



# https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder
RUN pip install .

RUN pip install pip --upgrade
RUN pip show prefect || pip install git+https://github.com/PrefectHQ/prefect.git@0.10.4#egg=prefect[kubernetes]

RUN pip install pandas sqlalchemy psycopg2 s3fs lxml boto3 humps requests yfinance wheel 

RUN mkdir -p /root/.prefect/
COPY ./tmprw9wmqh4/extract-transform-tsx-moc.flow /root/.prefect/flows/extract-transform-tsx-moc.prefect

COPY ./tmprw9wmqh4/healthcheck.py /root/.prefect/healthcheck.py


ENV PREFECT__USER_CONFIG_PATH=/root/.prefect/config.toml


RUN python /root/.prefect/healthcheck.py '["/root/.prefect/flows/extract-transform-tsx-moc.prefect"]' '(3, 7)'
