FROM prefecthq/prefect:0.11.2-python3.7

COPY ./ /MOC/
WORKDIR /MOC

ENV PYTHONPATH /MOC

RUN apt update && apt install build-essential -y build-essential libpq-dev postgresql-client postgresql-client-common && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install curl -y



# Expose the PostgreSQL port
EXPOSE 5432


EXPOSE 80


# https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder
RUN pip install .