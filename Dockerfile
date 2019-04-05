# need to set the following env vars

# REDCAP_APPLICATION_TOKEN
# POSTGRES_USER
# POSTGRES_PASSWORD
# POSTGRES_HOST
# POSTGRES_PORT
# POSTGRES_DATABASE_NAME
# POSTGRES_DUMP_PATH
# build TIC preprocessing-assembly.jar from https://github.com/RENCI/map-pipeline/tree/0.1.0
# download "HEAL data mapping.csv" from https://github.com/RENCI/HEAL-data-mapping/blob/0.1.0/HEAL%20data%20mapping_finalv5.csv
# need to mount backup dir to POSTGRES_DUMP_PATH
FROM ubuntu:18.04
COPY ["TIC preprocessing-assembly.jar", "TIC preprocessing-assembly.jar"]
COPY ["HEAL data mapping.csv", "HEAL data mapping.csv"]
COPY ["reload.py", "reload.py"]

RUN mkdir data

RUN apt-get update && apt-get install -y wget gnupg

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" >> /etc/apt/sources.list.d/pgdg.list

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update && apt-get install -y python3-pip wget openjdk-11-jdk postgresql-client-11
RUN pip3 install schedule pandas psycopg2-binary csvkit requests

RUN wget http://apache.spinellicreations.com/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz
RUN tar zxvf spark-2.4.1-bin-hadoop2.7.tgz
ENV PATH="/spark-2.4.1-bin-hadoop2.7/bin:${PATH}"
ENV RELOAD_DATABASE=1
ENV RELOAD_SCHEDULE=1

ENTRYPOINT ["python3", "reload.py"]

