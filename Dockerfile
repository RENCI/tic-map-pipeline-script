# version 0.2.0
# need to set the following env vars
# REDCAP_APPLICATION_TOKEN
# POSTGRES_USER
# POSTGRES_PASSWORD
# POSTGRES_HOST
# POSTGRES_PORT
# POSTGRES_DATABASE_NAME
# POSTGRES_DUMP_PATH
# REDCAP_URL_BASE default should be set to https://redcap.vanderbilt.edu/api/
# SCHEDULE_RUN_TIME default should be set to 00:00
# need to mount backup dir to POSTGRES_DUMP_PATH
FROM ubuntu:18.04 AS schema

RUN apt-get update && apt-get install -y wget curl

COPY ["HEAL-data-mapping/HEAL data mapping_finalv6.csv", "HEAL data mapping.csv"]

RUN curl -sSL https://get.haskellstack.org/ | sh
COPY ["map-pipeline-schema", "map-pipeline-schema"]
WORKDIR map-pipeline-schema
RUN stack build
RUN ["stack", "exec", "map-pipeline-schema-exe", "/HEAL data mapping.csv", "/tables.sql"]

FROM ubuntu:18.04 AS transform

RUN apt-get update && apt-get install -y wget openjdk-8-jdk gnupg

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update && apt-get install -y sbt
COPY ["map-pipeline", "map-pipeline"]
WORKDIR map-pipeline
RUN sbt assembly

FROM ubuntu:18.04

RUN mkdir data

RUN apt-get update && apt-get install -y wget gnupg

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" | tee -a /etc/apt/sources.list.d/pgdg.list

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update && apt-get install -y python3-pip wget openjdk-8-jdk postgresql-client-11 git
RUN pip3 install schedule pandas psycopg2-binary csvkit requests flask redis rq
RUN pip3 install git+https://github.com/vaidik/sherlock.git@77742ba91a24f75ee62e1895809901bde018654f

RUN wget http://apache.spinellicreations.com/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && echo "E8B7F9E1DEC868282CADCAD81599038A22F48FB597D44AF1B13FCC76B7DACD2A1CAF431F95E394E1227066087E3CE6C2137C4ABAF60C60076B78F959074FF2AD spark-2.4.3-bin-hadoop2.7.tgz" | sha512sum -c -

RUN tar zxvf spark-2.4.3-bin-hadoop2.7.tgz
ENV PATH="/spark-2.4.3-bin-hadoop2.7/bin:${PATH}"
# set to 1 to reload data from redcap database
ENV RELOAD_DATABASE=1
# set to 1 for one off reload
ENV RELOAD_ONE_OFF=0
# set to 1 to schedule periodic reload
ENV RELOAD_SCHEDULE=1
# set to 1 to create tables
ENV CREATE_TABLES=1
# set to 1 to insert data
ENV INSERT_DATA=0
ENV SERVER=0

COPY ["HEAL-data-mapping/HEAL data mapping_finalv6.csv", "HEAL data mapping.csv"]
COPY --from=schema ["/tables.sql", "data/tables.sql"] 
COPY --from=transform ["map-pipeline/target/scala-2.11/TIC preprocessing-assembly-0.2.0.jar", "TIC preprocessing-assembly.jar"]

COPY ["reload.py", "reload.py"]C
COPY ["server.py", "server.py"]
COPY ["application.py", "application.py"]

ENTRYPOINT ["python3", "application.py"]

