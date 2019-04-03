# need to set the following env vars

# REDCAP_APPLICATION_TOKEN
# POSTGRES_USER
# POSTGRES_PASSWORD
# POSTGRES_HOST
# POSTGRES_DATABASE_NAME
# build TIC preprocessing-assembly-1.0.jar from https://github.com/xu-hao/map-pipeline.git
FROM ubuntu:18.04
COPY ["TIC preprocessing-assembly-1.0.jar", "TIC preprocessing-assembly-1.0.jar"]
COPY ["HEAL data mapping.csv", "HEAL data mapping.csv"]

RUN mkdir data

RUN apt-get update && apt-get install -y python3-pip wget openjdk-11-jdk
RUN pip3 install schedule pandas

RUN wget http://apache.spinellicreations.com/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar zxvf spark-2.4.0-bin-hadoop2.7.tgz
ENV PATH="/spark-2.4.0-bin-hadoop2.7/bin:${PATH}"
ENV RELOAD_DATABASE=1
RUN echo 12 > apt-get install csvkit -y
COPY ["reload.py", "reload.py"]
RUN pip3 install psycopg2

ENTRYPOINT ["python3", "reload.py"]

