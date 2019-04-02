FROM ubuntu:18.04

COPY ["TIC preprocessing-assembly-1.0.jar", "TIC preprocessing-assembly-1.0.jar"]
COPY ["HEAL data mapping.csv", "HEAL data mapping.csv"]

RUN mkdir data

RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install pause pandas

RUN apt-get install -y wget
RUN wget http://apache.spinellicreations.com/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar zxvf spark-2.4.0-bin-hadoop2.7.tgz
RUN apt-get install -y openjdk-11-jdk
ENV PATH="/spark-2.4.0-bin-hadoop2.7/bin:${PATH}"

ENTRYPOINT ["python3", "reload.py"]

