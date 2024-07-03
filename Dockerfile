FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update

RUN apt-get install -y sudo vim wget curl tar ssh gzip

RUN pip install jupyterlab pandas kafka-python pyarrow findspark

ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /app
