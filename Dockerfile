FROM apache/airflow:2.7.1-python3.9

RUN pip install apache-airflow-providers-apache-spark

USER root

###############################
## JAVA installation
###############################
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
RUN export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark
ENV SPARK_VERSION 3.4.1

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
    curl -o "spark-${SPARK_VERSION}-bin-hadoop3.tgz" "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    mkdir -p "${SPARK_HOME}/bin" && \
    mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop3/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop3/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    rm -rf "spark-${SPARK_VERSION}-bin-hadoop3/"

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin
###############################
## Install MongoDB Tools
###############################
RUN command sudo apt-get update
RUN cd "/tmp" && \
    curl -o "mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" && \
    apt-get install "./mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb" && \
    rm "./mongodb-database-tools-ubuntu1604-x86_64-100.8.0.deb"
USER airflow
# Install the required Python packages, including google_drive_downloader
RUN pip install googledrivedownloader