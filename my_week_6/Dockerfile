FROM quay.io/jupyter/all-spark-notebook:latest

USER root

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget tar git curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Kafka Python and pandas
RUN pip install kafka-python==2.0.2 pyspark==3.5.1 
# py4j

#ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH" 

ENV SPARK_HOME=/usr/local/spark

# Download and add the Kafka package jar to the Spark jars directory
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -O $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar

RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar -O $SPARK_HOME/jars/kafka-clients-2.8.0.jar

# Switch back to the default non-root user for Bitnami images

# Expose the port Jupyter will run on
EXPOSE 8888

ENTRYPOINT ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--no-browser", "--notebook-dir=/home/jovyan/work"]

# "--notebook-dir=/home/jovyan/work"
