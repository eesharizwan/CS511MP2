####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM eclipse-temurin:8-jdk-jammy

RUN apt update && \
    apt upgrade --yes && \
    apt install ssh openssh-server python3 --yes

COPY resources/configure-heartbeats.py /configure-heartbeats.py

# Setup common SSH key.
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/shared_rsa -C common && \
    cat ~/.ssh/shared_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark resources here
# (the PySpark skeletons of Parts 1-4 also need python3 on every node; Spark 3.4.1
#  supports Python 3.7-3.11, so do not install a newer interpreter)

RUN apt install --yes wget

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xzf hadoop-3.3.6.tar.gz -C /opt \
    && rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV HADOOP_CONF_DIR=/opt/hadoop-3.3.6/etc/hadoop
ENV PATH=$PATH:/opt/hadoop-3.3.6/bin:/opt/hadoop-3.3.6/sbin
ENV JAVA_HOME=/opt/java/openjdk

COPY hadoop-config/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY hadoop-config/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY hadoop-config/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh
COPY hadoop-config/workers       $HADOOP_CONF_DIR/workers

RUN wget -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt \
    && rm spark-3.4.1-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark-3.4.1-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY spark-config/spark-env.sh $SPARK_HOME/conf/spark-env.sh
