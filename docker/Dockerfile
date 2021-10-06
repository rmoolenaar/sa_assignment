FROM bitnami/spark

USER root

ARG SBT_VERSION=1.5.5

# Install sbt
RUN \
  mkdir /working/ && \
  cd /working/ && \
  curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  cd && \
  rm -r /working/

RUN useradd --create-home --shell /bin/bash ubuntu

ENV HOME /home/ubuntu

WORKDIR ${HOME}

ADD project.tgz /home/ubuntu/

RUN chown -R ubuntu:ubuntu /home/ubuntu/*
RUN mkdir -p /home/ubuntu/.bsp
RUN chown -R ubuntu:ubuntu /home/ubuntu/.bsp

RUN useradd --home /jobs --user-group --shell /usr/sbin/nologin --uid 1001 spark
RUN apt-get remove -y libnss-wrapper
RUN chown spark:spark /opt/bitnami/spark/conf/

USER ubuntu

RUN sbt sbtVersion
RUN sbt package

USER root
