FROM gitpod/workspace-python:2022-02-04-06-25-23

ENV trigger_rebuild 1
ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_LOCAL_IP=0.0.0.0
# needed for master

USER root
# Install apt packages and clean up cached files
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# Install the AWS CLI and clean up tmp files
RUN wget https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -O ./awscliv2.zip && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf ./aws awscliv2.zip

USER gitpod

# For vscode
EXPOSE 3000
# for spark
EXPOSE 4040
