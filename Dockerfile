FROM centos:7
LABEL authors="data_transfer"

RUN yum update -y  \
    && yum install -y python3 python3-devel jq sshpass git  \
    && pip3 install --upgrade pip  \
    && pip3 install --upgrade setuptools  \
    && pip3 install clickzetta-migration==0.0.0.3

RUN mkdir -p /app/migration/scripts

COPY ./migration/scripts/linux/meta_cmd /app/migration/scripts/