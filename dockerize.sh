#!/usr/bin/env bash

# wait for MSSQL container to start
export DOCKERIZE_VERSION="v0.3.0"
wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz
dockerize -wait tcp://mssql:1433

sleep 20
