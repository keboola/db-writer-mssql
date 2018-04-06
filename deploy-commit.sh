#!/bin/bash

set -e

KBC_DEVELOPERPORTAL_VENDOR="keboola"
KBC_DEVELOPERPORTAL_APP="keboola.wr-db-mssql-v2"
APP_IMAGE="keboola/db-writer-mssql"

# Obtain the component repository and log in
docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm  \
    -e KBC_DEVELOPERPORTAL_USERNAME \
    -e KBC_DEVELOPERPORTAL_PASSWORD \
    quay.io/keboola/developer-portal-cli-v2:latest \
    ecr:get-repository ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP}`
eval $(docker run --rm \
    -e KBC_DEVELOPERPORTAL_USERNAME \
    -e KBC_DEVELOPERPORTAL_PASSWORD \
    quay.io/keboola/developer-portal-cli-v2:latest \
    ecr:get-login ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP})

# Push to the repository
docker tag ${APP_IMAGE}:latest ${REPOSITORY}:${TRAVIS_COMMIT}
docker push ${REPOSITORY}:${TRAVIS_COMMIT}
