#!/usr/bin/env bash
set -ex
PYTHON_VERISON=3.7.4
PREFECT_VERSION=0.9.8

docker build \
  --build-arg PYTHON_VERISON=${PYTHON_VERISON} \
  --build-arg PREFECT_VERSION=${PREFECT_VERSION} \
  . \
  -f ./Dockerfile -t szelenka/python-selenium-chromium:${PYTHON_VERISON}
docker push szelenka/python-selenium-chromium:${PYTHON_VERISON}