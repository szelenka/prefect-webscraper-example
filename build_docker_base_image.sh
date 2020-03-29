#!/usr/bin/env bash
set -ex
PYTHON_VERISON=3.7.4
SELENIUM_VERSION=3.141.59

docker build \
  --build-arg PYTHON_VERISON=${PYTHON_VERISON} \
  --build-arg SELENIUM_VERSION=${SELENIUM_VERSION} \
  ./docker/Base \
  -f docker/Base/Dockerfile \
  -t szelenka/selenium-python-base:${SELENIUM_VERSION}-${PYTHON_VERISON}
#docker push szelenka/selenium-python-base:${SELENIUM_VERSION}-${PYTHON_VERISON}

docker build \
  --build-arg PYTHON_VERISON=${PYTHON_VERISON} \
  --build-arg SELENIUM_VERSION=${SELENIUM_VERSION} \
  ./docker/NodeBase \
  -f docker/NodeBase/Dockerfile -t szelenka/selenium-python-node-base:${SELENIUM_VERSION}-${PYTHON_VERISON}
#docker push szelenka/selenium-python-node-base:${SELENIUM_VERSION}-${PYTHON_VERISON}

docker build \
  --build-arg PYTHON_VERISON=${PYTHON_VERISON} \
  --build-arg SELENIUM_VERSION=${SELENIUM_VERSION} \
  ./docker/NodeChrome \
  -f docker/NodeChrome/Dockerfile -t szelenka/selenium-python-node-chrome:${SELENIUM_VERSION}-${PYTHON_VERISON}
#docker push szelenka/selenium-python-node-chrome:${SELENIUM_VERSION}-${PYTHON_VERISON}

docker build \
  --build-arg PYTHON_VERISON=${PYTHON_VERISON} \
  --build-arg SELENIUM_VERSION=${SELENIUM_VERSION} \
  ./docker/StandaloneChrome \
  -f docker/StandaloneChrome/Dockerfile -t szelenka/selenium-python-standalone-chrome:${SELENIUM_VERSION}-${PYTHON_VERISON}
#docker push szelenka/selenium-python-standalone-chrome:${SELENIUM_VERSION}-${PYTHON_VERISON}