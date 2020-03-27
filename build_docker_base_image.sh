#!/usr/bin/env bash

docker build . -f Dockerfile -t szelenka/selenium-chrome:latest
docker push szelenka/selenium-chrome:latest
