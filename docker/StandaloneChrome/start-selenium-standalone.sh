#!/usr/bin/env bash

java ${JAVA_OPTS} -cp ${JAVA_CLASSPATH:-"/opt/selenium/*:."} org.openqa.grid.selenium.GridLauncherV3 \
    ${SE_OPTS}