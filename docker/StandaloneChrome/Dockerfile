ARG PYTHON_VERISON=3.7.4
ARG SELENIUM_VERSION=3.141.59

FROM szelenka/selenium-python-node-chrome:${SELENIUM_VERSION}-${PYTHON_VERISON}
LABEL authors="Scott Zelenka <https://github.com/szelenka>"

USER seluser

#====================================
# Scripts to run Selenium Standalone
#====================================
COPY start-selenium-standalone.sh /opt/bin/start-selenium-standalone.sh
RUN set -ex \
  && sudo chmod 777 /opt/bin/start-selenium-standalone.sh

#==============================
# Supervisor configuration file
#==============================
COPY selenium.conf /etc/supervisor/conf.d/


EXPOSE 4444