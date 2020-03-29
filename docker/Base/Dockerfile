ARG PYTHON_VERSION=3.7.4
ARG DEBIAN_VERSION=buster
FROM python:${PYTHON_VERSION}-slim-${DEBIAN_VERSION} as base
LABEL authors="Scott Zelenka <https://github.com/szelenka>"

# No interactive frontend during docker build
ENV DEBIAN_FRONTEND=noninteractive \
    DEBCONF_NONINTERACTIVE_SEEN=true

#========================
# Miscellaneous packages
# Helper packages to setup the base image
#========================
RUN set -ex \
  && echo ">>> Core packages Setup" \
  && apt-get -yqq update \
  && apt-get -yqq dist-upgrade \
  && apt-get -yqq --no-install-recommends install \
    bzip2 \
    ca-certificates \
    tzdata \
    sudo \
    unzip \
    wget \
    jq \
    curl \
    supervisor \
    gnupg2 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

#========================
# Java packages
# Includes minimal runtime used for executing non GUI Java programs
#========================
RUN set -ex \
  && echo ">>> Java JRE Setup" \
  && buildDeps='\
    apt-transport-https \
    dirmngr \
    software-properties-common \
  ' \
  && apt-get update -yqq \
  && apt-get -yqq install $buildDeps \
  && wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
  && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
  && mkdir -p /usr/share/man/man1 \
  && apt-get update -yqq \
  && apt-get -yqq install \
    adoptopenjdk-8-hotspot \
  && sed -i 's/securerandom\.source=file:\/dev\/random/securerandom\.source=file:\/dev\/urandom/' ./usr/lib/jvm/adoptopenjdk-8-hotspot-amd64/jre/lib/security/java.security \
  && apt-get purge --auto-remove -yqq $buildDeps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

#===================
# Timezone settings
# Possible alternative: https://github.com/docker/docker/issues/3359#issuecomment-32150214
#===================
ENV TZ "UTC"
RUN set -ex \
  && echo ">>> Timezone Setup" \
  && echo "${TZ}" > /etc/timezone \
  && dpkg-reconfigure --frontend noninteractive tzdata

#========================================
# Add normal user with passwordless sudo
#========================================
RUN set -ex \
  && echo ">>> passwordless SUDO Setup" \
  && useradd seluser \
         --shell /bin/bash  \
         --create-home \
  && usermod -a -G sudo seluser \
  && echo 'ALL ALL = (ALL) NOPASSWD: ALL' >> /etc/sudoers \
  && echo 'seluser:secret' | chpasswd
ENV HOME=/home/seluser

#======================================
# Add Grid check script
#======================================
COPY check-grid.sh entry_point.sh /opt/bin/
RUN set -ex \
  && chmod 777 /opt/bin/check-grid.sh /opt/bin/entry_point.sh

#======================================
# Add Supervisor configuration file
#======================================
COPY supervisord.conf /etc

#==========
# Selenium & relaxing permissions for OpenShift and other non-sudo environments
#==========
ARG SELENIUM_VERSION=3.141.59
RUN set -ex \
  && echo ">>> Selenium Setup" \
  && SELENIUM_MAJOR=$(echo ${SELENIUM_VERSION} | cut -d'.' -f 1-2) \
  && mkdir -p /opt/selenium /var/run/supervisor /var/log/supervisor \
  && touch /opt/selenium/config.json \
  && chmod -R 777 /opt/selenium /var/run/supervisor /var/log/supervisor /etc/passwd \
  && wget --no-verbose https://selenium-release.storage.googleapis.com/${SELENIUM_MAJOR}/selenium-server-standalone-${SELENIUM_VERSION}.jar \
    -O /opt/selenium/selenium-server-standalone.jar \
  && chgrp -R 0 /opt/selenium ${HOME} /var/run/supervisor /var/log/supervisor \
  && chmod -R g=u /opt/selenium ${HOME} /var/run/supervisor /var/log/supervisor

#===================================================
# Run the following commands as non-privileged user
#===================================================
USER seluser


CMD ["/opt/bin/entry_point.sh"]