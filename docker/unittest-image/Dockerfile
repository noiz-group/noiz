# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

FROM ubuntu:22.04

## Main python version to install + use for tox
ARG PYTHON_MAIN_VERSION=3.9
## Other python versions to install
# Must be available either in the deadsnakes PPA or in
# the official Ubuntu repositories
ARG PYTHON_OTHER_VERSIONS="3.10 3.11 3.12"
## GPG key for the deadsnakes PPA
# See https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa
ARG DEADSNAKES_GPG_KEY=F23C5A6CF475977595C89F51BA6932366A755776


ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive

# Install common build dependencies, add deadsnakes PPA and cleanup.
# (see https://github.com/deadsnakes)
# hadolint ignore=DL3008,SC2086
RUN set -eux \
  ; apt-get update \
  ; apt-get install -y --no-install-recommends \
      ca-certificates \
      g++ \
      gcc \
      git \
      curl \
      bzip2 \
      make \
  ; savedAptMark="$(apt-mark showmanual)" \
  \
  ; apt-get install -y --no-install-recommends \
      dirmngr \
      gnupg \
  \
  ; tmp_home="$(mktemp -d)"; export GNUPGHOME="$tmp_home" \
  ; gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys "$DEADSNAKES_GPG_KEY" \
  ; gpg -o /usr/share/keyrings/deadsnakes.gpg --export "$DEADSNAKES_GPG_KEY" \
  ; echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/deadsnakes.gpg] https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main" >> /etc/apt/sources.list \
  \
  ; apt-mark auto '.*' > /dev/null \
  ; apt-mark manual $savedAptMark \
  ; apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  ; rm -rf /var/lib/apt/lists/*

# Install pip3, all python versions and tox in the main python version
# hadolint ignore=DL3008,SC2086
RUN set -eux \
  ; apt-get update \
  ; apt-get install -y --no-install-recommends python3-pip \
  ; for version in ${PYTHON_OTHER_VERSIONS} ${PYTHON_MAIN_VERSION} \
    ; do \
      apt-get install -y --no-install-recommends \
        python${version} \
        python${version}-dev \
        python${version}-venv \
        python${version}-distutils \
      ; python${version} -m pip install --upgrade pip \
    ; done \
  ; python${PYTHON_MAIN_VERSION} -m pip install --no-cache hatch \
  ; rm -rf /var/lib/apt/lists/*;
