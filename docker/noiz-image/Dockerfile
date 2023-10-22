# syntax=docker/dockerfile:experimental

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.


FROM python:3.9-buster
LABEL name="noiz"
LABEL description="Base image for Noiz"
LABEL maintainer="Damian Kula, dkula@unistra.fr"
LABEL version="0.7.4"
LABEL date="2020.11.12"

ENV PYTHONUNBUFFERED 1

RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        openssh-client \
        git \
        vim \
        curl \
        libpq-dev && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

RUN git clone https://github.com/EarthScope/mseedindex.git /mseedindex && \
  cd /mseedindex && \
  git checkout e8853ccc4c685af399d1c8e5b8136cb2a15b09b1 && \
  make clean && \
  CFLAGS='-I/usr/include/postgresql/' make
ENV MSEEDINDEX_EXECUTABLE="/mseedindex/mseedindex"

RUN curl https://www.ietf.org/timezones/data/leap-seconds.list -o /leap-seconds.list
ENV LIBMSEED_LEAPSECOND_FILE="/leap-seconds.list"

RUN mkdir /noiz
WORKDIR /noiz
COPY ../.. /noiz/

RUN cd /noiz/ && \
  python -m pip install -r requirements.txt --no-cache-dir && \
  python -m pip install jupyterlab --no-cache-dir && \
  pip install -e .
ENV FLASK_APP="autoapp.py"
