FROM python:3.8-slim-buster

LABEL name="noiz-unittest"
LABEL description="Image for testing noiz as well as building docs"
LABEL maintainer="Damian Kula, dkula@unistra.fr"
LABEL version="1.0.0"
LABEL date="2020.11.09"
LABEL schema-version="1.0.0"

COPY  requirements*.txt /

RUN apt-get update -yqq && \
    apt-get install -yqq --no-install-recommends \
        build-essential \
        gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade --no-cache-dir pip setuptools wheel && \
    python -m pip install -r /requirements.txt --no-cache-dir && \
    rm -rv /requirements*.txt
