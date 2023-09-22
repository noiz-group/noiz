# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

FROM python:3.8-slim-buster

RUN apt-get update -yqq && \
    apt-get install -yqq --no-install-recommends \
        build-essential \
        gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install hatch
