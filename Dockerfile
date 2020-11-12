#LABEL name="noiz"
#LABEL description="Base image for Noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.4"
#LABEL date="2019.12.16"
#LABEL schema-version="1.0.0"

FROM python:3.8
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

RUN --mount=type=ssh git clone git@gitlab.com:noiz-group/mseedindex.git /mseedindex && \
  cd /mseedindex && \
  git checkout 2c7620f7727033c67140e430078a8130dab36ba5 && \
  make clean && \
  CFLAGS='-I/usr/include/postgresql/' make
ENV MSEEDINDEX_EXECUTABLE="/mseedindex/mseedindex"

RUN mkdir /noiz
WORKDIR /noiz
COPY ./ /noiz/

RUN cd /noiz/ && \
  python -m pip install -r requirements.txt --no-cache-dir && \
  python -m pip install jupyterlab --no-cache-dir && \
  pip install -e .
ENV FLASK_APP="autoapp.py"
