#LABEL name="noiz"
#LABEL description="Base image for Noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.4"
#LABEL date="2019.12.16"
#LABEL schema-version="1.0.0"

FROM continuumio/miniconda3:latest
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get upgrade -yy
RUN  /opt/conda/bin/conda install \
 -c conda-forge \
 -c heavelock \
 -y \
 python=3.7 \
 celery \
 click \
 environs \
 flask \
 flask-sqlalchemy \
 flask-migrate \
 scipy \
 jupyterlab \
 numpy \
 matplotlib \
 obspy \
 dash \
 pandas \
 plotly \
 psycopg2 \
 utm \
 pytest \
 black \
 pre_commit
RUN mkdir /noiz

WORKDIR /noiz
COPY ./ /noiz/

RUN /opt/conda/bin/conda install -c conda-forge -c heavelock --file requirements.txt -y && \
 pip install -e . && \
 conda clean --all --yes