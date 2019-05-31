#LABEL name="noiz"
#LABEL description="Descirption of noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.1"
#LABEL date="2019.04.02"
#LABEL schema-version="1.0.0"

FROM continuumio/miniconda3:latest
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get upgrade -y
RUN  /opt/conda/bin/conda install -c conda-forge -c heavelock -y \
 click \
 flask \
 flask-sqlalchemy \
 flask-migrate \
 scipy \
 numpy \
 matplotlib \
 obspy \
 dash \
 plotly \
 psycopg2 \
 environs \
 pytest \
 black \
 pre_commit
RUN mkdir /noiz
WORKDIR /noiz
COPY ./ /noiz/
RUN /opt/conda/bin/conda install -c conda-forge -c heavelock --file requirements.txt -y
RUN ls