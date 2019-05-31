FROM continuumio/miniconda3:latest
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get upgrade -y
RUN  /opt/conda/bin/conda install -c conda-forge -c heavelock -y click flask flask-sqlalchemy \
 flask-migrate scipy numpy matplotlib obspy \
 dash plotly psycopg2 environs pytest black pre_commit

