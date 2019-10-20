#LABEL name="noiz"
#LABEL description="Descirption of noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.4"
#LABEL date="2019.05.31"
#LABEL schema-version="1.0.0"

FROM continuumio/miniconda3:latest
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get upgrade -y
RUN  /opt/conda/bin/conda install -c conda-forge -c heavelock -y \
 celery \
 click \
 flask \
 flask-sqlalchemy \
 flask-migrate \
 scipy \
 numpy \
 matplotlib \
 obspy \
 dash \
 pandas \
 plotly \
 psycopg2 \
 environs \
 redis \
 pytest \
 black \
 pre_commit
RUN mkdir /noiz
WORKDIR /noiz
COPY ./ /noiz/
RUN /opt/conda/bin/conda install -c conda-forge pip -y
RUN /opt/conda/bin/conda install -c conda-forge -c heavelock --file requirements.txt -y
RUN /opt/conda/bin/pip install redis
RUN pip install -e .