FROM continuumio/miniconda:latest
ENV PYTHONUNBUFFERED 1
RUN mkdir /noiz
WORKDIR /noiz
COPY . .
RUN /opt/conda/bin/conda install -c conda-forge --file requirements.txt -y
RUN pip install -e .