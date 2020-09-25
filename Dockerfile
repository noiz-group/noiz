#LABEL name="noiz"
#LABEL description="Base image for Noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.4"
#LABEL date="2019.12.16"
#LABEL schema-version="1.0.0"

FROM python:3.8
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get upgrade -yy
RUN python -m pip install -r requirements.txt --no-cache-dir
RUN mkdir /noiz

WORKDIR /noiz
COPY ./ /noiz/

RUN python -m pip install -r requirements.txt --no-cache-dir && \
 pip install -e .

