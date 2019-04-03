#LABEL name="noiz"
#LABEL description="Descirption of noiz app"
#LABEL maintainer="Damian Kula, dkula@unistra.fr"
#LABEL version="0.0.1"
#LABEL date="2019.04.02"
#LABEL schema-version="1.0.0"

FROM noiz:noizbase

RUN mkdir /noiz
WORKDIR /noiz
COPY . .
RUN /opt/conda/bin/conda install -c conda-forge -c heavelock --file requirements.txt -y