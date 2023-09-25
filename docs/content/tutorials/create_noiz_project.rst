Creation of a Noiz project
**************************

This tutorial provides a step-by-step guide to learning how to create a Noiz project

Folder creation
================

First of all, a folder that will contain the entire project.
In this tutorial we have stored the main code, data and results in the same directory.
This is of course not mandatory.

::

     mkdir demo
     cd demo

Then we have to copy the dataset:

::

    git clone git@gitlab.com:noiz-group/noiz-tutorial-dataset.git

Checkout might take some time, the dataset is few GBs big.

Noiz loading
============

The user have to clone the gitlab repository:

::

    git clone git@gitlab.com:noiz-group/noiz-deployment.git

Once, the gitlab repository has been created, the user needs to go into the noiz-deployment folder.

::

    cd noiz-deployment


docker-compose.yml modification
===============================

The docker-compose.yml have to be modified:

- ports:
    - "5432:5432" must be modified into "5436:5432"


- volumes:
    - ./noiz-postgres-data:/var/lib/postgresql/data/pgdata must be modified into ./noiz-postgres-data:/var/lib/postgresql/data

- ports:
    - "8020:8080"

- ports:
      - "5000:5000"  # Flask port        must be modified into "5050:5050" 
      - "5088:8888"  # Jupyterlab port   must be modified into "5053:8853"

- command: jupyter lab --no-browser --ip=0.0.0.0 --port=8888 --allow-root
becomes command: jupyter lab --no-browser --ip=0.0.0.0 --port=8853 --allow-root

-    volumes:
      - ./processed-data-dir:/processed-data-dir
      - ./SDS:/SDS
      - ./noiz:/noiz
becomes:
    volumes:
      - /home/XXX/demo/processed-data-dir:/processed-data-dir
      - /home/XXX/demo/noiz-tutorial-dataset/dataset:/SDS
      - ./noiz:/noiz

Pull Noiz source code
=====================

The user has to "up" the container

::
    
    docker-compose pull
    git clone https://gitlab.com/noiz-group/noiz.git
    docker-compose -p noiz_nameuser up -d
    docker exec -it noiz_nameuser_noiz_1 /bin/bash
    
