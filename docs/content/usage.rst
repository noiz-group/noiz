Usage
************************************

This is chapter on usage of Noiz package.
It will guide you through installation, first run and basic usage patterns.

System Requirements
====================================

Noiz as a system requires PostgreSQL database.
If you are eager enough, you can install everything on your computer, but I recommend rather using `Docker`_.
In this case, the requirement is just Docker and internet connection that will allow you to connect to the Gitlab's
container registry.
In that case, from theoretical point of view, you can run Noiz on any system you will manage to install Docker on.
This said, I did not test it, I am testing it only on Linux.

If you do not want to use Docker, you have have:

#. `PostgreSQL`_
#. `Python`_
#. `mseedindex`_

Mseedindex dependency will be removed when system of ingesting data will be implemented using Obspy.


Preparing deployment
====================================

The fastest way of deploying noiz is to clone that git repository::

    https://gitlab.com/noiz-group/noiz-deployment

This repository contains an up-to-date `docker-compose.yml` file along with instructions that will help you with
running noiz successfully.

To start, you have to clone the repository::

    git clone git@gitlab.com:noiz-group/noiz-deployment.git
    cd noiz-deployment

After successful cloning, you have to login to Gitlab's docker registry::

    docker login registry.gitlab.com


It will prompt you for giving user and password.
You can either use your gitlab credentials or use something called `deploy token` that is delivered by Gitlab.
I highly recommend using the latter one since they are going to be stored in plain text file.

Running all required services
====================================

First of all, adjust mount paths in the `docker-compose.yml`.
This will allow you to persist all the data generated in the process.
If you want to test it, just comment out `volumes` sections of `docker-compose.yml`.


After you do that you can run in terminal::

    docker-compose pull


It will download all required docker images from the repository.
After download is finished you can run::

    docker-compose up


This will run the whole stack, without detaching it from your terminal.
It's good to test but not to run it long term.
After you see that everything boots up properly, click `CTRL+C` that will stop everything.
It will not remove those containers so you can resume their work without loosing all the content.
This does not mean that you will have access to those files from you own system's filesystem!
In order to remove stopped containers, run::

    docker-compose down

If you want to run everything in the *detached* mode, run::

    docker-compose up -d

It means that it will continue to run even if you close the terminal.

.. _logs:

To see the logs, execute::

    docker-compose logs -f -t

It will print out of the logs and will keep following them, as if you were attached to the process.
If you don't want to print the whole history, you need to add a flag `--tail 50` that will show only last 50 lines for
each of the containers.

Starting interaction with noiz
====================================

Since the stack of all required containers is running in the background, you have to learn how to use the noiz.

By default, there are two ways how to use it.
First one is through the interface of JupyterLab that is by default running on one of the containers.
The second one is through standard shell of the container with noiz.

JupyterLab
--------------

By default from one of the containers, the port `5089` is exposed and JupyerLab server is being run.
In order to connect to the JupyterLab, you have to find in the `logs`_ of the docker-compose
a `token` that will allow you to log in to the JupyterLab instance.
This is a default security measure of the JupyerLab.
It is looking like that: `http://127.0.0.1:8888/?token=7931878cc39f300b0dd2e833f2d3516fa4164d08a55ece56`.
You need to copy everything after the `token=`.

Then, in your browser, navigate to the address::

    localhost:5088/

And paste the token in the proper field.
Voila, you have now access to the JupyterLab instance that has Noiz installed.

Shell
--------------

Open new terminal and execute::

    docker exec  -it noiz-deployment_noiz_1 /bin/bash

That's it.

Using noiz
====================================

First of all, you have to prepare database structure and make sure it contains all the tables that are required.
For that you need to execute::

    noiz db migrate
    noiz db upgrade

This will create the whole structure of the database.

Next, if you want to work with some sample data, head to system test directory of noiz.
It contains a sample dataset with all required data::

    cd /noiz/tests/system-tests/dataset

Now, you can for example add the inventory file to the DB so Noiz will store the information about all components that
you are having in your network::

    noiz data add_inventory station.xml

.. _Docker: https://www.docker.com/products/docker-desktop
.. _PostgreSQL: https://www.postgresql.org/
.. _Python: https://www.python.org/
.. _mseedindex: https://github.com/iris-edu/mseedindex
