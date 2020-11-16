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


Deploying noiz
====================================




.. _Docker: https://www.docker.com/products/docker-desktop
.. _PostgreSQL: https://www.postgresql.org/
.. _Python: https://www.python.org/
.. _mseedindex: https://github.com/iris-edu/mseedindex
