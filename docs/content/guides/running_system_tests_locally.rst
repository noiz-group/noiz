.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Running system tests locally
**********************************

System testing is an important part of our testing ecosystem.
They test the behaviour of the entire Noiz, going through the standard workflow from start to finish.
For this reason, they require quite a few dependencies in order to run.
One of the notable dependencies is Postgres, a database that sits behind Noiz.
Apart from that, there are a few others that are packaged a bit suboptimally.

Because of these dependencies, we usually do not recommend running noiz or system tests outside of docker.
However, sometimes it is necessary to run system tests on your local system.
This guide will show you how to run system tests locally and what's the easiest way to get all the dependencies.

Dependencies
============

There are two main dependencies that are necessary for Noiz.
`Postgres <https://www.postgresql.org/>`_ and `mseedindex <https://github.com/EarthScope/mseedindex>`_.
We are also using `just command runner <https://github.com/casey/just>`_ for our command recipes.

mseedindex
----------

The easiest way of getting mseedindex is to compile it.
To do that, while bein in main directory of the repository, run:
::

    ❯ cd ..
    ❯ git clone git@github.com:EarthScope/mseedindex.git
    ❯ cd mseedindex
    ❯ make

mseedindex depends on the dev headers of postgres, so there is a high chance that you will need to install those.
On macos you can install it via brew:
::

    ❯ brew install libpq
    ❯ CFLAGS='-I/usr/local/opt/libpq/include' make

On Linux systems, use your package manager to install missing libraries.
On Windows, use Windows Subsystem for Linux.


PostgreSQL
----------
We recommend not installing Postgres locally but still running it within Docker.
In order to do that, while being in a main directory of the repository, run:
::

    ❯ docker compose --file tests/system_tests/docker-compose.yml up

Just
----

To install just, follow `installation instructions on their website <https://github.com/casey/just#installation>`_

Noiz
----

In the end, you need to install Noiz itself.
As Noiz is a Python project, we recommend installing it into some sort of a virtual environment.
After you create your virtual environment and activate it, install noiz via:
::

    ❯ python -m pip install -r requirements.txt
    ❯ python -m pip install .

This will install Noiz and all python dependencies to that virtual environment.

Running system tests
====================

When you have ``postgres`` running, ``mseedindex`` compiled (in ``../mseedindex/mseedindex``), ``just`` and
``noiz`` itself installed, it should be easy from now on.
Run a command:
::

    ❯ just run_system_tests

If you want to see details of what is executed, take a look at the content of the ``justfile``.
Justfiles are in many ways similar to Makefiles, you will manage to get your head around.

Cleaning up
===========

Running those system tests will leave some junk behind, namely all the processed files.
We decided not to clean them up right after tests in order to give us chance to inspect the files afterwards.
If you want to get rid of those files, run:
::

    ❯ just clean_after_tests

Possible setup errors
=====================

There are multiple ways how this setup can break.
However, here are some problems we are anticipating:

MSEEDINDEX_EXECUTABLE not found
-------------------------------

This error will come if you cloned and compiled Mseedindex in different path then ``../mseedindex/mseedindex``.
To fix it, just modify path in ``justfile`` to path in your local filesystem.
