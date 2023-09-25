.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Tutorial
********

This article describes how to set up and perform basic actions with Noiz, based on our sample dataset.

Check out the dataset
=====================

Dataset is currently stored on Gitlab, next to our main repository.
It can be downloaded by checking out the repository.
In order to do that, run:
::

    git clone git@gitlab.com:noiz-group/noiz-tutorial-dataset.git

Checkout might take some time, the dataset is few GBs big.
After it finishes, navigate to a directory with the dataset:
::

    cd noiz-tutorial-dataset

This dataset contains:

* seismic data in miniseed format
* Network metadata in stationXML format
* SOH data in form of CSV files (Nanometrics Taurus & Centaur specific)
* Noiz processing config files

All those files will allow you to perform all operations with Noiz.

Setting up local Noiz deployment
=================================

For portability reasons, Noiz was designed to work withing containerized environment.
Due to that, one of major requirement for your system is having Docker.
The rest of dependencies is contained within docker images we provide thus it should be much easier down the stream.
If you don't have docker yet, please `download and install it <https://www.docker.com/>`_ from official source.


Main concepts of Noiz
=====================

There are few major concepts in Noiz, that are going to be used in this text.
We are introducing them at the beginning will help you understand further parts of the tutorial.

Timespan
---------

Timespan is overarching piece of time along which all subsequent operations will be performed.
Timespans are defined globally, for the whole project.
Currently, there is no possibility of setting different sets of timespans within a single Noiz deployment.
All timespans have the same length and by default they are non overlapping.
However, it is possible to generate timespans in overlapping manner.


Datachunk
----------

Datachunk is fundamental slice of data.
It's a piece of raw data that was sliced out of an original file according to a start and end points of the Timespan.
Datachunks originated from different stations that are sliced along the same Timespan,
are connected in our database to that timespan.
Thanks to that, it is extremely easy to select all available data for any timespan or group of timespans.
This allows to significantly optimize cross station calculations such like cross-correlations.

Processed Datachunk
--------------------

Processed datachunk is a datachunk that went through some processing such as tapering, spectral whitening or others.
It can be configured what operations as well as what parameters of each of them are performed.


Random
===========

Here is an example how to link an image in rst

.. image:: _images/example_image.png
