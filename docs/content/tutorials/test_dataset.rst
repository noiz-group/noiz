.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Test dataset
************

This article describes how to get our test dataset that we ship with concepts of noiz.

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
