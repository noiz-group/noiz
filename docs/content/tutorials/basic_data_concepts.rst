.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Main concepts of Noiz
*********************

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
