.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.


=========================
Noiz changelog
=========================

master
=========================

Breaking changes
------------------
- Renamed ComponentPair to ComponentPairCartesian. !191

New functionality
------------------
- Added ComponentPairCylyndrical. !191
- Added event detection mechanism. !183

Bugfix
------------------
- Added restarts of dask clients due to a memory leak during processing large amounts of data. !188
- Fixed generating timespans over midnight. !182

Maintenance
------------------
- Released dependency locks on multiple dependencies. !200
- Migrated build system to hatchling. !194

Documentation
------------------
- Added documentation on how to use scalene for profiling system tests. !197


0.5.20210405 and before
=========================
Noiz was developed as a closed source, without proper tracking of changes.
There were not that many end users to worry about. :)
