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
- Fixes for race condition in creation of directory for results. !211

Maintenance
------------------
- Released dependency locks on multiple dependencies. !200
- Migrated build system to hatchling. !194
- Added file .git-blame-ignore-revs and documentation about it. !199
- Moved doc8 config to pyproject.toml. !210
- Added a __main__.py file with entrypoint to the cli. !209
- Removed some random import that was introduced by mistake. !209
- Fixes for building images in out pipeline. !206
- Simplified and fixed docs building in CI. !214

Documentation
------------------
- Added documentation on how to use scalene for profiling system tests. !197

Deprecations
------------
- noiz.models.component.Component.read_inventory function is deprecated. !207

0.5.20210405 and before
=========================
Noiz was developed as a closed source, without proper tracking of changes.
There were not that many end users to worry about. :)
