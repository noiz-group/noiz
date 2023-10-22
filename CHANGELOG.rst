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
- Removed CrosscorrelationOld model and renamed table under Crosscorrelation model to crosscorrelation. !220
- Completely reworked Beamforming computations. !219
- Remove support for python 3.8. !232

Significant changes to processing
----------------------------------
- Omit files with STEIM1 integrity issues from processing. !225
- Change downsampling procedure during creation of Datachunks. Mostly affecting high downsampling factors. !225

New functionality
------------------
- Added ComponentPairCylindrical. !191
- Added event detection mechanism. !183
- Full introduction of Cylindrical CCFs

Bugfix
------------------
- Added restarts of dask clients due to a memory leak during processing large amounts of data. !188
- Fixed generating timespans over midnight. !182
- Fixes for race condition in creation of directory for results. !211
- Adds `with_file=True` parameter to parallel datachunk creation call. !224
- Fix logic for rejection of datachunks in QCOne and CCFs in QCTwo based on preconfigured rejection periods !227

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
- Switch to absolute https address of submodule with data for system tests. !226
- Adds bunch of things to gitignore, adds a dockerignore. !331

Documentation
------------------
- Added documentation on how to use scalene for profiling system tests. !197
- Added documentation group "Development" and policy on deprecations in Noiz. !228

Deprecations
------------
- noiz.models.component.Component.read_inventory function is deprecated. !207
- Unified deprecations of multiple functions. !228

0.5.20210405 and before
=========================
Noiz was developed as a closed source, without proper tracking of changes.
There were not that many end users to worry about. :)
