.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Deprecations in Noiz
********************

Deprecations are a natural part of every software and software development cycle.
In Noiz, for now, we do not have a strict development cycle nor wide userbase.
Thus we decided to make our deprecation system quite straightforward.

We will mark every deprecated function with :code:`numpy.deprecate_with_doc` decorator.
It emits a deprecation warning while using the function.

Or deprecations will live usually for maximum of two releases.
This means that every function that is deprecated right now (0.5.x) will be deleted when we will finally release 0.7.0.
Since we don't even have a proper build system for now, this policy is a subject to change.
