.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Ignore revs file
*****************

Couple of times in history of noiz there were performed changes on the whole codebase.
One of examples of such actions were adding SPDX headers or introducing code formatting.
In such instances, changes done to the files are usually irrelevant from the content point of view.
Thus it can be desirable to ignore those commits from for example :code:`git blame` command.
For this reason, noiz comes with `.git-blame-ignore-revs` file.
In order to use it effectively, you can add it to config of your git repo::

    git config blame.ignoreRevsFile .git-blame-ignore-revs

This will make sure that all revisions listed in that file will be ignored when running blame.
