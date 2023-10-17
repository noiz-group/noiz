.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

#####
Noiz
#####

[![pipeline status](https://gitlab.com/noiz-group/noiz/badges/master/pipeline.svg)](https://gitlab.com/noiz-group/noiz/commits/master)
[![coverage report](https://gitlab.com/noiz-group/noiz/badges/master/coverage.svg)](https://gitlab.com/noiz-group/noiz/commits/master)

Ambient Seismic Noise processing application

Documentation can be found at https://noiz-group.gitlab.io/noiz

Acknowledgements
-----------------

Me, Damian Kula, I would like to personally thank two people who had a significant influence on how Noiz was developed.

I would like to thank **Alexandre Kazantsev** for countless hours spent on brainstorming, consultations and validation of the Noiz.
Alex, you are the best.

General tree-like structure of how the processing is performed was inspired by **Maximilien Lehujeur** and software that he wrote during his stay at EOST that was named ``labex``.
Thank you, Max!

Original Funding & Open Sourcing
---------------------------------

Initial version (up to ``0.5``) was developed by Damian Kula during his time at EOST, University of Strasbourg.
The first sketch of the Noiz was created in frame of collaboration between EOST, Storengy SAS and ES Geothermie.
Further developments were done in frame of collaboration between EOST and Storengy SAS.
In 2023 project was released to Open Source under CeCILL-B license.
Developments since git commit tagged with ``0.5`` are thanks to community efforts.

Open-source dependencies
------------------------
Noiz uses Obspy (https://github.com/obspy/obspy and https://github.com/obspy) both as a dependency as well as a source of derived methods.
The latter derived methods are all located in the subpackage src/noiz/processing/obspy_derived
Noiz devoloppers thank all the Obpsy contributors
