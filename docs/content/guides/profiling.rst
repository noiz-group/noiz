.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Profiling with Scalene
*************************

In order to find some performance bottlenecks as well as memory leaks,
I ran `Scalene <https://github.com/plasma-umass/scalene>`_. profiler on system tests of noiz.

"Scalene is a high-performance CPU, GPU and memory profiler for Python that does a number of things that
other Python profilers do not and cannot do."
Scalene promises a lot and what is the most important, it promises to do memory-profiling without too much of
an overhead.


I tried running Scalene for integration tests.
Unfortunately, it seems that it doesn't leak memory at that scale,
or it need a deeper look that what I had time for at that particular moment.

There are few things that you have to have prepared in order to run Scalene on integration tests with success.

Namely:

Add to tests/system_tests/cli/test_data_ingestion.py
::

    if __name__ == '__main__':
        import sys, pytest
        pytest.main(sys.argv)

This will make scalene execute pytest command internally.
In order to run those integration tests through scalene, you

#. :code:`git clone noiz`.
#. :code:`cd noiz && git submodules update`
#. Add the code I quoted before to the file you want to run
#. Make sure you are mounting your local noiz in the docker-compose.yml
#. :code:`docker compose up -d`
#. :code:`docker exec  -it noiz-deployment-noiz-1 /bin/bash`
#. :code:`noiz db migrate && noiz db upgrade && python -m pip install scalene`
#. :code:`scalene --html --memory --cpu --- tests/system_tests/cli/test_data_ingestion.py  --runcli >> report.html`
#. :code:`ctrl+d; docker compose down`
