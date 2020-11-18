Datachunk preprocessing
**********************************

This is just a scratch of description how tha data are treated on this step.

Trace slicing
===============

Trace validation
==================

:param:`noiz.models.processing_params.DatachunkParams.datachunk_sample_threshold` defines minimum percentage of samples
that have to be present in the slice of the Stream in order to proceed with it.
If the minimum condition is not exceeded, the :class:`noiz.models.Datachunk` is not created.

Gap and overlap management
++++++++++++++++++++++++++++++

If gaps or overlaps are present in the sliced data, there is a way of proceeding with them.
Parameter :param:`noiz.models.processing_params.DatachunkParams.max_gap_for_merging` defines how many samples of a gap
or overlap can be present in the signal.
The existence of gaps is checked with :meth:`obspy.Stream.merge` with parameters of `method=0`.
For details of the implementation check in their documentation. Basically, it rejects all overlaps (that are not equal
to each other) and does not interpolate gaps.
If the non of the gaps/overlaps is exceeding the set limit, both gaps and overlaps are merged with method
:meth:`obspy.Stream.merge` with parameters of `method=1, interpolation_samples=-1, fill_value='interpolate'`
