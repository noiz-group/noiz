# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from collections import defaultdict
import numpy as np
import numpy.typing as npt
import pandas as pd

import obspy
from typing import Tuple, Dict, DefaultDict, Collection, List

from noiz.exceptions import CorruptedDataException
from noiz.models import Crosscorrelation, CrosscorrelationParams
from noiz.models.component_pair import ComponentPairCartesian
from noiz.models.datachunk import ProcessedDatachunk
from noiz.models.timespan import Timespan


def get_time_vector_ccf(max_lag: float, sampling_rate: float) -> npt.ArrayLike:
    """
    Calculate a timeseries symmetric around 0 with given max timelag and sampling rate.
    Length of the timeseries will be 2*max_lag*sampling_rate + 1

    :param max_lag: Maximum timelag in seconds
    :type max_lag: float
    :param sampling_rate: sampling rate in Hz
    :type sampling_rate: float
    :return: Timeseries from -max_lag to +max_lag
    :rtype: np.array
    """
    step = 1 / sampling_rate
    start = -max_lag
    stop = max_lag + step

    return np.arange(start=start, stop=stop, step=step)


def group_chunks_by_timespanid_componentid(
    processed_datachunks: List[Tuple[Timespan, ProcessedDatachunk]]
) -> DefaultDict[Timespan, Dict[int, ProcessedDatachunk]]:
    """
    Groups provided :py:class:`~noiz.models.datachunk.ProcessedDatachunk` by timespan_id and then by component_id.
    It has to come with preloaded :py:attr:`~noiz.models.datachunk.ProcessedDatachunk.datachunk`.

    :param processed_datachunks: Collection fo ProcessedDatachunks
    :type processed_datachunks: Collection[ProcessedDatachunk]
    :return: Grouped processed datachunks
    :rtype: DefaultDict[int, Dict[int, ProcessedDatachunk]]
    """

    groupped_chunks: DefaultDict[Timespan, Dict[int, ProcessedDatachunk]] = defaultdict(dict)
    for timespan, chunk in processed_datachunks:
        groupped_chunks[timespan][chunk.datachunk.component_id] = chunk
    return groupped_chunks


def load_data_for_chunks(
    chunks: Dict[int, ProcessedDatachunk]
) -> Dict[int, obspy.Trace]:
    """
    Takes a dict of ProcessedDatachunks grouped by
    :py:attr:`noiz.models.datachunk.ProcessedDatachunk.datachunk.component_id` and loads data for them.
    Returns dictionary organized by the sam key but instead of ProcessedDatachunk instances with
    :py:class:`obspy.Trace` loaded from disk.

    :param chunks: Dict with ProcessedDataChunks grouped by some key
    :type chunks: Dict[int, ProcessedDatachunk]
    :return: Dict with the same keys but Traces instead
    :rtype: Dict[int, obspy.Trace]
    """
    traces = {}
    for cmp_id, proc_chunk in chunks.items():
        st = proc_chunk.load_data()
        if len(st) != 1:
            msg = f"Mseed file for ProcessedDatachunk {proc_chunk} has different number of traces than 1!" \
                  f"Found number of traces: {len(st)}"
            raise CorruptedDataException(msg)
        traces[cmp_id] = st[0]
    return traces


def validate_component_code_pairs(component_pairs: Collection[str]) -> Tuple[str, ...]:
    """
    Checks if provided component_code_pairs are strings with two characters only and removes duplicates.

    :param component_pairs: Collection of component_code_pairs strings to check
    :type component_pairs: Collection[str]
    :return: Validated and deduplicated tuple of component_code_pairs
    :rtype: Tuple[str]
    """
    for x in component_pairs:
        if len(x) != 2:
            raise ValueError(f"Component_pairs accept only 2 character long strings such as `ZZ` or `NE`. "
                             f"String of `{x}` was provided.")

    return tuple(set(component_pairs))


def extract_component_ids_from_component_pairs(fetched_component_pairs: Collection[ComponentPairCartesian]) -> Tuple[int, ...]:
    """
    Takes a collection of :py:class:`noiz.models.component_pair.ComponentPairCartesian` and extracts ids of all components that
    are included in them. Resulting tuple does not contain repetitions of values.

    :param fetched_component_pairs: componentpairs_cartesian to be processed
    :type fetched_component_pairs: Collection[ComponentPairCartesian]
    :return: Ids of all components that are in input pairs
    :rtype: Tuple[int, ...]
    """

    single_component_ids_pre: List[int] = [pair.component_a_id for pair in fetched_component_pairs]
    single_component_ids_pre.extend([pair.component_b_id for pair in fetched_component_pairs])
    single_component_ids: Tuple[int, ...] = tuple(set(single_component_ids_pre))
    return single_component_ids


def assembly_ccf_dataframe(
        crosscorrelations: Collection[Crosscorrelation],
        crosscorrelation_params: CrosscorrelationParams,
) -> pd.DataFrame:

    time_vector = crosscorrelation_params.correlation_time_vector
    midtimes = []
    ccf_data = []
    for ccf in crosscorrelations:
        midtimes.append(ccf.timespan.midtime)
        ccf_data.append(ccf.load_data())
    ccfs = np.vstack(ccf_data)
    df = pd.DataFrame(index=midtimes, columns=time_vector, data=ccfs)
    df = df.sort_index()
    return df
