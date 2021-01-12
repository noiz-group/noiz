from collections import defaultdict
import numpy as np
import obspy
from typing import Tuple, Iterable, Dict, DefaultDict, Collection

from noiz.models.component_pair import ComponentPair
from noiz.models.datachunk import ProcessedDatachunk


def get_time_vector_ccf(max_lag: float, sampling_rate: float) -> np.array:
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
    processed_datachunks: Iterable[Tuple[ProcessedDatachunk, int, int]]
) -> DefaultDict[int, Dict[int, ProcessedDatachunk]]:
    """
    Groups provided processed datachunks by first timespan id and then by component id
    :param processed_datachunks: Iterable of tuples with processed datachunks, timespan ids and component ids
    :type processed_datachunks: Iterable[Tuple[ProcessedDatachunk, int, int]]
    :return: Dict with Processed datachunks groupped by timespan id and then by component id
    :rtype: DefaultDict[int, Dict[int, ProcessedDatachunk]]
    """

    groupped_chunks: DefaultDict[int, Dict[int, ProcessedDatachunk]] = defaultdict(dict)
    for chunk, component_id, timespan_id in processed_datachunks:
        groupped_chunks[timespan_id][component_id] = chunk
    return groupped_chunks


def group_componentpairs_by_componenta_componentb(
    component_pairs: Iterable[Tuple[ComponentPair, int, int]]
) -> DefaultDict[int, Dict[int, ComponentPair]]:
    """
    Groups provided component pairs by first, ID of a first pair and then by id of a second pair.
    :param component_pairs: Iterable of tuples with ComponentPair and ids of both component
    :type component_pairs: Iterable[Tuple[ComponentPair, int, int]]
    :return: Dict with component pairs grouped by first component id and then second component id
    :rtype: DefaultDict[int, Dict[int, ComponentPair]]
    """
    groupped_componentpairs: DefaultDict[int, Dict[int, ComponentPair]] = defaultdict(
        dict
    )
    for component_pair, component_a_id, component_b_id in component_pairs:
        groupped_componentpairs[component_a_id][component_b_id] = component_pair
    return groupped_componentpairs


def find_correlations_in_chunks(
    chunks: Dict[int, ProcessedDatachunk],
    groupped_componentpairs: DefaultDict[int, Dict[int, ComponentPair]],
) -> DefaultDict[int, Dict[int, ComponentPair]]:

    local_corrs: DefaultDict[int, Dict[int, ComponentPair]] = defaultdict(dict)
    for component_a_id, chunk_a in chunks.items():

        group_component_a = groupped_componentpairs.get(component_a_id)
        if group_component_a is None:
            continue

        for component_b_id, chunk_b in chunks.items():
            found_pair = group_component_a.get(component_b_id)

            if found_pair is None:
                continue
            else:
                if (
                    found_pair.component_a == chunk_a.datachunk.component
                    and found_pair.component_b == chunk_b.datachunk.component
                ):
                    local_corrs[component_a_id][component_b_id] = found_pair
                else:
                    raise ValueError(
                        f"The found correlation has wrong relations with chunks! \
                                     Was expecting on `a`: {found_pair.component_a} got {chunk_a.datachunk.component};\
                                     on `b`: {found_pair.component_b} got {chunk_b.datachunk.component}"
                    )
    return local_corrs


def load_data_for_chunks(
    chunks: Dict[int, ProcessedDatachunk]
) -> Dict[int, obspy.Stream]:
    """
    Takes dict with ProcessedDatachunk objects and returns similar dict but with obspy.Streams
    :param chunks: Dict with ProcessedDataChunks with their ids
    :type chunks: Dict[int, ProcessedDatachunk]
    :return: Dicts with loaded seismic data
    :rtype: Dict[int, obspy.Stream]
    """
    streams = {}
    for cmp_id, proc_chunk in chunks.items():
        streams[cmp_id] = proc_chunk.load_data()
    return streams


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
