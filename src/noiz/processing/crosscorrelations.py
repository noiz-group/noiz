# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from collections import defaultdict
import numpy as np
import numpy.typing as npt
import pandas as pd

from loguru import logger

import obspy
from typing import Tuple, Dict, DefaultDict, Collection, List, FrozenSet

from noiz.exceptions import CorruptedDataException
from noiz.models import CrosscorrelationCartesian, CrosscorrelationCartesianParams
from noiz.models.component_pair import ComponentPairCartesian, ComponentPairCylindrical
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


def validate_component_code_pairs(component_pairs_cartesian: Collection[str]) -> Tuple[str, ...]:
    """
    Checks if provided component_code_pairs are strings with two characters only and removes duplicates.

    :param component_pairs_cartesian: Collection of component_code_pairs strings to check
    :type component_pairs_cartesian: Collection[str]
    :return: Validated and deduplicated tuple of component_code_pairs
    :rtype: Tuple[str]
    """
    for x in component_pairs_cartesian:
        if len(x) != 2:
            raise ValueError(f"Component_pairs accept only 2 character long strings such as `ZZ` or `NE`. "
                             f"String of `{x}` was provided.")

    return tuple(set(component_pairs_cartesian))


def extract_component_ids_from_component_pairs_cartesian(fetched_component_pairs: Collection[ComponentPairCartesian]) -> Tuple[int, ...]:
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


def assembly_ccf_cartesian_dataframe(
        crosscorrelations_cartesian: Collection[CrosscorrelationCartesian],
        crosscorrelation_cartesian_params: CrosscorrelationCartesianParams,
) -> pd.DataFrame:

    time_vector = crosscorrelation_cartesian_params.correlation_time_vector
    midtimes = []
    ccf_data = []
    for ccf in crosscorrelations_cartesian:
        midtimes.append(ccf.timespan.midtime)
        ccf_data.append(ccf.load_data())
    ccfs = np.vstack(ccf_data)
    df = pd.DataFrame(index=midtimes, columns=time_vector, data=ccfs)
    df = df.sort_index()
    return df


def group_xcrorrcartesian_by_timespanid_componentids(
    processed_xcorrcartesian: List[Tuple[Timespan, CrosscorrelationCartesian]],
) -> DefaultDict[Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]]:
    """
    Groups provided :py:class:`~noiz.models.crosscorrelation_cartesian` by timespan_id and then by
    a set of component ids for the correlations.

    :param processed_xcorrcartesian: Collection of crosscorrelation_cartesian where first element of a tuple is timespan_id
    :type processed_xcorrcartesian: List[Tuple[int, CrosscorrelationCartesian]]
    :return: Grouped Crosscorrelation_cartesian
    :rtype: DefaultDict[Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]]
    """

    grouped_xcorrcartesian: DefaultDict[
        Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]
    ] = defaultdict(dict)
    for timespan, xcorrcart in processed_xcorrcartesian:
        grouped_xcorrcartesian[timespan][
            frozenset(
                (xcorrcart.componentpair_cartesian.component_a_id, xcorrcart.componentpair_cartesian.component_b_id)
            )] = xcorrcart
    return grouped_xcorrcartesian


def _fetch_R_T_xcoor(
    gr_xcors_cart: Dict[FrozenSet[int], CrosscorrelationCartesian],
    comp_pairs_cyl: ComponentPairCylindrical,
) -> Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian, CrosscorrelationCartesian, CrosscorrelationCartesian]:
    """
    Fetch the cartesian cross-correlations that will be used for computing the cylindrical cross-correlations variant
     for RR, TT, RT, TR component pairs.

     If cross-correlation for any of the cylindrical component pairs is not available in provided gr_xcors_cart
     dictionary, a KeyError will be raised.

    :param gr_xcors_cart: Grouped cartesian crosscorrelations
    :type gr_xcors_cart: DefaultDict[Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]]
    :param comp_pairs_cyl: A group of cartesian component pair for which to compute cylindrical variant
    :type comp_pairs_cyl: ComponentPairCylindrical
    :return: list of crosscorrelation_cartesian
    :rtype: Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian]
    """

    xcorr_aN_bN = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aN_id, comp_pairs_cyl.component_bN_id))]
    xcorr_aE_bE = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aE_id, comp_pairs_cyl.component_bE_id))]
    xcorr_aN_bE = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aN_id, comp_pairs_cyl.component_bN_id))]
    xcorr_aE_bN = gr_xcors_cart[frozenset((comp_pairs_cyl.component_bE_id, comp_pairs_cyl.component_aN_id))]

    return xcorr_aN_bN, xcorr_aE_bE, xcorr_aN_bE, xcorr_aE_bN


def _computation_cylindrical_correlation_R_T(code, xcorr_aN_bN, xcorr_aE_bE, xcorr_aN_bE, xcorr_aE_bN, back_az):
    """
    Computation of the cylindrical crosscorrelations for either componnentpair RR, TT, RT or TR

    :param code: componentpair (RR, TT, TR, RT)
    :type code: str
    :param xcorr_aN_bN: cartesian componentpair for station A component N and station B component N
    :type xcorr_aN_bN: Crosscorrelation_cartesian
    :param xcorr_aE_bE: cartesian componentpair for station A component E and station B component E
    :type xcorr_aE_bE: Crosscorrelation_cartesian
    :param xcorr_aN_bE: cartesian componentpair for station A component N and station B component E
    :type xcorr_aN_bE: Crosscorrelation_cartesian
    :param xcorr_aE_bN: cartesian componentpair for station A component E and station B component N
    :type xcorr_aE_bN: Crosscorrelation_cartesian
    :param back_az: backazimuth angle measured between station A and B
    :type back_az: float
    :return: cylindrical crosscorrelation
    :rtype: array
    """

    if code == "RR":
        xcorr_cylindrical = np.sin(back_az)**2 * xcorr_aE_bE.ccf + np.cos(back_az)*np.sin(back_az)*(xcorr_aE_bN.ccf + xcorr_aN_bE.ccf) + np.cos(back_az)**2 * xcorr_aN_bN.ccf

    elif code == "TT":
        xcorr_cylindrical = np.cos(back_az)**2 * xcorr_aE_bE.ccf - np.cos(back_az)*np.sin(back_az)*(xcorr_aE_bN.ccf - xcorr_aN_bE.ccf) + np.sin(back_az)**2 * xcorr_aN_bN.ccf

    elif code == "RT":
        xcorr_cylindrical = np.cos(back_az)**2 * xcorr_aN_bE.ccf + np.cos(back_az)*np.sin(back_az)*(xcorr_aE_bE.ccf - xcorr_aN_bN.ccf) - np.sin(back_az)**2 * xcorr_aE_bN.ccf

    elif code == "TR":
        xcorr_cylindrical = np.cos(back_az)**2 * xcorr_aE_bN.ccf + np.cos(back_az)*np.sin(back_az)*(xcorr_aE_bE.ccf - xcorr_aN_bN.ccf) - np.sin(back_az)**2 * xcorr_aN_bE.ccf

    return xcorr_cylindrical


def _fetch_RT_Z_xcoor(
    gr_xcors_cart: Dict[FrozenSet[int], CrosscorrelationCartesian],
    comp_pairs_cyl: ComponentPairCylindrical,
) -> Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian]:
    """
    Fetch the cartesian crosscorrelation used for computing the cylindrical crosscorrelation for RZ, TZ componentpairs

    :param gr_xcors_cart: Grouped cartesian crosscorrelations
    :type gr_xcors_cart: DefaultDict[Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]]
    :param comp_pairs_cyl: A group of cartesian component pair for which to compute cylindrical variant
    :type comp_pairs_cyl: ComponentPairCylindrical
    :return: list of crosscorrelation_cartesian
    :rtype: Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian]
    """

    xcorr_aE_bZ = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aE_id, comp_pairs_cyl.component_bZ_id))]
    xcorr_aN_bZ = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aN_id, comp_pairs_cyl.component_bZ_id))]

    return xcorr_aE_bZ, xcorr_aN_bZ


def _computation_cylindrical_correlation_RT_Z(code, xcorr_aE_bZ, xcorr_aN_bZ, back_az):
    """
    Computation of the cylindrical crosscorrelations for either componnentpair RZ or TZ

    :param code: componentpair (RZ, TZ)
    :type code: str
    :param xcorr_aE_bZ: Crosscorrelation_cartesian componentpair for station A component E and station B component Z
    :type xcorr_aE_bZ: _type_
    :param xcorr_aN_bZ: cartesian componentpair for station A component N and station B component Z
    :type xcorr_aN_bZ: Crosscorrelation_cartesian
    :param back_az: backazimuth angle measured between station A and B
    :type back_az: float
    :return: cylindrical crosscorrelation
    :rtype: array
    """

    if code == "RZ":
        xcorr_cylindrical = np.sin(back_az) * xcorr_aE_bZ.ccf + np.cos(back_az) * xcorr_aN_bZ.ccf

    elif code == "TZ":
        xcorr_cylindrical = np.cos(back_az) * xcorr_aE_bZ.ccf - np.sin(back_az) * xcorr_aN_bZ.ccf

    return xcorr_cylindrical


def _fetch_Z_TR_xcoor(
    gr_xcors_cart: Dict[FrozenSet[int], CrosscorrelationCartesian],
    comp_pairs_cyl: ComponentPairCylindrical,
) -> Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian]:
    """
    Fetch the cartesian crosscorrelation used for computing the cylindrical crosscorrelation for ZR, ZT componentpairs

    :param gr_xcors_cart: Grouped cartesian crosscorrelations
    :type gr_xcors_cart: DefaultDict[Timespan, Dict[FrozenSet[int], CrosscorrelationCartesian]]
    :param comp_pairs_cyl: A group of cartesian component pair for which to compute cylindrical variant
    :type comp_pairs_cyl: ComponentPairCylindrical
    :return: list of crosscorrelation_cartesian
    :rtype: Tuple[CrosscorrelationCartesian, CrosscorrelationCartesian]
    """
    xcorr_aZ_bE = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aZ_id, comp_pairs_cyl.component_bE_id))]
    xcorr_aZ_bN = gr_xcors_cart[frozenset((comp_pairs_cyl.component_aZ_id, comp_pairs_cyl.component_bN_id))]

    return xcorr_aZ_bE, xcorr_aZ_bN


def _computation_cylindrical_correlation_Z_TR(code, xcorr_aZ_bE, xcorr_aZ_bN, back_az):
    """
    Computation of the cylindrical crosscorrelations for either componnentpair ZR or ZT

    :param code: componentpair (ZR, ZT)
    :type code: str
    :param xcorr_aZ_bE: Crosscorrelation_cartesian componentpair for station A component Z and station B component E
    :type xcorr_aZ_bE: Crosscorrelation_cartesian
    :param xcorr_aZ_bN: cartesian componentpair for station A component Z and station B component N
    :type xcorr_aZ_bN:  Crosscorrelation_cartesian
    :param back_az: backazimuth angle measured between station A and B
    :type back_az: float
    :return: cylindrical crosscorrelation
    :rtype: array
    """

    if code == "ZR":
        xcorr_cylindrical = np.sin(back_az) * xcorr_aZ_bE.ccf + np.cos(back_az) * xcorr_aZ_bN.ccf

    elif code == "ZT":
        xcorr_cylindrical = np.cos(back_az) * xcorr_aZ_bE.ccf - np.sin(back_az) * xcorr_aZ_bN.ccf

    return xcorr_cylindrical
