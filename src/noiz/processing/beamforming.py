import numpy as np
from loguru import logger
from typing import Tuple
from obspy.core import AttribDict, Stream
from obspy.signal.array_analysis import array_processing

from noiz.api.type_aliases import BeamformingRunnerInputs
from noiz.models import Timespan, Datachunk, Component
from noiz.models.beamforming import BeamformingResult
from noiz.models.processing_params import BeamformingParams


def calculate_qcone_results_wrapper_wrapper(inputs: BeamformingRunnerInputs) -> Tuple[BeamformingResult, ...]:
    """filldocs"""
    return (
        calculate_beamforming_results(
            beamforming_params=inputs["beamforming_params"],
            timespan=inputs["timespan"],
            datachunks=inputs["datachunks"],
        ),
    )


def calculate_beamforming_results(
        beamforming_params: BeamformingParams,
        timespan: Timespan,
        datachunks: Tuple[Datachunk, ...],

) -> BeamformingResult:
    """filldocs
    """

    logger.debug("Creating an empty BeamformingResult")
    res = BeamformingResult(timespan_id=timespan.id, beamforming_params_id=beamforming_params.id)

    if len(datachunks) <= 3:
        raise ValueError(f"You should use more than 3 datachunks for beamforming. You provided {len(datachunks)}")

    logger.debug("Loading seismic files")
    streams = Stream()
    for datachunk in datachunks:
        if not isinstance(datachunk.component, Component):
            raise ValueError('You should load Component together with the Datachunk.')
        st = datachunk.load_data()
        st[0].stats.coordinates = AttribDict({
            'latitude': datachunk.component.lat,
            'elevation': datachunk.component.elevation,
            'longitude': datachunk.component.lon/1000})
        streams.extend(st)

    logger.debug(f"Calculating beamforming for timespan {timespan}")

    first_starttime = min([tr.stats.starttime for tr in streams])
    first_endtime = min([tr.stats.endtime for tr in streams])

    array_proc_kwargs = dict(
        # slowness grid: X min, X max, Y min, Y max, Slow Step
        sll_x=beamforming_params.slowness_x_min,
        slm_x=beamforming_params.slowness_x_max,
        sll_y=beamforming_params.slowness_y_min,
        slm_y=beamforming_params.slowness_y_max,
        sl_s=beamforming_params.slowness_step,
        # sliding window properties
        win_len=beamforming_params.window_length,
        win_frac=beamforming_params.window_step,
        # frequency properties
        frqlow=beamforming_params.min_freq,
        frqhigh=beamforming_params.max_freq,
        prewhiten=int(beamforming_params.prewhiten),
        # restrict output
        semb_thres=beamforming_params.semblance_threshold,
        vel_thres=beamforming_params.velocity_threshold,
        timestamp='julsec',
        stime=first_starttime,
        etime=first_endtime,
        method=beamforming_params.method,
    )

    out = array_processing(streams, **array_proc_kwargs)
    timestamp, relative_relpow, absolute_relpow, backazimuth, slowness = np.hsplit(out, 5)

    res.mean_slowness = np.mean(slowness)
    res.mean_relative_relpow = np.mean(relative_relpow)
    res.mean_absolute_relpow = np.mean(absolute_relpow)
    res.mean_backazimuth = np.mean(backazimuth)
    res.datachunks = list(datachunks)

    return res
