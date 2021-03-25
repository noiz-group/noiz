import numpy as np
import pandas as pd
from loguru import logger
from numpy import typing as npt
from scipy import ndimage as ndimage
from scipy.ndimage import filters as filters
from typing import Tuple, Collection, Optional, List, Any

from noiz.exceptions import ObspyError, NotEnoughDataError, SubobjectNotLoadedError
from obspy.core import AttribDict, Stream
from obspy.signal.array_analysis import array_processing

from noiz.models.type_aliases import BeamformingRunnerInputs
from noiz.models import Timespan, Datachunk, Component
from noiz.models.beamforming import BeamformingResult
from noiz.models.processing_params import BeamformingParams


def calculate_beamforming_results_wrapper(inputs: BeamformingRunnerInputs) -> Tuple[BeamformingResult, ...]:
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
        raise NotEnoughDataError(
            f"You should use more than 3 datachunks for beamforming. You provided {len(datachunks)}")

    logger.debug("Loading seismic files")
    streams = Stream()
    for datachunk in datachunks:
        if not isinstance(datachunk.component, Component):
            raise SubobjectNotLoadedError('You should load Component together with the Datachunk.')
        st = datachunk.load_data()
        st[0].stats.coordinates = AttribDict({
            'latitude': datachunk.component.lat,
            'elevation': datachunk.component.elevation / 1000,
            'longitude': datachunk.component.lon})
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
        win_frac=beamforming_params.window_fraction,
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

    try:
        out = array_processing(streams, **array_proc_kwargs)
    except ValueError as e:
        raise ObspyError(f"Ecountered error while running beamforming routine. "
                         f"Error happenned for timespan: {timespan}, beamform_params: {beamforming_params} "
                         f"Error was: {e}")
    timestamp, relative_relpow, absolute_relpow, backazimuth, slowness = np.hsplit(out, 5)

    res.mean_slowness = np.mean(slowness)
    res.std_slowness = np.std(slowness)
    res.mean_relative_relpow = np.mean(relative_relpow)
    res.std_relative_relpow = np.std(relative_relpow)
    res.mean_absolute_relpow = np.mean(absolute_relpow)
    res.std_absolute_relpow = np.std(absolute_relpow)
    res.mean_backazimuth = np.mean(backazimuth)
    res.std_backazimuth = np.std(backazimuth)
    res.used_component_count = len(streams)
    res.datachunks = list(datachunks)

    return res


class BeamformerKeeper:
    """filldocs"""

    def __init__(self, xaxis, yaxis, time_vector, save_relpow=True, save_abspow=False):
        self.xaxis: npt.ArrayLike = xaxis
        self.yaxis: npt.ArrayLike = yaxis
        self.save_relpow: bool = save_relpow
        self.save_abspow: bool = save_abspow

        self.rel_pows: List[npt.ArrayLike] = []
        self.abs_pows: List[npt.ArrayLike] = []
        self.midtime_samples: List[int] = []

        self.iteration_count: int = 0
        self.time_vector: npt.ArrayLike = time_vector

        self.average_relpow: Optional[npt.ArrayLike] = None
        self.average_abspow: Optional[npt.ArrayLike] = None

    def get_midtimes(self):
        """filldocs"""
        return np.array([self.time_vector[x] for x in self.midtime_samples])

    def save_beamformers(self, pow_map: npt.ArrayLike, apow_map: npt.ArrayLike, midsample: int) -> None:
        """
        filldocs

        Important note: It trnsposes the array compared to the obspy one!
        """
        self.iteration_count += 1

        self.midtime_samples.append(midsample)
        if self.save_relpow:
            self.rel_pows.append(pow_map.copy().T)
        if self.save_abspow:
            self.abs_pows.append(apow_map.copy().T)

    def calculate_average_relpower_beamformer(self):
        """filldocs"""
        if self.save_relpow is not True:
            raise ValueError("The `save_relpow` was set to False, data were not kept")
        if len(self.rel_pows) == 0:
            raise ValueError("There are no data to average. "
                             "Are you sure you used `save_beamformers` method to keep data from beamforming procedure")
        self.average_relpow = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.rel_pows:
            self.average_relpow = np.add(self.average_relpow, arr)
        self.average_relpow = self.average_relpow / self.iteration_count

    def calculate_average_abspower_beamformer(self):
        """filldocs"""
        if self.save_abspow is not True:
            raise ValueError("The `save_abspow` was set to False, data were not kept")
        if len(self.abs_pows) == 0:
            raise ValueError("There are no data to average. "
                             "Are you sure you used `save_beamformers` method to keep data from beamforming procedure")
        self.average_abspow = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.abs_pows:
            self.average_abspow = np.add(self.average_abspow, arr)
        self.average_abspow = self.average_abspow / self.iteration_count

    def extract_best_maxima_from_average_relpower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ):
        """filldocs"""
        if self.average_relpow is None:
            self.calculate_average_relpower_beamformer()

        midtime = min(self.time_vector) + (max(self.time_vector) - min(self.time_vector))/2

        maxima = select_local_maxima(
            data=self.average_relpow,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        return _extract_most_significant_subbeams(maxima, beam_portion_threshold)

    def extract_best_maxima_from_average_abspower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ):
        """filldocs"""
        if self.average_abspow is None:
            self.calculate_average_abspower_beamformer()

        midtime = min(self.time_vector) + (max(self.time_vector) - min(self.time_vector))/2

        maxima = select_local_maxima(
            data=self.average_abspow,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        return _extract_most_significant_subbeams(maxima, beam_portion_threshold)

    def extract_best_maxima_from_all_relpower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ):
        """filldocs"""
        all_maxima = []
        for midtime, single_beamformer in zip(self.get_midtimes(), self.rel_pows):
            maxima = select_local_maxima(
                data=single_beamformer,
                xaxis=self.xaxis,
                yaxis=self.yaxis,
                time=midtime,
                neighborhood_size=neighborhood_size,
                maxima_threshold=maxima_threshold,
                best_point_count=best_point_count,
            )
            all_maxima.append(maxima)

        return _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)

    def extract_best_maxima_from_all_abspower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ):
        """filldocs"""
        all_maxima = []
        for midtime, single_beamformer in zip(self.get_midtimes(), self.abs_pows):
            maxima = select_local_maxima(
                data=single_beamformer,
                xaxis=self.xaxis,
                yaxis=self.yaxis,
                time=midtime,
                neighborhood_size=neighborhood_size,
                maxima_threshold=maxima_threshold,
                best_point_count=best_point_count,
            )
            all_maxima.append(maxima)

        return _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)


def _extract_most_significant_subbeams(
        all_maxima: Collection[pd.DataFrame],
        beam_portion_threshold: float,
) -> pd.DataFrame:
    """filldocs"""

    df_all = pd.concat(all_maxima).set_index('midtime')
    total_beam = df_all.loc[:, 'val'].groupby(level=0).sum()
    df_all.loc[:, 'beam_proportion'] = df_all.apply(lambda row: row.val / total_beam.loc[row.name], axis=1)
    df_res = df_all.loc[df_all.loc[:, 'beam_proportion'] > beam_portion_threshold, :]

    return df_res.groupby(by=['x', 'y']).mean().reset_index(level=[0, 1])


def select_local_maxima(
        data: npt.ArrayLike,
        xaxis: npt.ArrayLike,
        yaxis: npt.ArrayLike,
        time: Any,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int
) -> pd.DataFrame:

    data_max = filters.maximum_filter(data, neighborhood_size)
    maxima = (data == data_max)
    data_min = filters.minimum_filter(data, neighborhood_size)
    diff = ((data_max - data_min) > maxima_threshold)
    maxima[diff == 0] = 0

    labeled, num_objects = ndimage.label(maxima)
    slices = ndimage.find_objects(labeled)
    x, y, max_vals = [], [], []
    for dy, dx in slices:
        x_center = int((dx.start + dx.stop - 1) / 2)
        x.append(xaxis[x_center])
        y_center = int((dy.start + dy.stop - 1) / 2)
        y.append(yaxis[y_center])
        max_vals.append(data[y_center, x_center])

    df = pd.DataFrame(columns=["midtime", "x", "y", "val"], data=np.vstack([[time] * len(x), x, y, max_vals]).T)
    df = df.sort_values(by='val', ascending=False)

    return df.loc[df.index[:best_point_count], :]
