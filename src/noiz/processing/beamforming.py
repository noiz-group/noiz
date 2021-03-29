from functools import lru_cache

import math

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
from noiz.models.beamforming import BeamformingResult, BeamformingFile, BeamformingPeakAllRelpower, \
    BeamformingPeakAllAbspower, BeamformingPeakAverageRelpower, BeamformingPeakAverageAbspower
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

    if len(datachunks) <= beamforming_params.minimum_trace_count:
        raise NotEnoughDataError(
            f"There are not enough data for beamforming. "
            f"Minimum trace count: {beamforming_params.minimum_trace_count} "
            f"Got: {len(datachunks)}")

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
    time_vector = [pd.Timestamp.utcfromtimestamp(x).to_datetime64() for x in streams[0].times('timestamp')]

    bk = BeamformerKeeper(
        starttime=timespan.starttime_np,
        midtime=timespan.midtime_np,
        endtime=timespan.endtime_np,
        xaxis=beamforming_params.get_xaxis(),
        yaxis=beamforming_params.get_yaxis(),
        time_vector=time_vector,
        save_relpow=beamforming_params.save_relpow,
        save_abspow=beamforming_params.save_abspow,
    )

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
        store=bk.save_beamformers,
    )

    try:
        _ = array_processing(streams, **array_proc_kwargs)
    except ValueError as e:
        raise ObspyError(f"Ecountered error while running beamforming routine. "
                         f"Error happenned for timespan: {timespan}, beamform_params: {beamforming_params} "
                         f"Error was: {e}")

    if beamforming_params.extract_peaks_average_beamformer_abspower:
        res.average_abspower_peaks = bk.get_average_abspower_peaks(
            neighborhood_size=beamforming_params.neighborhood_size,
            maxima_threshold=beamforming_params.maxima_threshold,
            best_point_count=beamforming_params.best_point_count,
            beam_portion_threshold=beamforming_params.beam_portion_threshold,
        )

    res.used_component_count = len(streams)
    res.datachunks = list(datachunks)

    return res


class BeamformerKeeper:
    """filldocs"""

    def __init__(
            self,
            starttime: np.datetime64,
            midtime: np.datetime64,
            endtime: np.datetime64,
            xaxis: npt.ArrayLike,
            yaxis: npt.ArrayLike,
            time_vector: npt.ArrayLike,
            save_relpow: bool = True,
            save_abspow: bool = False
    ):
        self.starttime: np.datetime64 = starttime
        self.midtime: np.datetime64 = midtime
        self.endtime: np.datetime64 = endtime
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

    def save_beamforming_file(self, params: BeamformingParams, ts: Timespan) -> Optional[BeamformingFile]:
        bf = BeamformingFile()
        fpath = bf.find_empty_filepath(ts=ts, params=params)

        res_to_save = dict()

        if params.save_all_beamformers_abspower:
            for i, arr in self.abs_pows:
                res_to_save[f"abs_pow_{i}"] = arr
        if params.save_all_beamformers_relpower:
            for i, arr in self.rel_pows:
                res_to_save[f"rel_pow_{i}"] = arr
        if params.save_average_beamformer_abspower:
            res_to_save["avg_abs_pow"] = self.average_abspow
        if params.save_average_beamformer_relpower:
            res_to_save["avg_abs_pow"] = self.average_relpow

        if len(res_to_save) > 0:
            logger.info(f"File will be saved at {fpath}")
            res_to_save["file"] = fpath
            res_to_save["midtimes"] = self.get_midtimes()
            np.savez_compressed(**res_to_save)
            return bf
        else:
            return None

    @lru_cache
    def get_midtimes(self) -> npt.ArrayLike:
        """filldocs"""
        return np.array([self.time_vector[x] for x in self.midtime_samples])

    def save_beamformers(self, pow_map: npt.ArrayLike, apow_map: npt.ArrayLike, midsample: int) -> None:
        """
        filldocs

        """
        self.iteration_count += 1

        self.midtime_samples.append(midsample)
        if self.save_relpow:
            self.rel_pows.append(pow_map.copy())
        if self.save_abspow:
            self.abs_pows.append(apow_map.copy())

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

        maxima = select_local_maxima(
            data=self.average_relpow,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=self.midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        df = _extract_most_significant_subbeams([maxima, ], beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

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

        maxima = select_local_maxima(
            data=self.average_abspow,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=self.midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        df = _extract_most_significant_subbeams([maxima, ], beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

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

        df = _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

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

        df = _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def get_average_abspower_peaks(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ) -> Tuple[BeamformingPeakAverageAbspower, ...]:
        df = self.extract_best_maxima_from_average_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
        )
        res = []
        for i, row in df.iterrows():
            res.append(
                BeamformingPeakAverageAbspower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return tuple(res)

    def get_average_relpower_peaks(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ) -> Tuple[BeamformingPeakAverageRelpower, ...]:
        df = self.extract_best_maxima_from_average_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
        )
        res = []
        for i, row in df.iterrows():
            res.append(
                BeamformingPeakAverageRelpower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return tuple(res)

    def get_all_abspower_peaks(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ) -> Tuple[BeamformingPeakAllAbspower, ...]:
        df = self.extract_best_maxima_from_all_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
        )
        res = []
        for i, row in df.iterrows():
            res.append(
                BeamformingPeakAllAbspower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return tuple(res)

    def get_all_relpower_peaks(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
    ) -> Tuple[BeamformingPeakAllRelpower, ...]:
        df = self.extract_best_maxima_from_all_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
        )
        res = []
        for i, row in df.iterrows():
            res.append(
                BeamformingPeakAllRelpower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return tuple(res)


def _extract_most_significant_subbeams(
        all_maxima: Collection[pd.DataFrame],
        beam_portion_threshold: float,
) -> pd.DataFrame:
    """filldocs"""

    df_all = pd.concat(all_maxima).set_index('midtime')
    total_beam = df_all.loc[:, 'val'].groupby(level=0).sum()
    df_all.loc[:, 'beam_proportion'] = df_all.apply(lambda row: row.val / total_beam.loc[row.name], axis=1)
    df_res = df_all.loc[df_all.loc[:, 'beam_proportion'] > beam_portion_threshold, :]
    maximum_points = df_res.groupby(by=['x', 'y']).mean()
    maximum_points['occurence_counts'] = df_res.groupby(by=['x', 'y'])['val'].count()
    maximum_points = maximum_points.rename(
        columns={"val": "avg_val", "beam_proportion": "avg_beam_proportion"}
    ).reset_index(level=[0, 1])

    return maximum_points


def select_local_maxima(
        data: npt.ArrayLike,
        xaxis: npt.ArrayLike,
        yaxis: npt.ArrayLike,
        time: Any,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int
) -> pd.DataFrame:

    data = data.T
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

    df = pd.DataFrame(columns=["midtime", "x", "y", "val"], data=np.vstack([[time] * len(x), x, y, max_vals]).T, )
    df = df.sort_values(by='val', ascending=False)

    if len(df) == 0:
        raise ValueError("No peaks were found. Adjust neighbourhood_size and maxima_threshold values.")

    return df.loc[df.index[:best_point_count], :]


def _calculate_azimuth_backazimuth(
        df: pd.DataFrame,
) -> pd.DataFrame:
    """filldocs"""
    df.loc[:, "azimuth"] = df.apply(lambda row: 180 * math.atan2(row.x, row.y) / math.pi, axis=1)
    df.loc[:, "backazimuth"] = df.apply(lambda row: row.azimuth % -360 + 180, axis=1)
    return df


def _calculate_slowness(
        df: pd.DataFrame,
) -> pd.DataFrame:
    """filldocs"""
    df.loc[:, 'slowness'] = df.apply(lambda row: np.sqrt(row.x ** 2 + row.y ** 2), axis=1)
    return df
