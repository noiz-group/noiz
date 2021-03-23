import numpy as np
import numpy.typing as npt
from scipy.fft import fft
from typing import Tuple, List
import pandas as pd
from obspy import UTCDateTime

from noiz.models.type_aliases import PPSDRunnerInputs
from noiz.exceptions import InconsistentDataException
from noiz.models import Timespan, Datachunk, Component
from noiz.models.ppsd import PPSDResult, PPSDFile
from noiz.models.processing_params import PPSDParams


def calculate_ppsd_wrapper(inputs: PPSDRunnerInputs) -> Tuple[PPSDResult, ...]:
    """filldocs"""
    return (calculate_ppsd(
        ppsd_params=inputs["ppsd_params"],
        timespan=inputs["timespan"],
        datachunk=inputs["datachunk"],
        component=inputs["component"],
    ), )


def calculate_ppsd(
        ppsd_params: PPSDParams,
        timespan: Timespan,
        datachunk: Datachunk,
        component: Component,
) -> PPSDResult:
    """filldocs"""

    st = datachunk.load_data()
    if len(st) != 1:
        raise InconsistentDataException(f"Expected that in the stream from Datachunk there will be exactly one trace. "
                                        f"There are {len(st)} traces instead")
    tr = st[0]

    from obspy.core.util.misc import get_window_times

    windows_count = len(
        get_window_times(
            starttime=tr.stats.starttime,
            endtime=tr.stats.endtime,
            window_length=ppsd_params.segment_length,
            step=ppsd_params.segment_step,
            offset=0,
            include_partial_windows=False)
    )

    freqs = ppsd_params.expected_fft_freq

    all_ffts = np.empty((windows_count, freqs.shape[0]), dtype=np.complex128)

    subwindow_generator = tr.slide(
        window_length=ppsd_params.segment_length,
        step=ppsd_params.segment_step,
        nearest_sample=False,
        include_partial_windows=False
    )
    starttimes = []

    for i, tr_segment in enumerate(subwindow_generator):
        if tr_segment.stats.npts == ppsd_params.expected_sample_count+1:
            tr_segment.data = tr_segment.data[:-1]
        elif (tr_segment.stats.npts != ppsd_params.expected_sample_count+1) and \
                (tr_segment.stats.npts != ppsd_params.expected_sample_count):
            continue
        all_ffts[i, :] = abs(fft(tr_segment.data)[ppsd_params._where_accepted_freqs])

        starttimes.append(tr_segment.stats.starttime)

    all_ffts = 2*(1/ppsd_params.sampling_rate)**2*(1/ppsd_params.segment_length)*np.abs(all_ffts**2)

    energy_list = np.nansum(all_ffts, axis=1)

    acc_windows_by_energy = np.where(
        (energy_list < np.nanquantile(energy_list, 1 - ppsd_params.rejected_quantile)) &
        (energy_list > np.nanquantile(energy_list, ppsd_params.rejected_quantile)))[0]
    accepted_windows = all_ffts[acc_windows_by_energy, :]

    psd_file = PPSDFile()
    psd_file.find_empty_filepath(
        cmp=component,
        ts=timespan,
        ppsd_params=ppsd_params,
    )

    _save_psd_results(
        ppsd_params=ppsd_params,
        psd_file=psd_file,
        all_ffts=all_ffts,
        accepted_windows=accepted_windows,
        starttimes=starttimes
    )

    ret = PPSDResult(
        ppsd_params_id=ppsd_params.id,
        timespan_id=timespan.id,
        datachunk_id=datachunk.id,
        file=psd_file,
    )
    return ret


def _save_psd_results(
        ppsd_params: PPSDParams,
        psd_file: PPSDFile,
        all_ffts: npt.ArrayLike,
        accepted_windows: npt.ArrayLike,
        starttimes: List[UTCDateTime]
) -> None:
    """filldocs"""
    results_to_save = dict(
        fft_mean=np.nanmean(accepted_windows, axis=0),
        fft_std=np.nanstd(accepted_windows, axis=0)
    )
    if ppsd_params.save_all_windows:
        step_delta = pd.Timedelta(ppsd_params.segment_step, 'seconds') / 2
        midtimes = [(pd.Timestamp(stt.datetime) + step_delta).to_numpy() for stt in starttimes]

        results_to_save['all_windows'] = all_ffts
        results_to_save['window_midtimes'] = midtimes
    if ppsd_params.save_compressed:
        np.savez_compressed(
            file=psd_file.filepath,
            **results_to_save
        )
    else:
        np.savez(
            file=psd_file.filepath,
            **results_to_save
        )
    return
