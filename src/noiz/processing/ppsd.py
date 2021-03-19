import numpy as np
from scipy.fft import fft
from typing import Tuple

from noiz.api.type_aliases import PPSDRunnerInputs
from noiz.exceptions import InconsistentDataException
from noiz.models import Timespan, Datachunk
from noiz.models.ppsd import PPSDResult
from noiz.models.processing_params import PPSDParams


def calculate_ppsd_wrapper(inputs: PPSDRunnerInputs) -> Tuple[PPSDResult, ...]:
    """filldocs"""
    return (calculate_ppsd(
        ppsd_params=inputs["ppsd_params"],
        timespan=inputs["timespan"],
        datachunk=inputs["datachunk"],
    ), )


def calculate_ppsd(
        ppsd_params: PPSDParams,
        timespan: Timespan,
        datachunk: Datachunk,
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

    ffts = np.empty((windows_count, freqs.shape[0]), dtype=np.complex128)

    subwindow_generator = tr.slide(
        window_length=ppsd_params.segment_length,
        step=ppsd_params.segment_step,
        nearest_sample=False,
        include_partial_windows=False
    )

    for i, tr_segment in enumerate(subwindow_generator):
        if tr_segment.stats.npts == ppsd_params.expected_sample_count+1:
            tr_segment.data = tr_segment.data[:-1]
        elif (tr_segment.stats.npts != ppsd_params.expected_sample_count+1) and \
                (tr_segment.stats.npts != ppsd_params.expected_sample_count):
            continue

        ffts[i, :] = abs(fft(tr_segment.data)[ppsd_params._where_accepted_freqs])

    ffts = 2*(1/ppsd_params.sampling_rate)**2*(1/ppsd_params.segment_length)*np.abs(ffts**2)

    energy_list = np.nansum(ffts, axis=1)

    acc_windows_by_energy = np.where(
        (energy_list < np.nanquantile(energy_list, 1 - ppsd_params.rejected_quantile)) &
        (energy_list > np.nanquantile(energy_list, ppsd_params.rejected_quantile)))[0]
    accepted_windows = ffts[acc_windows_by_energy, :]

    fft_mean = np.nanmean(accepted_windows, axis=0)
    fft_median = np.nanmedian(accepted_windows, axis=0)
    fft_std = np.nanstd(accepted_windows, axis=0)
    
    # TODO Add saving results, including extended results

    ret = PPSDResult(
        ppsd_params_id=ppsd_params.id,
        timespan_id=timespan.id,
        datachunk_id=datachunk.id,
    )
    return ret
