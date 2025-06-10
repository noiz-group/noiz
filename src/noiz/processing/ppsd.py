# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from matplotlib import pyplot as plt
import numpy as np
import numpy.typing as npt
from obspy import UTCDateTime
import pandas as pd
from pathlib import Path
from scipy import interpolate
from scipy.fft import fft
from typing import Tuple, List, Union, Optional, Collection

from noiz.models.type_aliases import PPSDRunnerInputs
from noiz.models.ppsd import GroupedPSDs, GroupedAvgPSDs
from noiz.exceptions import InconsistentDataException
from noiz.models import Timespan, Datachunk, Component, PPSDParams, PPSDResult, PPSDFile


def calculate_ppsd_wrapper(inputs: PPSDRunnerInputs) -> Tuple[PPSDResult, ...]:
    """filldocs"""
    return (
        calculate_ppsd(
            ppsd_params=inputs["ppsd_params"],
            timespan=inputs["timespan"],
            datachunk=inputs["datachunk"],
            component=inputs["component"],
        ),
    )


def calculate_ppsd(
    ppsd_params: PPSDParams,
    timespan: Timespan,
    datachunk: Datachunk,
    component: Component,
) -> PPSDResult:
    """filldocs"""

    st = datachunk.load_data()
    if len(st) != 1:
        raise InconsistentDataException(
            f"Expected that in the stream from Datachunk there will be exactly one trace. "
            f"There are {len(st)} traces instead"
        )
    tr = st[0]

    from obspy.core.util.misc import get_window_times

    windows_count = len(
        get_window_times(
            starttime=tr.stats.starttime,
            endtime=tr.stats.endtime,
            window_length=ppsd_params.segment_length,
            step=ppsd_params.segment_step,
            offset=0,
            include_partial_windows=False,
        )
    )

    if ppsd_params.resample:
        freqs = ppsd_params.resampled_frequency_vector
    else:
        freqs = ppsd_params.expected_fft_freq

    all_ffts = np.empty((windows_count, freqs.shape[0]), dtype=np.complex128)

    subwindow_generator = tr.slide(
        window_length=ppsd_params.segment_length,
        step=ppsd_params.segment_step,
        nearest_sample=False,
        include_partial_windows=False,
    )
    starttimes = []

    for i, tr_segment in enumerate(subwindow_generator):
        if tr_segment.stats.npts == ppsd_params.expected_signal_sample_count + 1:
            tr_segment.data = tr_segment.data[:-1]
        elif (tr_segment.stats.npts != ppsd_params.expected_signal_sample_count + 1) and (
            tr_segment.stats.npts != ppsd_params.expected_signal_sample_count
        ):
            continue

        if ppsd_params.taper_type is not None:
            tr_segment.taper(type=ppsd_params.taper_type, max_percentage=ppsd_params.taper_max_percentage)

        fft_vec = abs(fft(tr_segment.data)[ppsd_params._where_accepted_freqs])

        if ppsd_params.resample:
            interpolator = interpolate.interp1d(ppsd_params.expected_fft_freq, fft_vec)
            fft_vec = interpolator(freqs)

        all_ffts[i, :] = fft_vec

        starttimes.append(tr_segment.stats.starttime)

    all_ffts = 2 * (1 / ppsd_params.sampling_rate) ** 2 * (1 / ppsd_params.segment_length) * np.abs(all_ffts**2)

    energy_list = np.nansum(all_ffts, axis=1)

    acc_windows_by_energy = np.where(
        (energy_list < np.nanquantile(energy_list, 1 - ppsd_params.rejected_windows_quantile))
        & (energy_list > np.nanquantile(energy_list, ppsd_params.rejected_windows_quantile))
    )[0]
    accepted_windows = all_ffts[acc_windows_by_energy, :]

    psd_file = PPSDFile()
    psd_file.find_empty_filepath(
        cmp=component,
        ts=timespan,
        params=ppsd_params,
    )

    _save_psd_results(
        ppsd_params=ppsd_params,
        psd_file=psd_file,
        all_ffts=all_ffts,
        accepted_windows=accepted_windows,
        starttimes=starttimes,
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
    starttimes: List[UTCDateTime],
) -> None:
    """filldocs"""
    results_to_save = {
        "fft_mean": np.nanmean(np.array(accepted_windows, dtype=np.float64), axis=0),
        "fft_std": np.nanstd(np.array(accepted_windows, dtype=np.float64), axis=0),
    }
    if ppsd_params.save_all_windows:
        step_delta = pd.Timedelta(ppsd_params.segment_step, "seconds") / 2
        midtimes = [(pd.Timestamp(stt.datetime) + step_delta).to_numpy() for stt in starttimes]

        results_to_save["all_windows"] = all_ffts
        results_to_save["window_midtimes"] = midtimes
    if ppsd_params.save_compressed:
        np.savez_compressed(file=psd_file.filepath, **results_to_save)
    else:
        np.savez(file=psd_file.filepath, **results_to_save)
    return


def _plot_avg_psds(
    avg_psds: GroupedAvgPSDs,
    fetched_psd_params: PPSDParams,
    fig_title: Optional[str] = None,
    filepath: Optional[Path] = None,
    show_legend: bool = True,
    showfig: bool = False,
    xlims: Optional[Tuple[float, float]] = None,
    ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:
    """
    Plots PSDs passed in a dictionary where key is :py:class:`~noiz.models.Component` and value is an array to plot.
    It should be used to plot average PSDs for multiple :py:class:`~noiz.models.Component` instances.

    :param avg_psds: Grouped arrays to plot
    :type avg_psds: GroupedAvgPSDs
    :param fetched_psd_params: PPSDParams that the psds were calculated for
    :type fetched_psd_params: PPSDParams
    :param fig_title: Title of the figure
    :type fig_title: Optional[str]
    :param filepath: Where to save the plot
    :type filepath: Optional[Path]
    :param show_legend: If legend should be visible
    :type show_legend: bool
    :param showfig: If figure should be explicitly showed
    :type showfig: bool
    :param xlims: Maximum extent of Xaxis
    :type xlims: Optional[Tuple[float, float]]
    :param ylims: Maximum extent of Yaxis
    :type ylims: Optional[Tuple[float, float]]
    :return: Plotted Figure
    :rtype: plt.Figure
    """
    fig, ax = plt.subplots(dpi=180)

    if fetched_psd_params.resample:
        freqs = fetched_psd_params.resampled_frequency_vector
    else:
        freqs = fetched_psd_params.expected_fft_freq

    for component, avg_fft in avg_psds.items():
        ax.semilogx(freqs, 10 * np.log10(avg_fft), label=str(component))

    ax.set_ylabel("Amplitude [(m/s)^2/Hz] [dB]")
    ax.set_xlabel("Frequency [Hz]")

    if fig_title is not None:
        ax.set_title(fig_title)
    if show_legend:
        ax.legend()
    if xlims is not None:
        ax.set_xlim(xlims)
    if ylims is not None:
        ax.set_yxlim(ylims)
    if filepath is not None:
        fig.savefig(filepath, bbox_inches="tight")
    if showfig:
        fig.show()
    return fig


def average_psd_by_component(psd_params: PPSDParams, grouped_psds: GroupedPSDs) -> GroupedAvgPSDs:
    """
    Takes :py:class:`~noiz.models.ppsd.PPSDResult` grouped in a dictionary where the keys are
    :py:class:`~noiz.models.component.Component` and values are lists of :py:class:`~noiz.models.ppsd.PPSDResult`.
    It calculates a mean value of all of those PSDs and returns a dictionary where keys are
    :py:class:`~noiz.models.component.Component` and values are :py:class:`numpy.ndarray` containing the average psd.

    :param psd_params: PPSDParams for which the PSDs were calculated for
    :type psd_params: PPSDParams
    :param grouped_psds: PSDResult instances grouped by Component
    :type grouped_psds: GroupedPSDs
    :return: Average PSDs grouped by Component
    :rtype: GroupedAvgPSDs
    """

    if psd_params.resample:
        freq_vector = psd_params.resampled_frequency_vector
    else:
        freq_vector = psd_params.expected_fft_freq

    avg_psds = {}
    for component, fetched_psds_cmp in grouped_psds.items():
        average_fft = np.zeros(len(freq_vector))
        _i = 0
        for _i, psd in enumerate(fetched_psds_cmp):
            loaded_file = psd.load_data()
            fft_mean = loaded_file["fft_mean"]
            average_fft += fft_mean

        average_fft /= _i + 1
        avg_psds[component] = average_fft

    return avg_psds


def plot_spectrogram_for_component_and_psds(
    component: Component,
    fetched_psd_params: PPSDParams,
    fetched_psds: Collection[PPSDResult],
    rolling_window: Optional[str] = None,
    fig_title: Optional[str] = None,
    log_freq_scale: bool = True,
    filepath: Optional[Path] = None,
    showfig: bool = False,
    vmin: Union[int, float] = -180,
    vmax: Union[int, float] = -120,
    xlims: Optional[Tuple[float, float]] = None,
    ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:
    """filldocs"""

    if fig_title is not None:
        fig_title = f"Spectrogram of {component}"
        if rolling_window is not None:
            fig_title = f"Spectrogram of {component} with rolling average of {rolling_window}"

    df = process_fetched_psds_for_spectrogram(
        fetched_psd_params=fetched_psd_params,
        fetched_psds=fetched_psds,
        rolling_window=rolling_window,
    )

    fig = _plot_spectrogram(
        df=df,
        fig_title=fig_title,
        log_freq_scale=log_freq_scale,
        filepath=filepath,
        showfig=showfig,
        vmin=vmin,
        vmax=vmax,
        xlims=xlims,
        ylims=ylims,
    )
    return fig


def _plot_spectrogram(
    df: pd.DataFrame,
    fig_title: Optional[str] = None,
    log_freq_scale: bool = True,
    filepath: Optional[Path] = None,
    showfig: bool = False,
    vmin: Union[int, float] = -180,
    vmax: Union[int, float] = -120,
    xlims: Optional[Tuple[float, float]] = None,
    ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:
    """filldocs"""
    fig, ax = plt.subplots(dpi=150, constrained_layout=True)

    mappable = ax.pcolormesh(df.index, df.columns, df.to_numpy().T, shading="auto", vmin=vmin, vmax=vmax)

    for label in ax.get_xticklabels():
        label.set_ha("right")
        label.set_rotation(45)

    if log_freq_scale:
        ax.set_yscale("log")

    ax.set_ylabel("Frequency [Hz]")

    if fig_title is not None:
        ax.set_title(fig_title)
    if xlims is not None:
        ax.set_xlim(xlims)
    if ylims is not None:
        ax.set_yxlim(ylims)
    if filepath is not None:
        fig.savefig(filepath, bbox_inches="tight")
    if showfig:
        fig.show()

    cb = fig.colorbar(mappable)
    cb.set_label("Amplitude [(m/s)^2/Hz] [dB]")
    return fig


def process_fetched_psds_for_spectrogram(
    fetched_psd_params: PPSDParams, fetched_psds: Collection[PPSDResult], rolling_window: Optional[str] = None
) -> pd.DataFrame:
    """filldocs"""

    if fetched_psd_params.resample:
        freq_vector = fetched_psd_params.resampled_frequency_vector
    else:
        freq_vector = fetched_psd_params.expected_fft_freq

    psd_array = np.zeros((len(fetched_psds), len(freq_vector)))
    time_vector = np.zeros(len(fetched_psds), dtype="datetime64[ns]")

    for i, psd in enumerate(fetched_psds):
        time_vector[i] = psd.timespan.midtime_np
        fft_mean = np.load(psd.file.filepath)["fft_mean"]
        psd_array[i, :] = 10 * np.log10(fft_mean)

    df = pd.DataFrame(columns=freq_vector, index=time_vector, data=psd_array).sort_index()
    timespan_frequency = (df.index[1:] - df.index[:-1]).dropna().min()
    df = df.asfreq(timespan_frequency)

    if rolling_window is not None:
        df = df.rolling(window=rolling_window).mean()

    return df
