from collections import defaultdict
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from noiz.models import PPSDParams, PPSDResult, Component
from noiz.processing.ppsd import _plot_avg_psds, average_psd_by_component
from obspy import UTCDateTime
from pathlib import Path
from typing import Optional, Union, Tuple, Collection

from noiz.api.component import fetch_components
from noiz.api.datachunk import fetch_datachunks
from noiz.api.ppsd import fetch_ppsd_params_by_id, fetch_ppsd_results
from noiz.api.timespan import fetch_timespans_between_dates


def plot_average_psd_between_dates(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
        ppsd_params_id: int,
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        component_codes: Optional[Union[Collection[str], str]] = None,
        fig_title: Optional[str] = None,
        show_legend: bool = True,
        filepath: Optional[Path] = None,
        showfig: bool = False,
        xlims: Optional[Tuple[float, float]] = None,
        ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_psd_params = fetch_ppsd_params_by_id(id=ppsd_params_id)
    fetched_components = fetch_components(networks=networks, stations=stations, components=component_codes)

    fetched_psds = defaultdict(list)
    for component in fetched_components:
        datachunks = fetch_datachunks(timespans=fetched_timespans, load_component=False, components=(component, ))
        if len(datachunks) == 0:
            continue
        fetched_psds[component] = fetch_ppsd_results(ppsd_params_id=ppsd_params_id, datachunks=datachunks)

    avg_psds = average_psd_by_component(fetched_psd_params, fetched_psds)

    if fig_title is not None:
        fig_title = f"Average PSD for \n{starttime} - {endtime}"

    fig = _plot_avg_psds(
        avg_psds=avg_psds,
        fetched_psd_params=fetched_psd_params,
        fig_title=fig_title,
        filepath=filepath,
        show_legend=show_legend,
        showfig=showfig,
        xlims=xlims,
        ylims=ylims,
    )

    return fig


def plot_spectrograms_between_dates(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
        ppsd_params_id: int,
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        component_codes: Optional[Union[Collection[str], str]] = None,
        rolling_window: Optional[str] = None,
        fig_title: Optional[str] = None,
        log_freq_scale: bool = True,
        vmin: Union[int, float] = -180,
        vmax: Union[int, float] = -120,
        filepath: Optional[Path] = None,
        showfig: bool = False,
        xlims: Optional[Tuple[float, float]] = None,
        ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_psd_params = fetch_ppsd_params_by_id(id=ppsd_params_id)
    fetched_components = fetch_components(networks=networks, stations=stations, components=component_codes)

    for component in fetched_components:
        datachunks = fetch_datachunks(timespans=fetched_timespans, load_component=False, components=(component, ))
        if len(datachunks) == 0:
            continue
        fetched_psds = fetch_ppsd_results(ppsd_params_id=ppsd_params_id, datachunks=datachunks)

        fig = plot_spectrogram_for_component_and_psds(
            component=component,
            fetched_psd_params=fetched_psd_params,
            fetched_psds=fetched_psds,
            rolling_window=rolling_window,
            fig_title=fig_title,
            log_freq_scale=log_freq_scale,
            filepath=filepath,
            showfig=showfig,
            vmin=vmin,
            vmax=vmax,
            xlims=xlims,
            ylims=ylims,
        )

        yield fig


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
    fig, ax = plt.subplots(dpi=150, constrained_layout=True)

    mappable = ax.pcolormesh(df.index, df.columns, df.to_numpy().T, shading="auto", vmin=vmin, vmax=vmax)

    for label in ax.get_xticklabels():
        label.set_ha("right")
        label.set_rotation(45)

    if log_freq_scale:
        ax.set_yscale('log')

    ax.set_ylabel("Frequency [Hz]")

    if fig_title is not None:
        ax.set_title(fig_title)
    if xlims is not None:
        ax.set_xlim(xlims)
    if ylims is not None:
        ax.set_yxlim(ylims)
    if filepath is not None:
        fig.savefig(filepath, bbox_inches='tight')
    if showfig:
        fig.show()

    cb = fig.colorbar(mappable)
    cb.set_label("Amplitude [(m/s)^2/Hz] [dB]")
    return fig


def process_fetched_psds_for_spectrogram(
        fetched_psd_params: PPSDParams,
        fetched_psds: Collection[PPSDResult],
        rolling_window: Optional[str] = None
) -> pd.DataFrame:

    if fetched_psd_params.resample:
        freq_vector = fetched_psd_params.resampled_frequency_vector
    else:
        freq_vector = fetched_psd_params.expected_fft_freq

    psd_array = np.zeros((len(fetched_psds), len(freq_vector)))
    time_vector = np.zeros(len(fetched_psds), dtype="datetime64[ns]")

    for i, psd in enumerate(fetched_psds):
        time_vector[i] = psd.timespan.midtime_np
        fft_mean = np.load(psd.file.filepath)['fft_mean']
        psd_array[i, :] = 10*np.log10(fft_mean)

    df = pd.DataFrame(columns=freq_vector, index=time_vector, data=psd_array).sort_index()
    timespan_frequency = (df.index[1:]-df.index[:-1]).min()
    df = df.asfreq(timespan_frequency)

    if rolling_window is not None:
        df = df.rolling(window=rolling_window).mean()

    return df
