import datetime

from pathlib import Path

from typing import Collection, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from noiz.api.beamforming import fetch_beamforming_peaks_avg_abspower_results_in_freq_slowness, fetch_beamforming_params
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.models.beamforming import BeamformingResultType
from obspy import UTCDateTime


def plot_histogram_frequency_slowness(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
        beamforming_params_ids: Optional[Collection[int]] = None,
        minimum_trace_used_count: Optional[int] = None,
        beamforming_result_type: BeamformingResultType = BeamformingResultType.AVGABSPOWER,
        fig_title: Optional[str] = None,
        filepath: Optional[Path] = None,
        showfig: bool = False,
) -> plt.Figure:
    """filldocs"""

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)

    fetched_beam_params = fetch_beamforming_params(ids=beamforming_params_ids)

    df = fetch_beamforming_peaks_avg_abspower_results_in_freq_slowness(
        beamforming_params_collection=fetched_beam_params,
        timespans=fetched_timespans,
        minimum_trace_used_count=minimum_trace_used_count,
        beamforming_result_type=beamforming_result_type,
    )

    column_to_be_binned = "slowness"

    if fig_title is None:
        fig_title = f"Beamforming in frequency-slowness space for \n{starttime} - {endtime}"

    max_slowness = max([param.max_slowness for param in fetched_beam_params])
    max_step = max([param.slowness_step*np.sqrt(2) for param in fetched_beam_params])
    bin_edges = np.arange(0, max_slowness+0.1*max_step, max_step)

    fig = _plot_histogram_of_beamforming_in_freq_slow_vel(
        df=df,
        bin_edges=bin_edges,
        column_to_be_binned=column_to_be_binned,
        fig_title=fig_title,
        filepath=filepath,
        showfig=showfig,
    )

    return fig


def _plot_histogram_of_beamforming_in_freq_slow_vel(
        df: pd.DataFrame,
        bin_edges,  # : npt.ArrayLike
        column_to_be_binned: str,
        fig_title: str,
        filepath: Optional[Path],
        showfig: bool
) -> plt.Figure:
    """filldocs"""

    if column_to_be_binned not in df.columns:
        raise ValueError(f"column_to_be_binned has to be one of {df.columns}")

    central_freqs = df.loc[:, "central_freq"].unique()
    histograms = np.zeros((len(central_freqs), len(bin_edges) - 1))

    for i, central_freq in enumerate(central_freqs):
        counts, _ = np.histogram(
            df.loc[df.loc[:, "central_freq"] == central_freq, column_to_be_binned].to_numpy(),
            bins=bin_edges
        )
        histograms[i, :] = counts

    fig, ax = plt.subplots(dpi=120)
    mappable = ax.pcolormesh(central_freqs, bin_edges[:-1], histograms.T, shading="auto")

    ax.set_title(fig_title)
    ax.set_ylabel("Slowness [s/km]")
    ax.set_xlabel("Frequency")

    fig.colorbar(mappable, label="Counts")

    if filepath is not None:
        fig.savefig(filepath, bbox_inches='tight')

    if showfig is True:
        fig.show()

    return fig
