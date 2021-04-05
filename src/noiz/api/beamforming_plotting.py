import datetime
import matplotlib.pyplot as plt
import numpy as np
from obspy import UTCDateTime
import pandas as pd
from pathlib import Path
from typing import Collection, Optional, Union

from noiz.api.beamforming import fetch_beamforming_peaks_avg_abspower_results_in_freq_slowness, fetch_beamforming_params
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.globals import ExtendedEnum
from noiz.models.beamforming import BeamformingResultType


class HistogramSpaceTypes(ExtendedEnum):
    SLOWNESS = "slowness"
    VELOCITY = "velocity"


def plot_histogram_frequency_slowness_velocity(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
        histogram_space_type: Union[HistogramSpaceTypes, str] = HistogramSpaceTypes.SLOWNESS,
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

    if not isinstance(histogram_space_type, HistogramSpaceTypes):
        try:
            histogram_space_type = HistogramSpaceTypes(histogram_space_type)
        except ValueError:
            raise ValueError(f"histogram_space_type has to be one of {list(HistogramSpaceTypes)}. "
                             f"Provided value was: {histogram_space_type}")

    if histogram_space_type == HistogramSpaceTypes.VELOCITY:
        df.loc[:, "velocity"] = 1/df.loc[:, "slowness"]

    if fig_title is None:
        if histogram_space_type == HistogramSpaceTypes.SLOWNESS:
            fig_title = f"Beamforming in frequency-slowness space for \n{starttime} - {endtime}"
        else:
            fig_title = f"Beamforming in frequency-velocity space for \n{starttime} - {endtime}"

    max_slowness = max([param.max_slowness for param in fetched_beam_params])
    max_step = max([param.slowness_step*np.sqrt(2) for param in fetched_beam_params])
    bin_edges = np.arange(0, max_slowness+0.1*max_step, max_step)

    fig = _plot_histogram_of_beamforming_in_freq_slow_vel(
        df=df,
        bin_edges=bin_edges,
        histogram_space_type=histogram_space_type,
        fig_title=fig_title,
        filepath=filepath,
        showfig=showfig,
    )

    return fig


def _plot_histogram_of_beamforming_in_freq_slow_vel(
        df: pd.DataFrame,
        bin_edges,  # : npt.ArrayLike
        histogram_space_type: HistogramSpaceTypes,
        fig_title: str,
        filepath: Optional[Path],
        showfig: bool
) -> plt.Figure:
    """filldocs"""

    if histogram_space_type.value not in df.columns:
        raise ValueError(f"{histogram_space_type.value} has to be one of {df.columns}")

    central_freqs = df.loc[:, "central_freq"].unique()
    central_freqs.sort()
    histograms = np.zeros((len(central_freqs), len(bin_edges) - 1))

    for i, central_freq in enumerate(central_freqs):
        counts, _ = np.histogram(
            df.loc[df.loc[:, "central_freq"] == central_freq, histogram_space_type.value].to_numpy(),
            bins=bin_edges
        )
        histograms[i, :] = counts

    fig, ax = plt.subplots(dpi=120)
    mappable = ax.pcolormesh(central_freqs, bin_edges[:-1], histograms.T, shading="auto")

    ax.set_title(fig_title)
    if histogram_space_type == HistogramSpaceTypes.SLOWNESS:
        ax.set_ylabel("Slowness [s/km]")
    else:
        ax.set_ylabel("Velocity [km/s]")

    ax.set_xlabel("Frequency")

    fig.colorbar(mappable, label="Counts")

    if filepath is not None:
        fig.savefig(filepath, bbox_inches='tight')

    if showfig is True:
        fig.show()

    return fig
