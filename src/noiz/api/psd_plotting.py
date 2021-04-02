from collections import defaultdict
import datetime
import matplotlib.pyplot as plt

from noiz.processing.ppsd import _plot_avg_psds, average_psd_by_component
from obspy import UTCDateTime
from pathlib import Path
from typing import Optional, Union, Tuple

from noiz.api.component import fetch_components
from noiz.api.datachunk import fetch_datachunks
from noiz.api.ppsd import fetch_ppsd_params_by_id, fetch_ppsd_results
from noiz.api.timespan import fetch_timespans_between_dates


def plot_average_psd_between_dates(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
        ppsd_params_id: int,
        fig_title: Optional[str] = None,
        show_legend: bool = True,
        filepath: Optional[Path] = None,
        showfig: bool = False,
        xlims: Optional[Tuple[float, float]] = None,
        ylims: Optional[Tuple[float, float]] = None,
) -> plt.Figure:

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_psd_params = fetch_ppsd_params_by_id(id=ppsd_params_id)
    fetched_components = fetch_components(components='Z')

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
