from collections import defaultdict
from typing import Optional, Collection, List, Dict

from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from pathlib import Path

from noiz.api import fetch_components
from noiz.api.datachunk.datachunk import fetch_datachunks
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.api.timespan import fetch_timespans_between_dates


def plot_datachunk_availability(
        networks: Optional[Collection[str]] = None,
        stations: Optional[Collection[str]] = None,
        components: Optional[Collection[str]] = None,
        processing_params_id: int = 1,
        starttime: datetime = datetime(2000,1,1),
        endtime: datetime = datetime(2030,1,1),
        filepath: Optional[Path] = None,
        showfig: bool = False

):
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime,
                                                      endtime=endtime)
    fetched_components = fetch_components(networks=networks,
                                          stations=stations,
                                          components=components)

    processing_params = fetch_processing_config_by_id(id=processing_params_id)
    datachunks = fetch_datachunks(components=fetched_components,
                                  timespans=fetched_timespans,
                                  processing_params=processing_params,
                                  load_timespan=True, load_component=True)

    midtimes = defaultdict(list)
    for datachunk in datachunks:
        midtimes[str(datachunk.component)].append(datachunk.timespan.midtime)

    fig_title = "Datachunk availability"

    availability = {}
    for key, times in midtimes.items():
        availability[key] = round(
            len(times) / len(fetched_timespans) * 100, 2)

    fig = plot_availability(midtimes, starttime, endtime, fig_title,
                            availability)

    if filepath is not None:
        fig.savefig(filepath)

    if showfig is True:
        fig.show()

    return


def plot_availability(
        midtimes: Dict[str, List[datetime]],
        starttime: datetime,
        endtime: datetime,
        fig_title: str,
        availability: Dict[str, float]
):
    days = (starttime - endtime).days

    fig, ax = plt.subplots(dpi=150)

    keys = list(midtimes.keys())
    keys.sort(reverse=True)

    for key in keys:
        ax.scatter(midtimes[key], [key] * len(midtimes[key]), marker='x',
                   linewidth=0.1, alpha=1)

    ax.set_xlim(starttime - timedelta(days=5),
                endtime + timedelta(days=5))
    fig.autofmt_xdate()

    height = len(midtimes.keys()) * 0.75
    height = max(4, height)
    fig.set_figheight(height)

    width = max(6, days / 30.)
    width = min(width, height * 4)
    fig.set_figwidth(width)

    ax.set_title(fig_title)
    fig.tight_layout()

    if availability is not None:
        labels = [tick.get_text() for tick in ax.get_yticklabels()]
        new_labels = []
        for label in labels:
            station_avail = availability.get(label)

            if station_avail is None:
                new_labels.append(label)
            else:
                new_labels.append(f"{label}\n{station_avail}%")

        ax.yaxis.set_ticklabels(new_labels)

    return fig
