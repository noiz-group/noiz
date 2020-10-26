import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Collection

from noiz.api.component import fetch_components
from noiz.api.soh import fetch_raw_soh_gps_df
from noiz.models import Component


def plot_raw_gps_data_availability(
        network: Optional[str] = None,
        station: Optional[str] = None,
        starttime: datetime = datetime(2000, 1, 1),
        endtime: datetime = datetime(2030, 1, 1),
        filepath: Optional[Path] = None,
        showfig: bool = False
):

    fetched_components = fetch_components(networks=network, stations=station)

    z_components = [cmp for cmp in fetched_components if cmp.component == 'Z']

    df = fetch_raw_soh_gps_df(components=fetched_components, startdate=starttime, enddate=endtime)

    fig_title = "Raw GPS SOH data"

    fig = plot_gps_data_soh(
        df=df,
        components=z_components,
        starttime=starttime,
        endtime=endtime,
        fig_title=fig_title,
    )

    if filepath is not None:
        fig.savefig(filepath)

    if showfig is True:
        fig.show()


def plot_gps_data_soh(
        df: pd.DataFrame,
        components: Collection[Component],
        starttime: datetime,
        endtime: datetime,
        fig_title: str,
):
    df.index = df['datetime']

    fig, axes = plt.subplots(nrows=len(components), sharex=True, sharey=True, dpi=150)

    for ax, cmp in zip(axes, components):
        subdf = df.loc[df.loc[:, 'z_component_id'] == cmp.id, :]
        ax.plot(subdf.index, subdf.loc[:, ['time_uncertainty']], label='Uncertainty')
        ax.plot(subdf.index, subdf.loc[:, ['time_error']], label='Error')
        ax.set_ylabel(str(cmp), rotation=0, labelpad=30)
        ax.yaxis.set_label_position("right")

    # This is used to show only one common label on X and Y axes
    common_ax = fig.add_subplot(111, frameon=False)
    common_ax.tick_params(labelcolor='none', top='off', bottom='off', left='off', right='off')
    common_ax.set_xlabel('Date', labelpad=45)
    common_ax.set_ylabel('Time [ms]', labelpad=30)
    common_ax.set_title(fig_title)

    days = (starttime - endtime).days

    height = len(components) * 1.2
    height = max(4, height)
    fig.set_figheight(height)

    width = max(6, days / 30.)
    width = min(width, height * 4)
    fig.set_figwidth(width)

    axes[-1].set_xlim(starttime - timedelta(days=1),
                      endtime + timedelta(days=1))

    fig.autofmt_xdate()
    return fig
