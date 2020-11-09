import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.units as munits
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Collection

from noiz.api.component import fetch_components
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.api.soh import fetch_raw_soh_gps_df, fetch_averaged_soh_gps_df
from noiz.models.component import Component

converter = mdates.ConciseDateConverter()
munits.registry[np.datetime64] = converter
munits.registry[datetime.date] = converter
munits.registry[datetime] = converter


def plot_raw_gps_data_availability(
        networks: Optional[Collection[str]] = None,
        stations: Optional[Collection[str]] = None,
        starttime: datetime = datetime(2000, 1, 1),
        endtime: datetime = datetime(2030, 1, 1),
        filepath: Optional[Path] = None,
        showfig: bool = False,
        show_legend: bool = True,
        minticks: int = 5,
        maxticks: int = 8,
) -> matplotlib.pyplot.Figure:
    """
    Method that allows for selection and plotting of raw GPS SOH data that are stored in the DB for given set of
    requirements.

    :param networks: Networks to be fetched
    :type networks: Optional[Collection[str]]
    :param stations: Stations to be fetched
    :type stations: Optional[Collection[str]]
    :param starttime: Starttime of the query
    :type starttime: datetime
    :param endtime: Endtime of the query
    :type endtime: datetime
    :param filepath: Filepath where plot should be saved
    :type filepath: Optional[Path]
    :param showfig: If the figure should be showed
    :type showfig: bool
    :param show_legend: If legend should be added to the bottom subplot
    :type show_legend: bool
    :param minticks: Value of minticks passed to ConciseDateFormatter
    :type minticks: int
    :param maxticks: Value of maxticks passed to ConciseDateFormatter
    :type maxticks: int
    :return: Figure object with the plot for further manipulation
    :rtype: matplotlib.pyplot.Figure:rtype:
    """

    fetched_components = fetch_components(networks=networks, stations=stations)

    z_components = [cmp for cmp in fetched_components if cmp.component == 'Z']

    df = fetch_raw_soh_gps_df(components=fetched_components, starttime=starttime, endtime=endtime)

    fig_title = "Raw GPS SOH data"

    df.index = df['datetime']

    fig = __plot_gps_data_soh(
        df=df,
        z_components=z_components,
        starttime=starttime,
        endtime=endtime,
        fig_title=fig_title,
        show_legend=show_legend,
        minticks=minticks,
        maxticks=maxticks,
    )

    if filepath is not None:
        fig.savefig(filepath, bbox_inches='tight')

    if showfig is True:
        fig.show()

    return fig


def plot_averaged_gps_data_availability(
        networks: Optional[Collection[str]] = None,
        stations: Optional[Collection[str]] = None,
        starttime: datetime = datetime(2000, 1, 1),
        endtime: datetime = datetime(2030, 1, 1),
        filepath: Optional[Path] = None,
        showfig: bool = False,
        show_legend: bool = True,
        minticks: int = 5,
        maxticks: int = 8,
) -> matplotlib.pyplot.Figure:
    """
    Method that allows for selection and plotting of raw GPS SOH data that are stored in the DB for given set of
    requirements.

    :param networks: Networks to be fetched
    :type networks: Optional[Collection[str]]
    :param stations: Stations to be fetched
    :type stations: Optional[Collection[str]]
    :param starttime: Starttime of the query
    :type starttime: datetime
    :param endtime: Endtime of the query
    :type endtime: datetime
    :param filepath: Filepath where plot should be saved
    :type filepath: Optional[Path]
    :param showfig: If the figure should be showed
    :type showfig: bool
    :param show_legend: If legend should be added to the bottom subplot
    :type show_legend: bool
    :param minticks: Value of minticks passed to ConciseDateFormatter
    :type minticks: int
    :param maxticks: Value of maxticks passed to ConciseDateFormatter
    :type maxticks: int
    :return: Figure object with the plot for further manipulation
    :rtype: matplotlib.pyplot.Figure:rtype:
    """

    fetched_components = fetch_components(networks=networks, stations=stations)
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)

    z_components = [cmp for cmp in fetched_components if cmp.component == 'Z']

    df = fetch_averaged_soh_gps_df(components=fetched_components, timespans=fetched_timespans)

    fig_title = "Averaged GPS SOH data"

    df.index = df['midtime']

    fig = __plot_gps_data_soh(
        df=df,
        z_components=z_components,
        starttime=starttime,
        endtime=endtime,
        fig_title=fig_title,
        show_legend=show_legend,
        minticks=minticks,
        maxticks=maxticks,
    )

    if filepath is not None:
        fig.savefig(filepath, bbox_inches='tight')

    if showfig is True:
        fig.show()

    return fig


def __plot_gps_data_soh(
        df: pd.DataFrame,
        z_components: Collection[Component],
        starttime: datetime,
        endtime: datetime,
        fig_title: str,
        show_legend: bool = True,
        minticks: int = 5,
        maxticks: int = 8,
) -> matplotlib.pyplot.Figure:
    """
     Plots content provided pd.DataFrame on a plot with n subplots where n = len(z_components)
     Each of the subplots shows data that have the same z_component_id as members of z_components collection.

    :param df: Dataframe containing data
    :type df: pd.DataFrame
    :param z_components: Components to be plotted
    :type z_components: Collection[Component]
    :param starttime: Starttime of the query
    :type starttime: datetime
    :param endtime: Endtime of the query
    :type endtime: datetime
    :param fig_title: Title of the whole plot, shown on the very top
    :type fig_title: str
    :param show_legend: If legend should be added to the bottom subplot
    :type show_legend: bool
    :param minticks: Value of minticks passed to ConciseDateFormatter
    :type minticks: int
    :param maxticks: Value of maxticks passed to ConciseDateFormatter
    :type maxticks: int
    :return:
    :rtype:
    """

    fig, axes = plt.subplots(nrows=len(z_components), sharex=True, sharey=True, dpi=150)

    if len(z_components) == 1:
        axes = (axes,)

    locator = mdates.AutoDateLocator(minticks=minticks, maxticks=maxticks)
    formatter = mdates.ConciseDateFormatter(locator)
    axes[0].xaxis.set_major_locator(locator)
    axes[0].xaxis.set_major_formatter(formatter)

    for ax, cmp in zip(axes, z_components):
        subdf = df.loc[df.loc[:, 'z_component_id'] == cmp.id, :].sort_index()
        ax.plot(subdf.index, subdf.loc[:, ['time_uncertainty']], label='Uncertainty [ms]')
        ax.plot(subdf.index, subdf.loc[:, ['time_error']], label='Error [ms]')
        ax.set_ylabel(str(cmp), rotation=0, labelpad=30)
        ax.yaxis.set_label_position("right")

    axes[0].set_title(fig_title)
    axes[0].set_xlim(starttime - timedelta(days=1),
                     endtime + timedelta(days=1))

    days = (starttime - endtime).days

    height = len(z_components) * 1.2
    height = max(4, height)
    fig.set_figheight(height)

    width = max(6, days / 30.)
    width = min(width, height * 4)
    fig.set_figwidth(width)

    if show_legend:
        axes[-1].legend(loc='upper center', ncol=2, bbox_to_anchor=(0.5, -0.15), fancybox=True)

    return fig
