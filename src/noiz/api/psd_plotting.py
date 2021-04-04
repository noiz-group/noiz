from collections import defaultdict
import datetime
import matplotlib.pyplot as plt
from loguru import logger

from noiz.processing.ppsd import _plot_avg_psds, average_psd_by_component, plot_spectrogram_for_component_and_psds
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
    """filldocs"""

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_psd_params = fetch_ppsd_params_by_id(id=ppsd_params_id)
    fetched_components = fetch_components(networks=networks, stations=stations, components=component_codes)

    grouped_psds = defaultdict(list)
    for component in fetched_components:
        datachunks = fetch_datachunks(timespans=fetched_timespans, load_component=False, components=(component, ))
        if len(datachunks) == 0:
            logger.info(f"There were no datachunks fetched for {component}. Did you prepare datachunks? Skipping")
            continue
        fetched_psds = fetch_ppsd_results(ppsd_params_id=ppsd_params_id, datachunks=datachunks)
        if len(fetched_psds) == 0:
            logger.info(f"There were no PSDResults fetched for {component}. Did you calculate PSDs? Skipping")
            continue
        grouped_psds[component] = fetched_psds

    avg_psds = average_psd_by_component(fetched_psd_params, grouped_psds)

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
        return_figures: bool = True,
        fig_title: Optional[str] = None,
        log_freq_scale: bool = True,
        vmin: Union[int, float] = -180,
        vmax: Union[int, float] = -120,
        dirpath: Path = Path.cwd(),
        showfig: bool = False,
        xlims: Optional[Tuple[float, float]] = None,
        ylims: Optional[Tuple[float, float]] = None,
) -> Tuple[plt.Figure, ...]:
    """filldocs"""

    logger.debug("Fetching timespans")
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.debug(f"There were {len(fetched_timespans)} timespans fetched")
    logger.debug("Fetching PSDParams")
    fetched_psd_params = fetch_ppsd_params_by_id(id=ppsd_params_id)
    logger.debug(f"Fetched PSDParams {fetched_psd_params}")
    logger.debug("Fetching components")
    fetched_components = fetch_components(networks=networks, stations=stations, components=component_codes)
    logger.debug(f"Fetched {len(fetched_components)} components")

    figs = []
    for component in fetched_components:
        datachunks = fetch_datachunks(timespans=fetched_timespans, load_component=False, components=(component, ))
        if len(datachunks) == 0:
            logger.info(f"There were no datachunks fetched for {component}. Did you prepare datachunks? Skipping")
            continue
        fetched_psds = fetch_ppsd_results(ppsd_params_id=ppsd_params_id, datachunks=datachunks)
        if len(fetched_psds) == 0:
            logger.info(f"There were no PSDResults fetched for {component}. Did you calculate PSDs? Skipping")
            continue

        filepath = dirpath.joinpath(f"spectrogram_{component}_{starttime}_{endtime}.png")

        logger.debug(f"Starting processing and plotting of psds for {component}")
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
        if return_figures:
            figs.append(fig)
        else:
            plt.close(fig)
        logger.debug(f"Plotting of psds for {component} finished")

    return tuple(figs)
