from matplotlib import pyplot as plt
import numpy as np
import pickle
import collections
from obspy import UTCDateTime, Trace, Stream
from obspy.signal.trigger import trigger_onset, coincidence_trigger, classic_sta_lta
import pandas as pd
import re
from pathlib import Path
from typing import Dict, Tuple, List, Union, Optional, Collection
from loguru import logger
from datetime import datetime, timedelta

from noiz.models.type_aliases import EventDetectionRunnerInputs, EventConfirmationRunnerInputs
from noiz.exceptions import InconsistentDataException
from noiz.models import Timespan, Datachunk, Component, EventDetectionParams, EventDetectionResult, EventDetectionFile, \
    EventConfirmationParams, EventConfirmationResult, EventConfirmationFile, EventConfirmationRun
from noiz.validation_helpers import validate_timestamp_as_pydatetime


def calculate_event_detection_wrapper(inputs: EventDetectionRunnerInputs) -> Tuple[EventDetectionResult, ...]:
    """
    A simple wrapper converting an EventDetectionRunnerInputs into adequate inputs for
    calculate_event_detection(). Also convert the return object from a List[EventDetectionResult]
    to a tuple().

    :param inputs: a generated EventDetectionRunnerInputs
    :type inputs: EventDetectionRunnerInputs
    :return:a tuple with several EventDetectionResult
    :rtype: Tuple[EventDetectionResult, ...]
    """

    return tuple(
        calculate_event_detection(
            event_detection_params=inputs["event_detection_params"],
            timespan=inputs["timespan"],
            datachunk=inputs["datachunk"],
            component=inputs["component"],
            event_detection_run_id=inputs["event_detection_run_id"],
            plot_figures=inputs["plot_figures"],
        ),
    )


def calculate_event_detection(
        event_detection_params: EventDetectionParams,
        timespan: Timespan,
        datachunk: Datachunk,
        component: Component,
        event_detection_run_id: int,
        plot_figures: bool = True,
) -> List[EventDetectionResult]:
    """
    Uses a sta/lta or an amplitude spike algorithm to detect seismic events
    that occurred during a timespan on a datachunk. For each event detected
    an EventDetectionResult will be created.

    :param event_detection_params: an event_detection_params
    :type event_detection_params: EventDetectionParams
    :param timespan: a timespan
    :type timespan: Timespan
    :param datachunk: a datachunk
    :type datachunk: Datachunk
    :param component: a component
    :type component: Component
    :param event_detection_run_id: the id of the current event_detection_run_id
    :type event_detection_run_id: int
    :param plot_figures: If figures should be saved alonside the miniseed trace.
    :type plot_figures: bool
    :return:a list with several EventDetectionResult
    :rtype: List[EventDetectionResult]
    """

    st = datachunk.load_data()
    if len(st) != 1:
        raise InconsistentDataException(f"Expected that in the stream from Datachunk there will be exactly one trace. "
                                        f"There are {len(st)} traces instead")

    if event_detection_params.trace_trimming_sec is not None:
        st[0].trim(
            starttime=st[0].stats.starttime + timedelta(seconds=event_detection_params.trace_trimming_sec),
            endtime=st[0].stats.endtime - timedelta(seconds=event_detection_params.trace_trimming_sec),
            nearest_sample=False
            )

    st.filter(
        type="bandpass",
        freqmin=event_detection_params.minimum_frequency,
        freqmax=event_detection_params.maximum_frequency,
        )

    if event_detection_params.detection_type == "sta_lta":
        res = sta_lta_detection(
            st=st,
            event_detection_params=event_detection_params,
            timespan=timespan,
            datachunk=datachunk,
            component=component,
            event_detection_run_id=event_detection_run_id,
            plot_figures=plot_figures,
        )

    elif event_detection_params.detection_type == "amplitude_spike":
        res = amplitude_spike_detection(
            st=st,
            event_detection_params=event_detection_params,
            timespan=timespan,
            datachunk=datachunk,
            component=component,
            event_detection_run_id=event_detection_run_id,
            plot_figures=plot_figures,
        )
    else:
        raise ValueError('The detection_type of event_detection_params is invalid.')

    return res


def sta_lta_detection(
        event_detection_params: EventDetectionParams,
        st: Stream,
        timespan: Timespan,
        datachunk: Datachunk,
        component: Component,
        event_detection_run_id: int,
        plot_figures: bool = True,
) -> List[EventDetectionResult]:
    """
    Detects seismic events by using an Sta Lta algorithm from the obspy function coincidence_trigger.
    For each event detected, an EventDetectionResult will be outputed and inserted in the db.

    :param event_detection_params: the EventDetectionParams for the StaLta detection
    :type event_detection_params: EventDetectionParams
    :param st: the data stream of a datachunks
    :type st: obspy.Stream
    :param timespan: a timespan to check for seismic event.
    :type timespan: Timespan
    :param datachunk: the corresponding datachunk.
    :type datachunk: Datachunk
    :param component: a component
    :type component: Component
    :param event_detection_run_id: the id of the event detection run.
    :type event_detection_run_id: int
    :param plot_figures: If figures should be saved alonside the miniseed trace.
    :type plot_figures: bool
    :return: a List storing every EventDetectionResult found.
    :rtype: List[EventDetectionResult]

    """

    length_sta_sec = event_detection_params.n_short_time_average / event_detection_params.minimum_frequency
    length_lta_sec = event_detection_params.n_long_time_average / event_detection_params.minimum_frequency

    multi_trigger = coincidence_trigger(
        trigger_type="classicstalta",
        thr_on=event_detection_params.trigger_value,
        thr_off=event_detection_params.detrigger_value,
        stream=st,
        thr_coincidence_sum=1,
        trigger_off_extension=0,
        similarity_threshold=0,
        sta=length_sta_sec,
        lta=length_lta_sec,
        )

    cft = classic_sta_lta(
        st[0].data,
        length_sta_sec * st[0].stats.sampling_rate,
        length_lta_sec * st[0].stats.sampling_rate
        )
    cft = np.vstack([st[0].times(type="utcdatetime"), cft])

    logger.info(f"Number of events detected by run_event_detection : {len(multi_trigger)} in timespan [{timespan.starttime} ; {timespan.endtime}].")

    res = []

    for trigger in multi_trigger:

        st_copy = st.copy()
        t_event = trigger['time']
        t_duration = trigger['duration']
        start_event = max(
            t_event - event_detection_params.output_margin_length_sec,
            st_copy[0].stats.starttime
            )
        end_event = min(
            t_event + t_duration + event_detection_params.output_margin_length_sec,
            st_copy[0].stats.endtime
            )

        tr = st_copy[0].trim(
            starttime=start_event,
            endtime=end_event,
            nearest_sample=False
            )

        tmp = (cft[:, np.argwhere(cft[0, :] >= start_event)])
        tmp = tmp[:, :, 0]
        tmp = tmp[:, :len(tr.data)]
        characteristic_function = dict(times=tmp[0], cft=tmp[1])

        event_detection_file = EventDetectionFile()
        event_detection_file.find_empty_filepath(
            cmp=component,
            ts=timespan,
            params=event_detection_params,
            time_start=str(t_event.strftime('%Hh%Mm%Ss')),
        )
        logger.debug(f"FILEPATH {event_detection_file.filepath} .")

        tr.write(event_detection_file.filepath, format="MSEED")
        if plot_figures:
            plot_trace(tr, event_detection_file.pngpath)
            plot_trigger(
                trace=tr,
                cft=characteristic_function['cft'],
                thr_on=event_detection_params.trigger_value,
                thr_off=event_detection_params.detrigger_value,
                outfile=str(event_detection_file.trgpath)
            )
            np.savez(
                file=event_detection_file.npzpath,
                **characteristic_function
            )

        res.append(
            EventDetectionResult(
                event_detection_params_id=event_detection_params.id,
                timespan_id=timespan.id,
                datachunk_id=datachunk.id,
                detection_type=event_detection_params.detection_type,
                time_start=validate_timestamp_as_pydatetime(t_event),
                time_stop=validate_timestamp_as_pydatetime(t_event+t_duration),
                minimum_frequency=event_detection_params.minimum_frequency,
                maximum_frequency=event_detection_params.maximum_frequency,
                peak_ground_velocity=np.max(np.abs(tr.data)),
                file=event_detection_file,
                event_detection_run_id=event_detection_run_id,
            )
        )
    return res


def amplitude_spike_detection(
        event_detection_params: EventDetectionParams,
        st: Stream,
        timespan: Timespan,
        datachunk: Datachunk,
        component: Component,
        event_detection_run_id: int,
        plot_figures: bool = True,
) -> List[EventDetectionResult]:
    """
    Detects seismic events when an amplitude spike exceeds a given threshold. For each event
    detected, an EventDetectionResult will be outputed and inserted in the db.

    :param event_detection_params: the EventDetectionParams for the amplitude spike detection
    :type event_detection_params: EventDetectionParams
    :param st: the data stream of a datachunks
    :type st: obspy.Stream
    :param timespan: a timespan to check for seismic event.
    :type timespan: Timespan
    :param datachunk: the corresponding datachunk.
    :type datachunk: Datachunk
    :param component: a component
    :type component: Component
    :param event_detection_run_id: the id of the event detection run.
    :type event_detection_run_id: int
    :param plot_figures: If figures should be saved alonside the miniseed trace.
    :type plot_figures: bool
    :return: a List storing every EventDetectionResult found.
    :rtype: List[EventDetectionResult]
    """

    pgv_char_fun = get_pgv_characteristic_function(
        trace=st[0],
        peak_ground_velocity_threshold=event_detection_params.peak_ground_velocity_threshold,
        time_tolerance=event_detection_params.output_margin_length_sec,
    )

    pgv_char_fun = np.vstack([st[0].times(type="utcdatetime"), pgv_char_fun])

    multi_trigger = get_trigger_from_pgv(pgv_char_fun)

    logger.info(f"Number of events detected by run_event_detection : {len(multi_trigger)} in timespan [{timespan.starttime} ; {timespan.endtime}].")

    res = []

    for trigger in multi_trigger:

        st_copy = st.copy()
        start_event = trigger['start']
        end_event = trigger['stop']

        tr = st_copy[0].trim(
            starttime=start_event,
            endtime=end_event,
            nearest_sample=False,
            )

        event_detection_file = EventDetectionFile()
        event_detection_file.find_empty_filepath(
            cmp=component,
            ts=timespan,
            params=event_detection_params,
            time_start=str(start_event.strftime('%Hh%Mm%Ss')),
        )
        logger.debug(f"FILEPATH {event_detection_file.filepath} .")

        tr.write(event_detection_file.filepath, format="MSEED")
        if plot_figures:
            plot_trace(tr, event_detection_file.pngpath)

        res.append(
            EventDetectionResult(
                event_detection_params_id=event_detection_params.id,
                timespan_id=timespan.id,
                datachunk_id=datachunk.id,
                detection_type=event_detection_params.detection_type,
                time_start=validate_timestamp_as_pydatetime(start_event),
                time_stop=validate_timestamp_as_pydatetime(end_event),
                minimum_frequency=event_detection_params.minimum_frequency,
                maximum_frequency=event_detection_params.maximum_frequency,
                peak_ground_velocity=np.max(np.abs(tr.data)),
                file=event_detection_file,
                event_detection_run_id=event_detection_run_id,
            )
        )

    return res


def calculate_event_confirmation_wrapper(inputs: EventConfirmationRunnerInputs) -> Tuple[EventConfirmationResult, ...]:
    """
    A simple wrapper converting an EventConfirmationRunnerInputs into adequate inputs for
    calculate_event_confirmation(). Also convert the return object from a List[EventDetectionResult]
    to a tuple().

    :param inputs: a generated EventConfirmationRunnerInputs
    :type inputs: EventConfirmationRunnerInputs
    :return:a tuple with several EventConfirmationResult
    :rtype: Tuple[EventConfirmationResult, ...]
    """

    return tuple(
        calculate_event_confirmation(
            event_confirmation_params=inputs["event_confirmation_params"],
            timespan=inputs["timespan"],
            event_detection_results=inputs["event_detection_results"],
            event_confirmation_run=inputs["event_confirmation_run"],
        ),
    )


def calculate_event_confirmation(
        event_confirmation_params: EventConfirmationParams,
        event_detection_results: Collection[EventDetectionResult],
        timespan: Timespan,
        event_confirmation_run: EventConfirmationRun,
) -> List[EventConfirmationResult]:
    """
    Uses a voting algorithm to confirm seismic event previously detected by
    multiple stations. For each event confirmed an EventConfirmationResult
    will be created.

    :param event_confirmation_params: an event_detection_params
    :type event_confirmation_params: EventDetectionParams
    :param event_detection_results: a collection of every EventDetectionResult selected?
    :type event_detection_results: Collection[EventDetectionResult]
    :param timespan: the timespan during which events to be confirmed have occurred
    :type timespan: Timespan
    :param event_confirmation_run: an EventConfirmationRun storing the details of the current cmd call.
    :type event_confirmation_run: EventConfirmationRun
    :return:a list with several EventConfirmationResult
    :rtype: List[EventConfirmationResult]
    """

    confirmed_events = event_confirmation(
        timespan=timespan,
        event_detection_results=event_detection_results,
        time_lag=event_confirmation_params.time_lag,
        vote_threshold=event_confirmation_params.vote_threshold,
        sampling_step=event_confirmation_params.sampling_step,
        vote_weight=event_confirmation_params.vote_weight,
        )

    # Checking if all stations have equal voting rights.
    dict_vote_weight = {}
    if event_confirmation_params.vote_weight is not None:
        dict_vote_weight = _parse_str_collection_as_dict(event_confirmation_params.vote_weight)

    res = []

    for ev in confirmed_events:

        active_ids = [int(s) for s in ev['active_id']]
        active_event_detection_results = [x for x in event_detection_results if x.id in active_ids]

        # Checks if the event has several event_detection_results coming from the same datachunk
        sort_by_datachunk = collections.defaultdict(list)
        for result in active_event_detection_results:
            sort_by_datachunk[result.datachunk_id].append(result)

        for d_id in sort_by_datachunk:
            sort_by_datachunk[d_id] = min(sort_by_datachunk[d_id], key=lambda x: abs(x.time_start - ev['start']))  # type: ignore

        active_event_detection_results = list(sort_by_datachunk.values())  # type: ignore

        # Confirming that the event still has enough event to pass the vote threshold
        votes_number = 0
        for event_detection_result in active_event_detection_results:
            vote = 1
            if event_detection_result.datachunk.device.station in dict_vote_weight:
                vote = int(dict_vote_weight[event_detection_result.datachunk.device.station])
            votes_number += vote
        if votes_number < event_confirmation_params.vote_threshold:
            continue

        event_confirmation_file = EventConfirmationFile()
        event_confirmation_file.find_empty_folder_path(
            cmp=None,
            ts=timespan,
            params=event_confirmation_params,
            time_start=str(ev['start'].strftime('%Hh%Mm%Ss')),
        )
        logger.debug(f"FILEPATH {event_confirmation_file.filepath} .")

        _save_event_files(active_event_detection_results, ev['start'], ev['stop'], event_confirmation_file.filepath)

        res.append(
            EventConfirmationResult(
                event_confirmation_params_id=event_confirmation_params.id,
                timespan_id=timespan.id,
                time_start=validate_timestamp_as_pydatetime(ev['start']),
                time_stop=validate_timestamp_as_pydatetime(ev['stop']),
                peak_ground_velocity=max(s.peak_ground_velocity for s in active_event_detection_results),
                number_station_triggered=len(active_event_detection_results),
                file=event_confirmation_file,
                event_detection_results=active_event_detection_results,
                event_confirmation_run_id=event_confirmation_run.id,
            )
        )

    return res


def event_confirmation(
    timespan: Timespan,
    event_detection_results: Collection[EventDetectionResult],
    time_lag: int = 5,
    sampling_step: float = 0.5,
    vote_threshold: int = 1,
    vote_weight: Optional[Collection[str]] = None,
) -> List[Dict]:
    """
    Samples a timeframe in intervals of (sampling_step) seconds and indexes
    the starttime and endtime of the events stored in event_detection_results.
    For each time events, a new columns is created in a dataframe with 1
    if the events occured in an interval, 0 if not. Once every events in
    event_detection_results is parsed, the results are compared to identify
    the confirmed event. They are returned in a list of Dict with
    'start', 'stop', 'active_id' as keys.

    :param timespan: the timespan to parse
    :type timespan: Timespan
    :param event_detection_results: a collection of EventDetectionResult
    :type event_detection_results: Collection[EventDetectionResult]
    :param time_lag: the time lag tolerated between concomitant EventDetectionResult.
    :type time_lag: int
    :param sampling_step: the length of the intervals sampling the given timespan
    :type sampling_step: float
    :param vote_threshold: the number of votes needed to declare a confirmed event.
    :type vote_threshold: int
    :param vote_weight: Adds a vote weight to specific stations.
    :type vote_weight: Collection[str]
    :return: a List storing one Dict per confirmed event.
    :rtype: List[Dict]
    """

    df = pd.to_datetime(np.arange(
                    timespan.starttime,
                    timespan.endtime,
                    timedelta(seconds=sampling_step)
                    ), utc=True).to_frame(name='start')
    df.reset_index(inplace=True, drop=True)
    df['stop'] = df['start'] + timedelta(seconds=sampling_step)
    df['votes'] = 0

    # Parsing the tuples with voting weight
    dict_vote_weight = {}
    if vote_weight is not None:
        dict_vote_weight = _parse_str_collection_as_dict(vote_weight)

    # classifying every event_detection_result in the dataframe.
    for event_detection_result in event_detection_results:

        t_start = event_detection_result.time_start - timedelta(seconds=time_lag)
        t_stop = event_detection_result.time_stop + timedelta(seconds=time_lag)
        t_id = event_detection_result.id
        vote = 1

        if event_detection_result.datachunk.device.station in dict_vote_weight:
            vote = int(dict_vote_weight[event_detection_result.datachunk.device.station])

        df[str(t_id)] = 0
        df.loc[(df['stop'] >= t_start) & (df['start'] < t_stop), [str(t_id)]] += vote
        df['votes'] += df[str(t_id)]

    # counting the vote and identifying confirmed events
    event_in_progress = False
    confirmed_event = []

    for _, row in df.iterrows():

        if row.votes >= vote_threshold:
            if not event_in_progress:
                event_in_progress = True
                event = dict(start=row.start)
                event['active_id'] = row[3:].index[row[3:].ge(1)].tolist()
            else:
                event['active_id'] += row[3:].index[row[3:].ge(1)].tolist()
                event['stop'] = row.stop

        else:
            if event_in_progress:
                event['active_id'] = list(dict.fromkeys(event['active_id']))
                event_in_progress = False
                confirmed_event.append(event)

    return confirmed_event


def _save_event_files(
    event_detection_results: Collection[EventDetectionResult],
    starttime: pd.Timestamp,
    endtime: pd.Timestamp,
    event_confirmation_folder_path: Path
):
    """
    Iterates on every EventDetectionResult in event_detection_results from a confirmed event
    and extractes the traces, cft and png of each Sta/Lta detection.

    :param event_detection_results: a collection of the EventDetectionResult used for the vote confirmation
    :type event_detection_results:  Collection[EventDetectionResult]
    :param starttime: the start of the event, time_lag included
    :type starttime: pd.Timestamp
    :param endtime: the end of the event, time_lag included
    :type endtime: pd.Timestamp
    :param event_confirmation_folder_path: the folder path where files will be saved.
    :type event_confirmation_folder_path: Path
    """

    multi_traces = Stream()

    for event_detection_result in event_detection_results:

        st = event_detection_result.datachunk.load_data()
        st.filter(
            type="bandpass",
            freqmin=event_detection_result.event_detection_params.minimum_frequency,
            freqmax=event_detection_result.event_detection_params.maximum_frequency,
        )

        st_copy = st.copy()
        tr = st_copy[0].trim(
            starttime=UTCDateTime(starttime),
            endtime=UTCDateTime(endtime),
            nearest_sample=False
        )
        multi_traces.append(tr)

        if event_detection_result.detection_type == "sta_lta":
            length_sta_sec = event_detection_result.event_detection_params.n_short_time_average / event_detection_result.event_detection_params.minimum_frequency
            length_lta_sec = event_detection_result.event_detection_params.n_long_time_average / event_detection_result.event_detection_params.minimum_frequency

            cft = classic_sta_lta(
                st[0].data,
                length_sta_sec * st[0].stats.sampling_rate,
                length_lta_sec * st[0].stats.sampling_rate
            )
            cft = np.vstack([st[0].times(type="utcdatetime"), cft])

            tmp = (cft[:, np.argwhere(cft[0, :] >= starttime)])
            tmp = tmp[:, :, 0]
            tmp = tmp[:, :len(tr.data)]
            characteristic_function = dict(times=tmp[0], cft=tmp[1])

            plot_trigger(
                trace=tr,
                cft=characteristic_function['cft'],
                thr_on=event_detection_result.event_detection_params.trigger_value,
                thr_off=event_detection_result.event_detection_params.detrigger_value,
                outfile=str(event_confirmation_folder_path / event_detection_result.file.trgpath.name)
            )
            np.savez(
                file=str(event_confirmation_folder_path / event_detection_result.file.npzpath.name),
                **characteristic_function
            )

        tr.write(str(event_confirmation_folder_path / event_detection_result.file.filepath.name), format="MSEED")
        # plot_trace(tr, event_detection_folder_path / event_detection_result.file.pngpath.name)

    with open(str(event_confirmation_folder_path / "multi-traces.pl"), 'wb') as pickle_file:
        pickle.dump(multi_traces, pickle_file)

    multi_traces.plot(
        size=(800, 1000),
        equal_scale=False,
        type="relative",
        outfile=str(event_confirmation_folder_path / "plot_multi-traces.png"),
        show=False
    )
    plt.close("all")


def plot_trace(
    trace: Trace,
    path: Union[Path, str]
):
    """
    Calls trace.plot() on a given trace and saves it using the given path.

    :param tr: an trace to plot
    :type tr:  obspy.Trace
    :param path: the save path
    :type path: Union[Path, str]
    """
    trace.plot(
        size=(800, 1000),
        equal_scale=False,
        type="relative",
        outfile=str(path),
        show=False
    )
    plt.close("all")


def plot_trigger(
    trace: Trace,
    cft: np.ndarray,
    thr_on: float,
    thr_off: float,
    outfile: Union[Path, str],
    show=False
):
    """
    Plot characteristic function of trigger along with waveform data and
    trigger On/Off from given thresholds.

    Note: this is a mirror of the obspy.trace.plot_trigger. A duplication was required to
    save the figures

    :type trace: :class:`~obspy.core.trace.Trace`
    :param trace: waveform data
    :type cft: :class:`numpy.ndarray`
    :param cft: characteristic function as returned by a trigger in
        :mod:`obspy.signal.trigger`
    :type thr_on: float
    :param thr_on: threshold for switching trigger on
    :type thr_off: float
    :param thr_off: threshold for switching trigger off
    :type show: bool
    :param show: Do not call `plt.show()` at end of routine. That way,
        further modifications can be done to the figure before showing it.
    """
    df = trace.stats.sampling_rate
    npts = trace.stats.npts
    t = np.arange(npts, dtype=np.float32) / df
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    ax1.plot(t, trace.data, 'k')
    ax2 = fig.add_subplot(212, sharex=ax1)
    ax2.plot(t, cft, 'k')
    on_off = np.array(trigger_onset(cft, thr_on, thr_off))
    i, j = ax1.get_ylim()
    try:
        ax1.vlines(on_off[:, 0] / df, i, j, color='r', lw=2,
                   label="Trigger On")
        ax1.vlines(on_off[:, 1] / df, i, j, color='b', lw=2,
                   label="Trigger Off")
        ax1.legend()
    except IndexError:
        pass
    ax2.axhline(thr_on, color='red', lw=1, ls='--')
    ax2.axhline(thr_off, color='blue', lw=1, ls='--')
    ax2.set_xlabel("Time after %s [s]" % trace.stats.starttime.isoformat())
    fig.suptitle(trace.id)
    fig.canvas.draw()
    if show:
        plt.show()
    fig.savefig(str(outfile))
    plt.close(fig)


def get_pgv_characteristic_function(
    trace: Trace,
    peak_ground_velocity_threshold: float,
    time_tolerance: float,
) -> np.ndarray :
    """
    Takes an input trace, convolve the absolute value of the signal with a
    time tolerance kernel and output a characteristic boolean function with 1 when
    the signal absolute value is above the peak_ground_velocity_threshold,
    with 0 when below.

    :param trace: an trace to plot
    :type trace:  obspy.Trace
    :param peak_ground_velocity_threshold: the detection threshold for pgv
    :type peak_ground_velocity_threshold: float
    :param time_tolerance: the time tolerance used for the time tolerance kernal in seconds
    :type time_tolerance: float
    :return: a characteristic boolean function with 1 and 0.
    :rtype: np.darray
    """

    # initialize char fun
    tr = trace.copy()
    t_axis = tr.times(reftime=tr.stats.starttime)
    char_fun_out = 0*t_axis

    # initialize convolution kernel
    dt = tr.stats.delta
    nt_kernel = int(time_tolerance/dt)
    kernel = np.ones(nt_kernel)

    # find points above pgv thresh
    char_fun_out[np.abs(tr.data) >= peak_ground_velocity_threshold] = 1

    if max(np.abs(tr.data)-peak_ground_velocity_threshold) >= 0:
        # convolve with time tolerance kernel and threshold the result above 0
        char_fun_conv = np.convolve(char_fun_out, kernel, mode='same')
        char_fun_out[char_fun_conv > 0] = 1

    return char_fun_out


def get_trigger_from_pgv(
    pgv_char_fun=np.ndarray,
) -> List[Dict] :
    """
    Parse a characteristic function and return a list of Dict with start & stop times
    for each consecutive time period where the function is over 1.

    :param pgv_char_fun: a  2d array with characteristic function and relevant times.
    :type pgv_char_fun:  np.darray
    :return: a list of dict with ['start'] and ['stop'] as keys.
    :rtype: list[dict]
    """

    multi_trigger = []
    event_in_progress = False

    for i in range(len(pgv_char_fun[0, :])):
        if pgv_char_fun[1, i] > 0:
            if not event_in_progress:
                event = {}
                event['start'] = pgv_char_fun[0, i]
                event_in_progress = True
            else:
                event['stop'] = pgv_char_fun[0, i]

        elif event_in_progress:
            event_in_progress = False
            multi_trigger.append(event)

    return multi_trigger


def _parse_str_collection_as_dict(
        collection_str: Collection[str]
) -> dict:
    """
    Parse a string collection storing keys and values separated by :,- or \s and returns
    it in a dict[key]=value. Mostly used for station:EventDetectionParamsID or
    station:vote_weight

    :param collection_str: a collection of string storing a key:value combos.
    :type Collection[str]
    :return: A dict with key and value
    :rtype: dict
    """
    if collection_str is None:
        raise ValueError('The string collection to parse is None.')
    new_dict = {}
    for sp in collection_str:
        tmp_list = re.split('-|:|\s', sp)
        new_dict[tmp_list[0]] = tmp_list[1]
    return new_dict
