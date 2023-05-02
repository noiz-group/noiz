# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
from loguru import logger
from obspy.signal.cross_correlation import correlate
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import subqueryload, Query
from sqlalchemy.sql import Insert
from typing import List, Union, Optional, Collection, Dict, Generator, Tuple, Any

from noiz.api.component_pair import fetch_componentpairs_cartesian, fetch_componentpairs_cartesian_by_id
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_on_dask, \
    _run_calculate_and_upsert_sequentially
from noiz.api.processing_config import fetch_crosscorrelation_params_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import InconsistentDataException, CorruptedDataException
from noiz.models import ComponentPairCartesian, CrosscorrelationFile, Crosscorrelation, Datachunk, ProcessedDatachunk, \
    CrosscorrelationParams, Timespan
from noiz.models.type_aliases import CrosscorrelationRunnerInputs
from noiz.processing.crosscorrelations import validate_component_code_pairs, group_chunks_by_timespanid_componentid, \
    load_data_for_chunks, extract_component_ids_from_component_pairs, assembly_ccf_dataframe
from noiz.processing.io import write_ccfs_to_npz
from noiz.validation_helpers import validate_to_tuple


def fetch_crosscorrelation(
        crosscorrelation_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_params: bool = False,
) -> List[Crosscorrelation]:
    """filldocs"""

    query = _query_crosscorrelation(
        crosscorrelation_params_id=crosscorrelation_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_params=load_crosscorrelation_params,
    )

    return query.all()


def count_crosscorrelation(
        crosscorrelation_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_params: bool = False,
) -> int:
    """filldocs"""
    query = _query_crosscorrelation(
        crosscorrelation_params_id=crosscorrelation_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_params=load_crosscorrelation_params,
    )

    return query.count()


def _query_crosscorrelation(
        crosscorrelation_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_params: bool = False,
) -> Query:
    """filldocs"""
    filters = []

    if crosscorrelation_params_id is not None:
        filters.append(Crosscorrelation.crosscorrelation_params_id == crosscorrelation_params_id)
    if componentpair_id is not None:
        filters.append(Crosscorrelation.componentpair_id.in_(componentpair_id))
    if timespan_id is not None:
        filters.append(Crosscorrelation.timespan_id.in_(timespan_id))
    if len(filters) == 0:
        filters.append(True)

    opts = []
    if load_timespan:
        opts.append(subqueryload(Crosscorrelation.timespan))
    if load_componentpair:
        opts.append(subqueryload(Crosscorrelation.componentpair_cartesian))
    if load_crosscorrelation_params:
        opts.append(subqueryload(Crosscorrelation.crosscorrelation_params))

    return db.session.query(Crosscorrelation).filter(*filters).options(opts)


def _prepare_upsert_command_crosscorrelation(xcorr: Crosscorrelation) -> Insert:
    insert_command = (
        insert(Crosscorrelation)
        .values(
            crosscorrelation_params_id=xcorr.crosscorrelation_params_id,
            componentpair_id=xcorr.componentpair_id,
            timespan_id=xcorr.timespan_id,
            ccf=xcorr.ccf,
        )
        .on_conflict_do_update(
            constraint="unique_ccf_per_timespan_per_componentpair_per_config",
            set_=dict(ccf=xcorr.ccf),
        )
    )
    return insert_command


def perform_crosscorrelations(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        network_codes_a: Optional[Union[Collection[str], str]] = None,
        station_codes_a: Optional[Union[Collection[str], str]] = None,
        component_codes_a: Optional[Union[Collection[str], str]] = None,
        network_codes_b: Optional[Union[Collection[str], str]] = None,
        station_codes_b: Optional[Union[Collection[str], str]] = None,
        component_codes_b: Optional[Union[Collection[str], str]] = None,
        accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
        include_autocorrelation: Optional[bool] = False,
        include_intracorrelation: Optional[bool] = False,
        only_autocorrelation: Optional[bool] = False,
        only_intracorrelation: Optional[bool] = False,
        raise_errors: bool = False,
        batch_size: int = 5000,
        parallel: bool = True,
) -> None:
    """
    Performs crosscorrelations according to provided set of selectors.
    Uses Dask.distributed for parallelism.
    All the calculations are divided into batches in order to speed up queries that gather all the inputs.

    :param crosscorrelation_params_id: ID of CrosscorrelationParams object to be used as config
    :type crosscorrelation_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param network_codes_a: Selector for network code of A station in the pair
    :type network_codes_a: Optional[Union[Collection[str], str]]
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]]
    :param component_codes_a: Selector for component code of A station in the pair
    :type component_codes_a: Optional[Union[Collection[str], str]]
    :param network_codes_b: Selector for network code of B station in the pair
    :type network_codes_b: Optional[Union[Collection[str], str]]
    :param station_codes_b: Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]]
    :param component_codes_b: Selector for component code of B station in the pair
    :type component_codes_b: Optional[Union[Collection[str], str]]
    :param accepted_component_code_pairs: Collection of component code pairs that should be fetched
    :type accepted_component_code_pairs: Optional[Union[Collection[str], str]]
    :param include_autocorrelation: If autocorrelation pairs should be also included
    :type include_autocorrelation: Optional[bool]
    :param include_intracorrelation: If intracorrelation pairs should be also included
    :type include_intracorrelation: Optional[bool]
    :param only_autocorrelation: If only autocorrelation pairs should be selected
    :type only_autocorrelation: Optional[bool]
    :param only_intracorrelation: If only intracorrelation pairs should be selected
    :type only_intracorrelation: Optional[bool]
    :param raise_errors: If errors should be raised or just logged
    :type raise_errors: bool
    :param batch_size: How big should be the batch of calculations
    :type batch_size: int
    :param parallel: If the calculations should be done in parallel
    :type parallel: bool
    :return: None
    :rtype: NoneType
    """

    calculation_inputs = _prepare_inputs_for_crosscorrelations(
        crosscorrelation_params_id=crosscorrelation_params_id,
        starttime=starttime,
        endtime=endtime,
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation,
            raise_errors=raise_errors,
            with_file=True,
        )
    return


def _prepare_inputs_for_crosscorrelations(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        network_codes_a: Optional[Union[Collection[str], str]] = None,
        station_codes_a: Optional[Union[Collection[str], str]] = None,
        component_codes_a: Optional[Union[Collection[str], str]] = None,
        network_codes_b: Optional[Union[Collection[str], str]] = None,
        station_codes_b: Optional[Union[Collection[str], str]] = None,
        component_codes_b: Optional[Union[Collection[str], str]] = None,
        accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
        include_autocorrelation: Optional[bool] = False,
        include_intracorrelation: Optional[bool] = False,
        only_autocorrelation: Optional[bool] = False,
        only_intracorrelation: Optional[bool] = False,
) -> Generator[CrosscorrelationRunnerInputs, None, None]:
    """
    Performs all the database queries to prepare all the data required for running crosscorrelations.
    Returns a tuple of inputs specific for the further calculations.

    :param crosscorrelation_params_id: ID of CrosscorrelationParams object to use
    :type crosscorrelation_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param network_codes_a: Selector for network code of A station in the pair
    :type network_codes_a: Optional[Union[Collection[str], str]]
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]]
    :param component_codes_a: Selector for component code of A station in the pair
    :type component_codes_a: Optional[Union[Collection[str], str]]
    :param network_codes_b: Selector for network code of B station in the pair
    :type network_codes_b: Optional[Union[Collection[str], str]]
    :param station_codes_b: Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]]
    :param component_codes_b: Selector for component code of B station in the pair
    :type component_codes_b: Optional[Union[Collection[str], str]]
    :param include_autocorrelation: If autocorrelation pairs should be also included
    :type include_autocorrelation: Optional[bool]
    :param include_intracorrelation: If intracorrelation pairs should be also included
    :type include_intracorrelation: Optional[bool]
    :param only_autocorrelation: If only autocorrelation pairs should be selected
    :type only_autocorrelation: Optional[bool]
    :param only_intracorrelation: If only intracorrelation pairs should be selected
    :type only_intracorrelation: Optional[bool]
    :return:
    :rtype:
    """

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_timespans_ids = extract_object_ids(fetched_timespans)
    logger.info(f"There are {len(fetched_timespans_ids)} timespan to process")

    if accepted_component_code_pairs is not None:
        accepted_component_code_pairs = validate_component_code_pairs(
            component_pairs=validate_to_tuple(accepted_component_code_pairs, str)
        )

    fetched_component_pairs: List[ComponentPairCartesian] = fetch_componentpairs_cartesian(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )
    logger.info(f"There are {len(fetched_component_pairs)} component pairs to process.")

    single_component_ids = extract_component_ids_from_component_pairs(fetched_component_pairs)
    logger.info(f"There are in total {len(single_component_ids)} unique components to be fetched from db.")

    params = fetch_crosscorrelation_params_by_id(id=crosscorrelation_params_id)
    logger.info(f"Fetched correlation_params object {params}")
    fetched_processed_datachunks = (
        db.session.query(Timespan, ProcessedDatachunk)
                  .join(Datachunk, Timespan.id == Datachunk.timespan_id)
                  .join(ProcessedDatachunk, Datachunk.id == ProcessedDatachunk.datachunk_id)
                  .filter(
                      Timespan.id.in_(fetched_timespans_ids),  # type: ignore
                      ProcessedDatachunk.processed_datachunk_params_id == params.processed_datachunk_params_id,
                      Datachunk.component_id.in_(single_component_ids),
        )
        .options(
            subqueryload(ProcessedDatachunk.datachunk)
        )
        .all())
    grouped_datachunks = group_chunks_by_timespanid_componentid(processed_datachunks=fetched_processed_datachunks)

    for timespan, grouped_processed_chunks in grouped_datachunks.items():
        db.session.expunge_all()
        yield CrosscorrelationRunnerInputs(
            timespan=timespan,
            crosscorrelation_params=params,
            grouped_processed_chunks=grouped_processed_chunks,
            component_pairs=tuple(fetched_component_pairs)
        )
    return


def _crosscorrelate_for_timespan_wrapper(
        inputs: CrosscorrelationRunnerInputs,
) -> Tuple[Crosscorrelation, ...]:
    """
    Thin wrapper around :py:meth:`noiz.api.crosscorrelations._crosscorrelate_for_timespan` translating
    single input TypedDict to standard keyword arguments and converting output to a Tuple.

    :param inputs: Input dictionary
    :type inputs: ~noiz.api.type_aliases.CrosscorrelationRunnerInputs
    :return: Finished Crosscorrelations in form of tuple
    :rtype: Tuple[~noiz.models.crosscorrelation.Crosscorrelation, ...]
    """
    return tuple(
        _crosscorrelate_for_timespan(
            timespan=inputs["timespan"],
            params=inputs["crosscorrelation_params"],
            grouped_processed_chunks=inputs["grouped_processed_chunks"],
            component_pairs=inputs["component_pairs"],
        )
    )


def assembly_ccf_filename(
        component_pair: ComponentPairCartesian,
        timespan: Timespan,
        count: int = 0
) -> str:
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    filename = ".".join([
        component_pair.component_a.network,
        component_pair.component_a.station,
        component_pair.component_a.component,
        component_pair.component_b.network,
        component_pair.component_b.station,
        component_pair.component_b.component,
        year,
        doy_time,
        str(count),
        "npy"
    ])

    return filename


def assembly_ccf_dir(component_pair: ComponentPairCartesian, timespan: Timespan) -> Path:
    """
    Assembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component_pair: Component object containing information about used channel
    :type component_pair: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Path object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(str(timespan.starttime.month))
        .joinpath(component_pair.component_code_pair)
        .joinpath(f"{component_pair.component_a.network}.{component_pair.component_a.station}-"
                  f"{component_pair.component_b.network}.{component_pair.component_b.station}")
    )


def _crosscorrelate_for_timespan(
        timespan: Timespan,
        params: CrosscorrelationParams,
        grouped_processed_chunks: Dict[int, ProcessedDatachunk],
        component_pairs: Tuple[ComponentPairCartesian, ...]
) -> List[Crosscorrelation]:
    """filldocs"""
    from noiz.globals import PROCESSED_DATA_DIR
    from noiz.processing.path_helpers import assembly_filepath, \
        increment_filename_counter, parent_directory_exists_or_create

    import numpy as np
    logger.info(f"Running crosscorrelation for {timespan}")

    logger.debug(f"Loading data for timespan {timespan}")
    try:
        streams = load_data_for_chunks(chunks=grouped_processed_chunks)
    except CorruptedDataException as e:
        logger.error(e)
        raise CorruptedDataException(e)
    xcorrs = []
    for pair in component_pairs:
        cmp_a_id = pair.component_a_id
        cmp_b_id = pair.component_b_id

        if cmp_a_id not in grouped_processed_chunks.keys() or cmp_b_id not in grouped_processed_chunks.keys():
            logger.debug(f"No data for pair {pair}")
            continue

        logger.debug(f"Processed chunks for {pair} are present. Starting processing.")

        if streams[cmp_a_id].data.shape != streams[cmp_b_id].data.shape:
            msg = f"The shapes of data arrays for {cmp_a_id} and {cmp_b_id} are different. " \
                  f"Shapes: {cmp_a_id} is {streams[cmp_a_id].data.shape} " \
                  f"{cmp_b_id} is {streams[cmp_b_id].data.shape} "
            logger.error(msg)
            raise InconsistentDataException(msg)

        ccf_data = correlate(
            a=streams[cmp_a_id],
            b=streams[cmp_b_id],
            shift=params.correlation_max_lag_samples,
        )

        filepath = assembly_filepath(
            PROCESSED_DATA_DIR,  # type: ignore
            "ccf",
            assembly_ccf_dir(component_pair=pair, timespan=timespan) \
            .joinpath(assembly_ccf_filename(
                component_pair=pair,
                timespan=timespan,
                count=0
            )),
        )

        if filepath.exists():
            logger.debug(f"Filepath {filepath} exists. "
                         f"Trying to find next free one.")
            filepath = increment_filename_counter(filepath=filepath, extension=True)
            logger.debug(f"Free filepath found. "
                         f"CCF will be saved to {filepath}")

        logger.info(f"CCF will be written to {str(filepath)}")
        parent_directory_exists_or_create(filepath)

        ccf_file = CrosscorrelationFile(filepath=str(filepath))

        np.save(file=ccf_file.filepath, arr=ccf_data)

        xcorr = Crosscorrelation(
            crosscorrelation_params_id=params.id,
            componentpair_id=pair.id,
            timespan_id=timespan.id,
            file=ccf_file,
        )

        xcorrs.append(xcorr)
    return xcorrs


def fetch_crosscorrelations_and_save(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        dirpath: Path,
        overwrite: bool = False,
        network_codes_a: Optional[Union[Collection[str], str]] = None,
        station_codes_a: Optional[Union[Collection[str], str]] = None,
        component_codes_a: Optional[Union[Collection[str], str]] = None,
        network_codes_b: Optional[Union[Collection[str], str]] = None,
        station_codes_b: Optional[Union[Collection[str], str]] = None,
        component_codes_b: Optional[Union[Collection[str], str]] = None,
        accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
        include_autocorrelation: Optional[bool] = False,
        include_intracorrelation: Optional[bool] = False,
        only_autocorrelation: Optional[bool] = False,
        only_intracorrelation: Optional[bool] = False,
):
    component_pairs = fetch_componentpairs_cartesian(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )

    logger.info(f"Found {len(component_pairs)} for provided parameters.")

    dirpath = dirpath.absolute()
    if dirpath.exists() and dirpath.is_file():
        raise FileExistsError("Provided dirpath should be a directory. It is a file.")

    dirpath.mkdir(exist_ok=True, parents=True)

    for pair in component_pairs:
        logger.info(f"Fetching data for pair {pair}")
        filepath = dirpath.joinpath(f"raw_ccfs_{pair}.npz")

        try:
            final_path = fetch_crosscorrelations_single_pair_and_save(
                crosscorrelation_params_id=crosscorrelation_params_id,
                starttime=starttime,
                endtime=endtime,
                filepath=filepath,
                component_pair=pair,
                overwrite=overwrite,
            )
        except FileExistsError:
            logger.error(f"File {filepath} with data for pair {pair} exists. "
                         f"It will not be overwritten. "
                         f"If you want to overwrite it, pass overwrite=True."
                         f"Skipping to the next pair. ")
            continue
        if final_path is None:
            continue

        logger.info(f"File {final_path} with data for pair {pair} was successfully written.")
    return


def fetch_crosscorrelations_single_pair_and_save(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        filepath: Path,
        component_pair: Optional[ComponentPairCartesian] = None,
        component_pair_id: Optional[int] = None,
        overwrite: bool = False,
) -> Optional[Path]:
    if component_pair_id is not None and component_pair is not None:
        raise ValueError("You cannot provide both component_pair and component_pair_id")
    if component_pair_id is None and component_pair is None:
        raise ValueError("You have to provide one of component_pair or component_pair_id")
    if component_pair_id is not None:
        pairs = fetch_componentpairs_cartesian_by_id(component_pair_id=component_pair_id)
        if len(pairs) != 1:
            raise ValueError(f"Expected only one component pair. Got {len(pairs)}.")
        else:
            pair = pairs[0]
    if component_pair is not None:
        pair = component_pair

    params = fetch_crosscorrelation_params_by_id(id=crosscorrelation_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    # noinspection PyUnboundLocalVariable
    fetched_ccfs = fetch_crosscorrelation(
        componentpair_id=(pair.id,),
        load_timespan=True,
        timespan_id=extract_object_ids(timespans)
    )

    if len(fetched_ccfs) == 0:
        logger.info(f"There are no crosscorrelations for pair {component_pair}")
        return None

    df = assembly_ccf_dataframe(fetched_ccfs, params)

    metadata = prepare_metadata_for_saving_raw_ccf_file(pair, starttime, endtime, params)

    write_ccfs_to_npz(
        df=df,
        filepath=filepath,
        overwrite=overwrite,
        metadata_keys=metadata.keys(),
        metadata_values=metadata.values(),
    )
    return filepath


def prepare_metadata_for_saving_raw_ccf_file(pair: ComponentPairCartesian, starttime, endtime, config):
    from noiz import __version__

    processing_parameters_dict = get_parent_configs_as_dict(config=config)

    metadata = {
        "noiz_version": __version__,
        "type": "raw_crosscorrelations_without_qc2_selection",
        "starttime": str(starttime),
        "endtime": str(endtime),
        "component_pair": str(pair),
    }
    metadata.update(processing_parameters_dict)
    return metadata


def get_parent_configs_as_dict(config) -> Dict[str, Any]:
    return {}
