# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import more_itertools

from loguru import logger
from obspy.signal.cross_correlation import correlate
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import subqueryload, Query
from sqlalchemy.sql import Insert
from typing import List, Union, Optional, Collection, Dict, Generator, Tuple, Any, FrozenSet

from noiz.api.component import fetch_components_by_id
from noiz.api.component_pair import fetch_componentpairs_cartesian, fetch_componentpairs_cartesian_by_id, \
    fetch_componentpairs_cylindrical
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_on_dask, \
    _run_calculate_and_upsert_sequentially, extract_object_ids_keep_objects
from noiz.api.processing_config import fetch_crosscorrelation_cartesian_params_by_id, \
    fetch_crosscorrelation_cylindrical_params_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import InconsistentDataException, CorruptedDataException
from noiz.models import ComponentPairCartesian, CrosscorrelationCartesianFile, CrosscorrelationCartesian, \
    Datachunk, ProcessedDatachunk, CrosscorrelationCartesianParams, Timespan, CrosscorrelationCylindrical, \
    ComponentPairCylindrical, CrosscorrelationCylindricalFile, CrosscorrelationCylindricalParams
from noiz.models.type_aliases import CrosscorrelationCartesianRunnerInputs, CrosscorrelationCylindricalRunnerInputs
from noiz.processing.crosscorrelations import validate_component_code_pairs, group_chunks_by_timespanid_componentid, \
    load_data_for_chunks, extract_component_ids_from_component_pairs_cartesian, assembly_ccf_cartesian_dataframe, \
    group_xcrorrcartesian_by_timespanid_componentids, _fetch_R_T_xcoor, _computation_cylindrical_correlation_R_T, _fetch_RT_Z_xcoor, \
    _computation_cylindrical_correlation_RT_Z, _fetch_Z_TR_xcoor, _computation_cylindrical_correlation_Z_TR
from noiz.processing.io import write_ccfs_to_npz
from noiz.processing.path_helpers import assembly_filepath, increment_filename_counter, parent_directory_exists_or_create
from noiz.validation_helpers import validate_to_tuple


def fetch_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_cartesian_params: bool = False,
) -> List[CrosscorrelationCartesian]:
    """filldocs"""

    query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_cartesian_params=load_crosscorrelation_cartesian_params,
    )

    return query.all()


def count_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_cartesian_params: bool = False,
) -> int:
    """filldocs"""
    query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_cartesian_params=load_crosscorrelation_cartesian_params,
    )

    return query.count()


def _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id: Optional[int] = None,
        componentpair_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        load_componentpair: bool = False,
        load_timespan: bool = False,
        load_crosscorrelation_cartesian_params: bool = False,
) -> Query:
    """filldocs"""
    filters = []

    if crosscorrelation_cartesian_params_id is not None:
        filters.append(CrosscorrelationCartesian.crosscorrelation_cartesian_params_id == crosscorrelation_cartesian_params_id)
    if componentpair_id is not None:
        filters.append(CrosscorrelationCartesian.componentpair_id.in_(componentpair_id))
    if timespan_id is not None:
        filters.append(CrosscorrelationCartesian.timespan_id.in_(timespan_id))
    if len(filters) == 0:
        filters.append(True)

    opts = []
    if load_timespan:
        opts.append(subqueryload(CrosscorrelationCartesian.timespan))
    if load_componentpair:
        opts.append(subqueryload(CrosscorrelationCartesian.componentpair_cartesian))
    if load_crosscorrelation_cartesian_params:
        opts.append(subqueryload(CrosscorrelationCartesian.crosscorrelation_cartesian_params))

    return db.session.query(CrosscorrelationCartesian).filter(*filters).options(opts)


def _prepare_upsert_command_crosscorrelation_cartesian(xcorr: CrosscorrelationCartesian) -> Insert:
    insert_command = (
        insert(CrosscorrelationCartesian)
        .values(
            crosscorrelation_cartesian_params_id=xcorr.crosscorrelation_cartesian_params_id,
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


def perform_crosscorrelations_cartesian(
        crosscorrelation_cartesian_params_id: int,
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
    Performs crosscorrelations_cartesian according to provided set of selectors.
    Uses Dask.distributed for parallelism.
    All the calculations are divided into batches in order to speed up queries that gather all the inputs.

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams object to be used as config
    :type crosscorrelation_cartesian_params_id: int
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

    calculation_inputs = _prepare_inputs_for_crosscorrelations_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
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
            upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
            raise_errors=raise_errors,
            with_file=True,
        )
    return


def _prepare_inputs_for_crosscorrelations_cartesian(
        crosscorrelation_cartesian_params_id: int,
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
) -> Generator[CrosscorrelationCartesianRunnerInputs, None, None]:
    """
    Performs all the database queries to prepare all the data required for running crosscorrelations_cartesian.
    Returns a tuple of inputs specific for the further calculations.

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams object to use
    :type crosscorrelation_cartesian_params_id: int
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
            component_pairs_cartesian=validate_to_tuple(accepted_component_code_pairs, str)
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

    single_component_ids = extract_component_ids_from_component_pairs_cartesian(fetched_component_pairs)
    logger.info(f"There are in total {len(single_component_ids)} unique components to be fetched from db.")

    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)
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
        yield CrosscorrelationCartesianRunnerInputs(
            timespan=timespan,
            crosscorrelation_cartesian_params=params,
            grouped_processed_chunks=grouped_processed_chunks,
            component_pairs_cartesian=tuple(fetched_component_pairs)
        )
    return


def _crosscorrelate_for_timespan_wrapper(
        inputs: CrosscorrelationCartesianRunnerInputs,
) -> Tuple[CrosscorrelationCartesian, ...]:
    """
    Thin wrapper around :py:meth:`noiz.api.crosscorrelations._crosscorrelate_for_timespan` translating
    single input TypedDict to standard keyword arguments and converting output to a Tuple.

    :param inputs: Input dictionary
    :type inputs: ~noiz.api.type_aliases.CrosscorrelationCartesianRunnerInputs
    :return: Finished CrosscorrelationCartesians in form of tuple
    :rtype: Tuple[~noiz.models.crosscorrelation.CrosscorrelationCartesian, ...]
    """
    return tuple(
        _crosscorrelate_for_timespan(
            timespan=inputs["timespan"],
            params=inputs["crosscorrelation_cartesian_params"],
            grouped_processed_chunks=inputs["grouped_processed_chunks"],
            component_pairs_cartesian=inputs["component_pairs_cartesian"],
        )
    )


def assembly_ccf_filename(
        component_pair_cartesian: ComponentPairCartesian,
        timespan: Timespan,
        count: int = 0
) -> str:
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    filename = ".".join([
        component_pair_cartesian.component_a.network,
        component_pair_cartesian.component_a.station,
        component_pair_cartesian.component_a.component,
        component_pair_cartesian.component_b.network,
        component_pair_cartesian.component_b.station,
        component_pair_cartesian.component_b.component,
        year,
        doy_time,
        str(count),
        "npy"
    ])

    return filename


def assembly_ccf_dir(component_pair_cartesian: ComponentPairCartesian, timespan: Timespan) -> Path:
    """
    Assembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component_pair_cartesian: Component object containing information about used channel
    :type component_pair_cartesian: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Path object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(str(timespan.starttime.month))
        .joinpath(component_pair_cartesian.component_code_pair)
        .joinpath(f"{component_pair_cartesian.component_a.network}.{component_pair_cartesian.component_a.station}-"
                  f"{component_pair_cartesian.component_b.network}.{component_pair_cartesian.component_b.station}")
    )


def _crosscorrelate_for_timespan(
        timespan: Timespan,
        params: CrosscorrelationCartesianParams,
        grouped_processed_chunks: Dict[int, ProcessedDatachunk],
        component_pairs_cartesian: Tuple[ComponentPairCartesian, ...]
) -> List[CrosscorrelationCartesian]:
    """filldocs"""
    from noiz.globals import PROCESSED_DATA_DIR
    from noiz.processing.path_helpers import assembly_filepath, \
        increment_filename_counter, parent_directory_exists_or_create

    import numpy as np
    logger.info(f"Running crosscorrelation_cartesian for {timespan}")

    logger.debug(f"Loading data for timespan {timespan}")
    try:
        streams = load_data_for_chunks(chunks=grouped_processed_chunks)
    except CorruptedDataException as e:
        logger.error(e)
        raise CorruptedDataException(e)
    xcorrs = []
    for pair in component_pairs_cartesian:
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
            assembly_ccf_dir(component_pair_cartesian=pair, timespan=timespan) \
            .joinpath(assembly_ccf_filename(
                component_pair_cartesian=pair,
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

        ccf_file = CrosscorrelationCartesianFile(filepath=str(filepath))

        np.save(file=ccf_file.filepath, arr=ccf_data)

        xcorr = CrosscorrelationCartesian(
            crosscorrelation_cartesian_params_id=params.id,
            componentpair_id=pair.id,
            timespan_id=timespan.id,
            file=ccf_file,
        )

        xcorrs.append(xcorr)
    return xcorrs


def fetch_crosscorrelations_cartesian_and_save(
        crosscorrelation_cartesian_params_id: int,
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
    component_pairs_cartesian = fetch_componentpairs_cartesian(
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

    logger.info(f"Found {len(component_pairs_cartesian)} for provided parameters.")

    dirpath = dirpath.absolute()
    if dirpath.exists() and dirpath.is_file():
        raise FileExistsError("Provided dirpath should be a directory. It is a file.")

    dirpath.mkdir(exist_ok=True, parents=True)

    for pair in component_pairs_cartesian:
        logger.info(f"Fetching data for pair {pair}")
        filepath = dirpath.joinpath(f"raw_ccfs_{pair}.npz")

        try:
            final_path = fetch_crosscorrelations_cartesian_single_pair_and_save(
                crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
                starttime=starttime,
                endtime=endtime,
                filepath=filepath,
                component_pair_cartesian=pair,
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


def fetch_crosscorrelations_cartesian_single_pair_and_save(
        crosscorrelation_cartesian_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        filepath: Path,
        component_pair_cartesian: Optional[ComponentPairCartesian] = None,
        component_pair_cartesian_id: Optional[int] = None,
        overwrite: bool = False,
) -> Optional[Path]:
    if component_pair_cartesian_id is not None and component_pair_cartesian is not None:
        raise ValueError("You cannot provide both component_pair_cartesian and component_pair_cartesian_id")
    if component_pair_cartesian_id is None and component_pair_cartesian is None:
        raise ValueError("You have to provide one of component_pair_cartesian or component_pair_cartesian_id")
    if component_pair_cartesian_id is not None:
        pairs = fetch_componentpairs_cartesian_by_id(component_pair_cartesian_id=component_pair_cartesian_id)
        if len(pairs) != 1:
            raise ValueError(f"Expected only one component pair. Got {len(pairs)}.")
        else:
            pair = pairs[0]
    if component_pair_cartesian is not None:
        pair = component_pair_cartesian

    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    # noinspection PyUnboundLocalVariable
    fetched_ccfs = fetch_crosscorrelation_cartesian(
        componentpair_id=(pair.id,),
        load_timespan=True,
        timespan_id=extract_object_ids(timespans)
    )

    if len(fetched_ccfs) == 0:
        logger.info(f"There are no crosscorrelations_cartesian for pair {component_pair_cartesian}")
        return None

    df = assembly_ccf_cartesian_dataframe(fetched_ccfs, params)

    metadata = prepare_metadata_for_saving_raw_ccf_cartesian_file(pair, starttime, endtime, params)

    write_ccfs_to_npz(
        df=df,
        filepath=filepath,
        overwrite=overwrite,
        metadata_keys=metadata.keys(),
        metadata_values=metadata.values(),
    )
    return filepath


def prepare_metadata_for_saving_raw_ccf_cartesian_file(pair: ComponentPairCartesian, starttime, endtime, config):
    from noiz import __version__

    processing_parameters_dict = get_parent_configs_as_dict(config=config)

    metadata = {
        "noiz_version": __version__,
        "type": "raw_crosscorrelations_cartesian_without_qc2_selection",
        "starttime": str(starttime),
        "endtime": str(endtime),
        "component_pair_cartesian": str(pair),
    }
    metadata.update(processing_parameters_dict)
    return metadata


def get_parent_configs_as_dict(config) -> Dict[str, Any]:
    return {}


def _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan):
    """
    Creating a file containing the cylindrical crosscorrelation previously computed

    :param component_pair_cylindrical: component_pair_cylindrical
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param xcorr_cylindrical: cylindrical crosscorrelation previously computed
    :type xcorr_cylindrical: array
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return: file
    :rtype:
    """

    import numpy as np
    from noiz.globals import PROCESSED_DATA_DIR

    filepath = assembly_filepath(
            PROCESSED_DATA_DIR,  # type: ignore
            "ccf_cylindrical",
            assembly_ccf_cylindrical_dir(component_pair_cylindrical=component_pair_cylindrical, timespan=timespan) \
            .joinpath(assembly_ccf_cylindrical_filename(
                component_pair_cylindrical=component_pair_cylindrical,
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

    ccf_file = CrosscorrelationCylindricalFile(filepath=str(filepath))

    np.save(file=ccf_file.filepath, arr=xcorr_cylindrical)

    return ccf_file


def assembly_ccf_cylindrical_dir(
    component_pair_cylindrical: ComponentPairCylindrical,
    timespan: Timespan,
) -> Path:
    """
    Assembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component_pair_cylindrical: Component object containing information about used channel
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return: Path object containing SDS-like directory hierarchy.
    :rtype: Path
    """

    if (component_pair_cylindrical.component_aE is None) & (component_pair_cylindrical.component_aN is None):
        a_network = component_pair_cylindrical.component_aZ.network
        a_station = component_pair_cylindrical.component_aZ.station
    else:
        a_network = component_pair_cylindrical.component_aE.network
        a_station = component_pair_cylindrical.component_aE.station

    if (component_pair_cylindrical.component_bE is None) & (component_pair_cylindrical.component_bN is None):
        b_network = component_pair_cylindrical.component_bZ.network
        b_station = component_pair_cylindrical.component_bZ.station
    else:
        b_network = component_pair_cylindrical.component_bE.network
        b_station = component_pair_cylindrical.component_bE.station

    return (
        Path(str(timespan.starttime.year))
        .joinpath(str(timespan.starttime.month))
        .joinpath(component_pair_cylindrical.component_cylindrical_code_pair)
        .joinpath(f"{a_network}.{a_station}-"
                  f"{b_network}.{b_station}")
    )


def assembly_ccf_cylindrical_filename(
        component_pair_cylindrical: ComponentPairCylindrical,
        timespan: Timespan,
        count: int = 0
) -> str:
    """
    Creating the filename for saving cylindrical crosscorrelation file

    :param component_pair_cylindrical: Component object containing information about used channel
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :param count: counter for increasing if filename exits, defaults to 0
    :type count: int, optional
    :return: filename to save cylindrical crosscorrelation
    :rtype: str
    """

    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    if (component_pair_cylindrical.component_aE is None) & (component_pair_cylindrical.component_aN is None):
        a_network = component_pair_cylindrical.component_aZ.network
        a_station = component_pair_cylindrical.component_aZ.station
    else:
        a_network = component_pair_cylindrical.component_aE.network
        a_station = component_pair_cylindrical.component_aE.station

    if (component_pair_cylindrical.component_bE is None) & (component_pair_cylindrical.component_bN is None):
        b_network = component_pair_cylindrical.component_bZ.network
        b_station = component_pair_cylindrical.component_bZ.station
    else:
        b_network = component_pair_cylindrical.component_bE.network
        b_station = component_pair_cylindrical.component_bE.station

    filename = ".".join([
        a_network,
        a_station,
        component_pair_cylindrical.component_cylindrical_code_pair[0],
        b_network,
        b_station,
        component_pair_cylindrical.component_cylindrical_code_pair[1],
        year,
        doy_time,
        str(count),
        "npy"
    ])

    return filename


def _crosscorrelate_cylindrical_for_timespan_wrapper(
        inputs: CrosscorrelationCylindricalRunnerInputs,
) -> Tuple[CrosscorrelationCylindrical, ...]:

    return tuple(
        _crosscorrelate_cylindrical_for_timespan(
            timespan=inputs["timespan"],
            crosscorrelation_cylindrical_params=inputs["crosscorrelation_cylindrical_params"],
            grouped_processed_xcorrcartisian=inputs["grouped_processed_xcorrcartisian"],
            component_pairs_cylindrical=inputs["component_pairs_cylindrical"],
        )
    )


def _crosscorrelate_cylindrical_for_timespan(
        timespan: Timespan,
        crosscorrelation_cylindrical_params: CrosscorrelationCylindricalParams,
        grouped_processed_xcorrcartisian,
        component_pairs_cylindrical: Tuple[ComponentPairCylindrical, ...]
) -> List[CrosscorrelationCylindrical]:

    from noiz.globals import PROCESSED_DATA_DIR
    from noiz.processing.path_helpers import assembly_filepath, \
        increment_filename_counter, parent_directory_exists_or_create

    import numpy as np
    logger.info(f"Running crosscorrelation_cylindrical for {timespan}")

    logger.debug(f"Loading data for timespan {timespan}")
    xcorrs_cylindrical = []

    for cp in component_pairs_cylindrical:
        xcorr_cylindrical_all = cylindrical_correlation_computation(cp, grouped_processed_xcorrcartisian, timespan, crosscorrelation_cylindrical_params)
        xcorrs_cylindrical.append(xcorr_cylindrical_all)

    return xcorrs_cylindrical


def _prepare_upsert_command_crosscorrelation_cylindrical(xcorr: CrosscorrelationCylindrical) -> Insert:
    insert_command = (
        insert(CrosscorrelationCylindrical)
        .values(
            crosscorrelation_cylindrical_params_id=xcorr.crosscorrelation_cylindrical_params_id,
            componentpair_cylindrical_id=xcorr.componentpair_cylindrical_id,
            timespan_id=xcorr.timespan_id,
            crosscorrelation_cartesian_1_id=xcorr.crosscorrelation_cartesian_1_id,
            crosscorrelation_cartesian_1_code_pair=xcorr.crosscorrelation_cartesian_1_code_pair,
            crosscorrelation_cartesian_2_id=xcorr.crosscorrelation_cartesian_2_id,
            crosscorrelation_cartesian_2_code_pair=xcorr.crosscorrelation_cartesian_2_code_pair,
            crosscorrelation_cartesian_3_id=xcorr.crosscorrelation_cartesian_3_id,
            crosscorrelation_cartesian_3_code_pair=xcorr.crosscorrelation_cartesian_3_code_pair,
            crosscorrelation_cartesian_4_id=xcorr.crosscorrelation_cartesian_4_id,
            crosscorrelation_cartesian_4_code_pair=xcorr.crosscorrelation_cartesian_4_code_pair,
            crosscorrelation_cylindrical_file_id=xcorr.crosscorrelation_cylindrical_file_id,
            ccf=xcorr.ccf,
        )

        .on_conflict_do_update(
            constraint="unique_ccfcylindrical_per_timespan_cylindrical_per_config",
            set_=dict(ccf=xcorr.ccf),
        )
    )
    return insert_command


def _fetch_cp_cartesian_associated_to_cp_cylindrical(
    component_pairs_cylindrical: List[ComponentPairCylindrical],
) -> List[ComponentPairCartesian]:
    """Fetch the cartesian componentpairs associated to the cylindrical componentpairs to process

    :param component_pairs_cylindrical: List of component_pairs_cylindrical to process
    :type component_pairs_cylindrical: ComponentPairCylindrical
    :return: List of component_pairs_cartesian
    :rtype: List[ComponentPairCartesian]
    """

    component_a_id = tuple(
        [
            cp.component_aE_id
            if cp.component_aE_id is not None else cp.component_aZ_id
            for cp in component_pairs_cylindrical
        ]
    )
    component_b_id = tuple(
        [
            cp.component_bE_id
            if cp.component_bE_id is not None else cp.component_bZ_id
            for cp in component_pairs_cylindrical
        ]
    )

    stationa = tuple([cp.station for cp in fetch_components_by_id(component_a_id)])
    stationb = tuple([cp.station for cp in fetch_components_by_id(component_b_id)])

    component_pairs_cartesian = fetch_componentpairs_cartesian(station_codes_a=stationa, station_codes_b=stationb)
    return list(component_pairs_cartesian)


def cylindrical_correlation_computation(
        component_pair_cylindrical: ComponentPairCylindrical,
        grouped_processed_xcorrcartisian: Dict[FrozenSet[int], CrosscorrelationCartesian],
        timespan: Timespan,
        params: CrosscorrelationCylindricalParams,
):
    """_summary_

    :param component_pair_cylindrical: _description_
    :type component_pair_cylindrical: _type_
    :param grouped_processed_xcorrcartisian: _description_
    :type grouped_processed_xcorrcartisian: _type_
    :param timespan: _description_
    :type timespan: _type_
    :param params: _description_
    :type params: _type_
    :return: _description_
    :rtype: _type_
    """

    code = component_pair_cylindrical.component_cylindrical_code_pair
    back_az = component_pair_cylindrical.backazimuth
    try:
        if (code == "RR") | (code == "TT") | (code == "RT") | (code == "TR"):
            xcorr_aN_bN, xcorr_aE_bE, xcorr_aN_bE, xcorr_aE_bN = _fetch_R_T_xcoor(
                grouped_processed_xcorrcartisian,
                component_pair_cylindrical
            )
            xcorr_cylindrical = _computation_cylindrical_correlation_R_T(
                code,
                xcorr_aN_bN,
                xcorr_aE_bE,
                xcorr_aN_bE,
                xcorr_aE_bN,
                back_az
            )
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            logger.info(f"ccf_file is {str(ccf_file)}")
            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aN_bN.id,
                crosscorrelation_cartesian_1_code_pair="NN",
                crosscorrelation_cartesian_2_id=xcorr_aE_bE.id,
                crosscorrelation_cartesian_2_code_pair="EE",
                crosscorrelation_cartesian_3_id=xcorr_aN_bE.id,
                crosscorrelation_cartesian_3_code_pair="NE",
                crosscorrelation_cartesian_4_id=xcorr_aE_bN.id,
                crosscorrelation_cartesian_4_code_pair="EN",
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )

        elif (code == "RZ") | (code == "TZ"):
            xcorr_aE_bZ, xcorr_aN_bZ = _fetch_RT_Z_xcoor(grouped_processed_xcorrcartisian, component_pair_cylindrical)
            xcorr_cylindrical = _computation_cylindrical_correlation_RT_Z(code, xcorr_aE_bZ, xcorr_aN_bZ, back_az)
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aE_bZ.id,
                crosscorrelation_cartesian_1_code_pair="EZ",
                crosscorrelation_cartesian_2_id=xcorr_aN_bZ.id,
                crosscorrelation_cartesian_2_code_pair="NZ",
                crosscorrelation_cartesian_3_id=None,  # type: ignore
                crosscorrelation_cartesian_3_code_pair=None,  # type: ignore
                crosscorrelation_cartesian_4_id=None,  # type: ignore
                crosscorrelation_cartesian_4_code_pair=None,  # type: ignore
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )

        elif (code == "ZR") | (code == "ZT"):
            xcorr_aZ_bE, xcorr_aZ_bN = _fetch_Z_TR_xcoor(grouped_processed_xcorrcartisian, component_pair_cylindrical)
            xcorr_cylindrical = _computation_cylindrical_correlation_Z_TR(code, xcorr_aZ_bE, xcorr_aZ_bN, back_az)
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aZ_bE.id,
                crosscorrelation_cartesian_1_code_pair="ZE",
                crosscorrelation_cartesian_2_id=xcorr_aZ_bN.id,
                crosscorrelation_cartesian_2_code_pair="ZN",
                crosscorrelation_cartesian_3_id=None,  # type: ignore
                crosscorrelation_cartesian_3_code_pair=None,  # type: ignore
                crosscorrelation_cartesian_4_id=None,  # type: ignore
                crosscorrelation_cartesian_4_code_pair=None,  # type: ignore
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )
        return xcorr
    except Exception:
        logger.error(f"No cartesian correlation for pair {component_pair_cylindrical} ")
        return None


def _prepare_inputs_for_crosscorrelations_cylindrical(
        crosscorrelation_cylindrical_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
        station_codes_a: Optional[Union[Collection[str], str]] = None,
        station_codes_b: Optional[Union[Collection[str], str]] = None,
        batch_size: int = 100,
) -> Generator[CrosscorrelationCylindricalRunnerInputs, None, None]:
    """Performs all the database queries to prepare all the data required for running crosscorrelations_cylindrical.
    Returns a tuple of inputs specific for the further calculations.

    :param crosscorrelation_cylindrical_params_id: ID of CrosscorrelationCylindricalParams object to use
    :type crosscorrelation_cylindrical_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param accepted_component_code_pairs_cylindrical: Code pair accepted for cylindrical componentpair
    :type accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]], optional
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]], optional
    :param station_codes_b:Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]], optional
    :param batch_size: Batch size for which inputs are fetched.
    It changes count of timespans that are pulled from db and prepared for processing at the same time.
    If you have a lot of stations, you might want to reduce that value.
    :type batch_size: int
    :return:
    :rtype: Generator[CrosscorrelationCylindricalRunnerInputs, None, None]
    """

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.info(f"There are {len(fetched_timespans)} timespan to process")
    if accepted_component_code_pairs_cylindrical is not None:
        accepted_component_code_pairs_cylindrical = validate_component_code_pairs(
            component_pairs_cartesian=validate_to_tuple(accepted_component_code_pairs_cylindrical, str)
        )
    component_pairs_cylindrical = fetch_componentpairs_cylindrical(
        station_codes_a=station_codes_a,
        station_codes_b=station_codes_b,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        starttime=starttime,
        endtime=endtime,
    )
    component_pairs_cartesian_ids = extract_object_ids(
        _fetch_cp_cartesian_associated_to_cp_cylindrical(
            component_pairs_cylindrical
        )
    )
    params = fetch_crosscorrelation_cylindrical_params_by_id(id=crosscorrelation_cylindrical_params_id)

    for timespan_batch in more_itertools.chunked(fetched_timespans, batch_size):
        batch_t_tid = extract_object_ids_keep_objects(timespan_batch)
        logger.info(f"batch_t_id length  is {len(batch_t_tid)}")
        fetched_xcorrelation_cartesian = (
            db.session.query(CrosscorrelationCartesian.timespan_id, CrosscorrelationCartesian)
                      .filter(
                          CrosscorrelationCartesian.timespan_id.in_(batch_t_tid.keys()),  # type: ignore
                          CrosscorrelationCartesian.crosscorrelation_cartesian_params_id
                          == params.crosscorrelation_cartesian_params_id,
                          CrosscorrelationCartesian.componentpair_id.in_(component_pairs_cartesian_ids),
            )
            .order_by(CrosscorrelationCartesian.timespan_id)
            .all()
        )

        grouped_xcorr = group_xcrorrcartesian_by_timespanid_componentids(
            processed_xcorrcartesian=fetched_xcorrelation_cartesian
        )

        comp_cart_id = list({
            xc.CrosscorrelationCartesian.componentpair_cartesian.component_a_id for xc in fetched_xcorrelation_cartesian
        }.union({
            xc.CrosscorrelationCartesian.componentpair_cartesian.component_b_id for xc in fetched_xcorrelation_cartesian
        }))

        component_pairs_cylindrical_select = []
        for xc in component_pairs_cylindrical:
            if any(comp.id in comp_cart_id for comp in xc.get_all_components() if comp is not None):
                component_pairs_cylindrical_select.append(xc)

        for timespan_id, grouped_processed_xcorr in grouped_xcorr.items():
            db.session.expunge_all()
            yield CrosscorrelationCylindricalRunnerInputs(
                timespan=batch_t_tid[timespan_id],
                crosscorrelation_cylindrical_params=params,
                grouped_processed_xcorrcartisian=grouped_processed_xcorr,
                component_pairs_cylindrical=tuple(component_pairs_cylindrical_select)
            )
    return


def perform_crosscorrelations_cylindrical(
    crosscorrelation_cylindrical_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    raise_errors: bool = False,
    batch_size: int = 100,
    parallel: bool = True,
) -> None:

    calculation_inputs = _prepare_inputs_for_crosscorrelations_cylindrical(
        crosscorrelation_cylindrical_params_id=crosscorrelation_cylindrical_params_id,
        starttime=starttime,
        endtime=endtime,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        station_codes_a=station_codes_a,
        station_codes_b=station_codes_b,
        batch_size=batch_size,
    )
    logger.info('calculation_inputs ok')
    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_cylindrical_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation_cylindrical,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation_cylindrical,
            raise_errors=raise_errors,
            with_file=True,
        )
    return
