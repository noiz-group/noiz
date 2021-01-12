from collections import defaultdict

import datetime
from loguru import logger
from noiz.api.component_pair import fetch_componentpairs

from noiz.api.helpers import extract_object_ids, validate_to_tuple
from noiz.api.processing_config import fetch_crosscorrelation_params_by_id

from noiz.api.timespan import fetch_timespans_between_dates

from obspy.signal.cross_correlation import correlate
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import subqueryload
from typing import Iterable, List, Tuple, Union, Optional, Collection

from noiz.database import db
from noiz.models.component import Component
from noiz.models.component_pair import ComponentPair
from noiz.models.crosscorrelation import Crosscorrelation
from noiz.models.datachunk import Datachunk, ProcessedDatachunk
from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan
from noiz.processing.crosscorrelations import (
    validate_component_code_pairs,
    group_componentpairs_by_componenta_componentb,
    group_chunks_by_timespanid_componentid,
    find_correlations_in_chunks,
    load_data_for_chunks,
)
from noiz.processing.time_utils import get_year_doy


def bulk_add_crosscorrelations(crosscorrelations: Iterable[Crosscorrelation]) -> None:
    """
    Tries to perform bulk insert of Crosscorrelation objects.
    Warning: Must be executed within app_context
    :param crosscorrelations: Crosscorrelations to be inserted
    :type crosscorrelations: Iterable[Crosscorrelation]
    :return: None
    :rtype: None
    """
    db.session.add_all(crosscorrelations)
    db.session.commit()
    return


def upsert_crosscorrelations(crosscorrelations: Iterable[Crosscorrelation]) -> None:
    """
    Upserts the Crosscorrelation objects.
    Tries to do insert, in case of conflict it updates existing entry by uploading new ccf timeseries.

    Warning: Must be executed within app_context
    Warning: Uses Postgres specific insert command

    :param crosscorrelations: Crosscorrelations to be inserted
    :type crosscorrelations: Iterable[Crosscorrelation]
    :return: None
    :rtype: None
    """
    logger.info("Starting upserting")
    for i, xcorr in enumerate(crosscorrelations):
        insert_command = (
            insert(Crosscorrelation)
            .values(
                processing_params_id=xcorr.datachunk_processing_config_id,
                componentpair_id=xcorr.componentpair_id,
                timespan_id=xcorr.timespan_id,
                ccf=xcorr.ccf,
            )
            .on_conflict_do_update(
                constraint="unique_ccf_per_timespan_per_componentpair_per_processing",
                set_=dict(ccf=xcorr.ccf),
            )
        )
        db.session.execute(insert_command)
        logger.info(f"{i + 1} Upserts done")
    logger.info("Commiting changes")
    db.session.commit()
    logger.info("Commit done")
    return


def perform_crosscorrelations_for_day_and_pairs(
    execution_date,
    pairs_to_correlate=("ZZ",),
    autocorrelations=False,
    intrastation_correlations=False,
    processing_params_id=1,
    bulk_insert=True,
):

    components_to_correlate = validate_component_code_pairs(pairs_to_correlate)
    year, day_of_year = get_year_doy(execution_date)

    processing_params = (
        db.session.query(DatachunkParams)
        .filter(DatachunkParams.id == processing_params_id)
        .first()
    )

    logger.info(
        f"Querrying for components that are present on day {year}.{day_of_year}"
    )

    components_day = (
        db.session.query(Timespan, Component)
        .join(Datachunk)
        .join(ProcessedDatachunk)
        .join(Component)
        .distinct(Component.id)
        .filter(
            Timespan.starttime_year == year,
            Timespan.starttime_doy == day_of_year,
            Component.component.in_(components_to_correlate),
        )
        .all()
    )

    components_day = [cmp for _, cmp in components_day]
    component_ids = [cmp.id for cmp in components_day]
    logger.info(f"There are {len(component_ids)} unique components")

    component_pairs_day = (
        db.session.query(
            ComponentPair, ComponentPair.component_a_id, ComponentPair.component_b_id
        )
        .options(
            subqueryload(ComponentPair.component_a),
            subqueryload(ComponentPair.component_b),
        )
        .filter(
            and_(
                and_(
                    ComponentPair.component_a_id.in_(component_ids),
                    ComponentPair.component_b_id.in_(component_ids),
                ),
                ComponentPair.autocorrelation == autocorrelations,
                ComponentPair.intracorrelation == intrastation_correlations,
                ComponentPair.component_names.in_(pairs_to_correlate),
            )
        )
    ).all()
    logger.info(
        f"There are {len(component_pairs_day)} component pairs to be correlated that day"
    )

    groupped_componentpairs = group_componentpairs_by_componenta_componentb(
        component_pairs_day
    )

    logger.info("Looking for all processed datachunks for that day")
    processed_datachunks_day = fetch_processeddatachunks_a_day(date=execution_date)
    logger.info(
        f"There are {len(processed_datachunks_day)} processed_datachunks available for {execution_date.date()}"
    )
    groupped_chunks = group_chunks_by_timespanid_componentid(processed_datachunks_day)

    no_timespans = len(groupped_chunks)
    logger.info(
        f"Groupping all_possible correlations. There are {no_timespans} timespans to check."
    )
    xcorrs = []
    for i, (timespan, chunks) in enumerate(groupped_chunks.items()):
        logger.info(f"Starting to look for correlations in {i + 1}/{no_timespans}")
        timespan_corrs = find_correlations_in_chunks(chunks, groupped_componentpairs)
        logger.info("Loading data for that timespan")
        streams = load_data_for_chunks(chunks)

        no_corrs = len(timespan_corrs)
        logger.info(f"Starting correlation of data. There are {no_corrs} to do")

        for cmp_a, components_b in timespan_corrs.items():
            for cmp_b, current_pair in components_b.items():
                if streams[cmp_a][0].data.shape != streams[cmp_b][0].data.shape:
                    logger.error(
                        f"The shapes of data arrays for {cmp_a} and {cmp_b} are different. "
                        f"Shapes: {cmp_a} is {streams[cmp_a][0].data.shape} "
                        f"{cmp_b} is {streams[cmp_b][0].data.shape} "
                    )

                ccf_data = correlate(
                    a=streams[cmp_a][0],
                    b=streams[cmp_b][0],
                    shift=processing_params.get_correlation_max_lag_samples(),
                )

                xcorr = Crosscorrelation(
                    processing_params_id=processing_params.id,
                    componentpair_id=current_pair.id,
                    timespan_id=timespan,
                    ccf=ccf_data,
                )
                xcorrs.append(xcorr)

        logger.info(f"Correlations for timespan {timespan} done")

    if bulk_insert:
        logger.info("Trying to do bulk insert")
        try:
            bulk_add_crosscorrelations(xcorrs)
        except IntegrityError as e:
            logger.warning(f"There was an integrity error thrown. {e}")
            logger.warning("Rollingback session")
            db.session.rollback()
            logger.warning("Retrying with upsert")
            upsert_crosscorrelations(xcorrs)
    else:
        logger.info(
            f"Starting to perform careful upsert. There are {len(xcorrs)} to insert"
        )
        upsert_crosscorrelations(xcorrs)

    logger.info("Success!")
    return

def perform_crosscorrelations(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        station_codes: Optional[Union[Collection[str], str]] = None,
        component_code_pairs: Optional[Union[Collection[str], str]] = None,
        autocorrelations=False,
        intrastation_correlations=False,
        bulk_insert=True,
):
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_timespans_ids = extract_object_ids(fetched_timespans)

    if component_code_pairs is not None:
        component_code_pairs = validate_component_code_pairs(
            component_pairs=validate_to_tuple(component_code_pairs, str)
        )

    fetched_component_pairs: List[ComponentPair] = fetch_componentpairs(
        station_codes_a = station_codes,
        accepted_component_code_pairs=component_code_pairs,
    )

    single_component_ids_pre: List[int] = [pair.component_a_id for pair in fetched_component_pairs]
    single_component_ids_pre.extend([pair.component_b_id for pair in fetched_component_pairs])
    single_component_ids: Tuple[int] = tuple(set(single_component_ids_pre))

    params = fetch_crosscorrelation_params_by_id(id=crosscorrelation_params_id)

    processed_datachunks = (db.session.query(Timespan, ProcessedDatachunk)
                            .join(Datachunk, Timespan.id == Datachunk.timespan_id)
                            .join(ProcessedDatachunk, Datachunk.id == ProcessedDatachunk.datachunk_id)
                            .filter(
        ProcessedDatachunk.processed_datachunk_params_id == params.processed_datachunk_params_id,
        Datachunk.component_id.in_(single_component_ids)
    )
                            .options(
        subqueryload(ProcessedDatachunk.datachunk)
    )
                            .all())

    groupped_datachunks = defaultdict(list)
    for timespan, chunk in processed_datachunks:
        groupped_datachunks[timespan].append(chunk)


    xcorrs = []
    for i, (timespan, chunks) in enumerate(groupped_datachunks.items()):
        logger.info(f"Starting to look for correlations in {i + 1}/{no_timespans}")
        timespan_corrs = find_correlations_in_chunks(chunks, groupped_componentpairs)
        logger.info("Loading data for that timespan")
        streams = load_data_for_chunks(chunks)

        no_corrs = len(timespan_corrs)
        logger.info(f"Starting correlation of data. There are {no_corrs} to do")

        for cmp_a, components_b in timespan_corrs.items():
            for cmp_b, current_pair in components_b.items():
                if streams[cmp_a][0].data.shape != streams[cmp_b][0].data.shape:
                    logger.error(
                        f"The shapes of data arrays for {cmp_a} and {cmp_b} are different. "
                        f"Shapes: {cmp_a} is {streams[cmp_a][0].data.shape} "
                        f"{cmp_b} is {streams[cmp_b][0].data.shape} "
                    )

                ccf_data = correlate(
                    a=streams[cmp_a][0],
                    b=streams[cmp_b][0],
                    shift=params.get_correlation_max_lag_samples(),
                )

                xcorr = Crosscorrelation(
                    processing_params_id=params.id,
                    componentpair_id=current_pair.id,
                    timespan_id=timespan,
                    ccf=ccf_data,
                )
                xcorrs.append(xcorr)

        logger.info(f"Correlations for timespan {timespan} done")

    if bulk_insert:
        logger.info("Trying to do bulk insert")
        try:
            bulk_add_crosscorrelations(xcorrs)
        except IntegrityError as e:
            logger.warning(f"There was an integrity error thrown. {e}")
            logger.warning("Rollingback session")
            db.session.rollback()
            logger.warning("Retrying with upsert")
            upsert_crosscorrelations(xcorrs)
    else:
        logger.info(
            f"Starting to perform careful upsert. There are {len(xcorrs)} to insert"
        )
        upsert_crosscorrelations(xcorrs)

    logger.info("Success!")
    return


def fetch_processeddatachunks_a_day(
    date: datetime.datetime,
) -> List[Tuple[ProcessedDatachunk, int, int]]:

    year, day_of_year = get_year_doy(date)
    processed_datachunks_day: List[Tuple[ProcessedDatachunk, int, int]] = (
        db.session.query(ProcessedDatachunk, Component.id, Timespan.id)
        .join(Datachunk)
        .join(Timespan)
        .join(Component)
        .options(
            subqueryload(ProcessedDatachunk.datachunk).subqueryload(Datachunk.component)
        )
        .filter(Timespan.starttime_year == year, Timespan.starttime_doy == day_of_year)
        .order_by(Timespan.id)
        .all()
    )
    return processed_datachunks_day
