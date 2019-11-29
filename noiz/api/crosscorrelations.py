import datetime
import logging
from typing import Iterable, List, Tuple

from obspy.signal.cross_correlation import correlate
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import subqueryload

from noiz.database import db
from noiz.models import (
    Crosscorrelation,
    ProcessingParams,
    Timespan,
    Component,
    DataChunk,
    ProcessedDatachunk,
    ComponentPair,
)
from noiz.processing.crosscorrelations import (
    split_component_pairs_to_components,
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
    logging.info("Starting upserting")
    for i, xcorr in enumerate(crosscorrelations):
        insert_command = (
            insert(Crosscorrelation)
            .values(
                processing_params_id=xcorr.processing_params_id,
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
        logging.info(f"{i+1} Upserts done")
    logging.info("Commiting changes")
    db.session.commit()
    logging.info("Commit done")
    return


def perform_crosscorrelations_for_day_and_pairs(
    execution_date,
    pairs_to_correlate=("ZZ",),
    autocorrelations=False,
    intrastation_correlations=False,
    processing_params_id=1,
    bulk_insert=True,
):

    components_to_correlate = split_component_pairs_to_components(pairs_to_correlate)
    year, day_of_year = get_year_doy(execution_date)

    processing_params = (
        db.session.query(ProcessingParams)
        .filter(ProcessingParams.id == processing_params_id)
        .first()
    )

    logging.info(
        f"Querrying for components that are present on day {year}.{day_of_year}"
    )

    components_day = (
        db.session.query(Timespan, Component)
        .join(DataChunk)
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
    logging.info(f"There are {len(component_ids)} unique components")

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
    logging.info(
        f"There are {len(component_pairs_day)} component pairs to be correlated that day"
    )

    groupped_componentpairs = group_componentpairs_by_componenta_componentb(
        component_pairs_day
    )

    logging.info(f"Looking for all processed datachunks for that day")
    processed_datachunks_day = fetch_processeddatachunks_a_day(date=execution_date)
    logging.info(
        f"There are {len(processed_datachunks_day)} processed_datachunks available for {execution_date.date()}"
    )
    groupped_chunks = group_chunks_by_timespanid_componentid(processed_datachunks_day)

    no_timespans = len(groupped_chunks)
    logging.info(
        f"Groupping all_possible correlations. There are {no_timespans} timespans to check."
    )
    xcorrs = []
    for i, (timespan, chunks) in enumerate(groupped_chunks.items()):
        logging.info(f"Starting to look for correlations in {i + 1}/{no_timespans}")
        timespan_corrs = find_correlations_in_chunks(chunks, groupped_componentpairs)
        logging.info(f"Loading data for that timespan")
        streams = load_data_for_chunks(chunks)

        no_corrs = len(timespan_corrs)
        logging.info(f"Starting correlation of data. There are {no_corrs} to do")

        for cmp_a, components_b in timespan_corrs.items():
            for cmp_b, current_pair in components_b.items():
                if streams[cmp_a][0].data.shape != streams[cmp_b][0].data.shape:
                    logging.error(
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

        logging.info(f"Correlations for timespan {timespan} done")

    if bulk_insert:
        logging.info("Trying to do bulk insert")
        try:
            bulk_add_crosscorrelations(xcorrs)
        except IntegrityError as e:
            logging.warning(f"There was an integrity error thrown. {e}")
            logging.warning("Rollingback session")
            db.session.rollback()
            logging.warning(f"Retrying with upsert")
            upsert_crosscorrelations(xcorrs)
    else:
        logging.info(
            f"Starting to perform careful upsert. There are {len(xcorrs)} to insert"
        )
        upsert_crosscorrelations(xcorrs)

    logging.info(f"Success!")
    return


def fetch_processeddatachunks_a_day(
    date: datetime.datetime,
) -> List[Tuple[ProcessedDatachunk, int, int]]:

    year, day_of_year = get_year_doy(date)
    processed_datachunks_day: List[Tuple[ProcessedDatachunk, int, int]] = (
        db.session.query(ProcessedDatachunk, Component.id, Timespan.id)
        .join(DataChunk)
        .join(Timespan)
        .join(Component)
        .options(
            subqueryload(ProcessedDatachunk.datachunk).subqueryload(DataChunk.component)
        )
        .filter(Timespan.starttime_year == year, Timespan.starttime_doy == day_of_year)
        .order_by(Timespan.id)
        .all()
    )
    return processed_datachunks_day
