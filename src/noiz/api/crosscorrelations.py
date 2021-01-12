import datetime
from loguru import logger
from obspy.signal.cross_correlation import correlate
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import subqueryload
from typing import Iterable, List, Tuple, Union, Optional, Collection

from noiz.api.component_pair import fetch_componentpairs
from noiz.api.helpers import extract_object_ids, validate_to_tuple
from noiz.api.processing_config import fetch_crosscorrelation_params_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import InconsistentDataException, CorruptedDataException
from noiz.models.component_pair import ComponentPair
from noiz.models.crosscorrelation import Crosscorrelation
from noiz.models.datachunk import Datachunk, ProcessedDatachunk
from noiz.models.timespan import Timespan
from noiz.processing.crosscorrelations import (
    validate_component_code_pairs,
    group_chunks_by_timespanid_componentid,
    load_data_for_chunks,
)


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


def perform_crosscorrelations(
        crosscorrelation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        station_codes: Optional[Union[Collection[str], str]] = None,
        component_code_pairs: Optional[Union[Collection[str], str]] = None,
        autocorrelations: bool = False,
        intrastation_correlations: bool = False,
        bulk_insert: bool = True,
        skip_errors: bool = True
):
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_timespans_ids = extract_object_ids(fetched_timespans)

    if component_code_pairs is not None:
        component_code_pairs = validate_component_code_pairs(
            component_pairs=validate_to_tuple(component_code_pairs, str)
        )

    fetched_component_pairs: List[ComponentPair] = fetch_componentpairs(
        station_codes_a=station_codes,
        accepted_component_code_pairs=component_code_pairs,
    )

    single_component_ids_pre: List[int] = [pair.component_a_id for pair in fetched_component_pairs]
    single_component_ids_pre.extend([pair.component_b_id for pair in fetched_component_pairs])
    single_component_ids: Tuple[int, ...] = tuple(set(single_component_ids_pre))

    params = fetch_crosscorrelation_params_by_id(id=crosscorrelation_params_id)

    fetched_processed_datachunks = (
        db.session.query(Timespan, ProcessedDatachunk)
        .join(Datachunk, Timespan.id == Datachunk.timespan_id)
        .join(ProcessedDatachunk, Datachunk.id == ProcessedDatachunk.datachunk_id)
        .filter(
            ProcessedDatachunk.processed_datachunk_params_id == params.processed_datachunk_params_id,
            Datachunk.component_id.in_(single_component_ids),
            Datachunk.timespan_id.in_(fetched_timespans_ids),
        )
        .options(
            subqueryload(ProcessedDatachunk.datachunk)
        )
        .all())

    grouped_datachunks = group_chunks_by_timespanid_componentid(processed_datachunks=fetched_processed_datachunks)

    xcorrs = []
    for i, (timespan, processed_chunks) in enumerate(grouped_datachunks.items()):

        logger.info("Loading data for {timespan}")
        try:
            streams = load_data_for_chunks(chunks=processed_chunks)
        except CorruptedDataException as e:
            logger.error(e)
            if skip_errors:
                continue
            else:
                raise CorruptedDataException(e)

        for pair in fetched_component_pairs:
            cmp_a_id = pair.component_a_id
            cmp_b_id = pair.component_b_id

            if cmp_a_id not in processed_chunks.keys() and cmp_b_id not in processed_chunks.keys():
                logger.debug(f"No data for pair {pair}")
                continue

            logger.debug(f"Processed chunks for {pair} are present. Starting processing.")

            if streams[cmp_a_id].data.shape != streams[cmp_b_id].data.shape:
                msg = f"The shapes of data arrays for {cmp_a_id} and {cmp_b_id} are different. " \
                      f"Shapes: {cmp_a_id} is {streams[cmp_a_id].data.shape} " \
                      f"{cmp_b_id} is {streams[cmp_b_id].data.shape} "
                logger.error(msg)
                if skip_errors:
                    continue
                else:
                    raise InconsistentDataException(msg)

            ccf_data = correlate(
                a=streams[cmp_a_id],
                b=streams[cmp_b_id],
                shift=params.correlation_max_lag_samples,
            )

            xcorr = Crosscorrelation(
                crosscorrelation_params_id=params.id,
                componentpair_id=pair.id,
                timespan_id=timespan,
                ccf=ccf_data,
            )
            xcorrs.append(xcorr)
        logger.debug(f"Correlations for timespan {timespan} done")

    if bulk_insert:
        logger.info("Trying to do bulk insert")
        try:
            bulk_add_crosscorrelations(xcorrs)
        except IntegrityError as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning("Retrying with upsert")
            upsert_crosscorrelations(xcorrs)
    else:
        logger.info(f"Starting to perform careful upsert. There are {len(xcorrs)} to insert")
        upsert_crosscorrelations(xcorrs)

    logger.info("Success!")
    return
