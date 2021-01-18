import datetime
from loguru import logger

from obspy.signal.cross_correlation import correlate
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import subqueryload
from typing import Iterable, List, Union, Optional, Collection, Dict

from noiz.api.component_pair import fetch_componentpairs
from noiz.api.helpers import extract_object_ids, validate_to_tuple
from noiz.api.processing_config import fetch_crosscorrelation_params_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import InconsistentDataException, CorruptedDataException
from noiz.models.component_pair import ComponentPair
from noiz.models.crosscorrelation import Crosscorrelation
from noiz.models.datachunk import Datachunk, ProcessedDatachunk
from noiz.models.processing_params import CrosscorrelationParams
from noiz.models.timespan import Timespan
from noiz.processing.crosscorrelations import (
    validate_component_code_pairs,
    group_chunks_by_timespanid_componentid,
    load_data_for_chunks, extract_component_ids_from_component_pairs,
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
    i = 0
    for i, xcorr in enumerate(crosscorrelations):
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
        db.session.execute(insert_command)
        logger.debug(f"{i + 1} Upserts done")

    logger.info(f"There were {i} upsert commands prepared.")
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
        raise_errors: bool = False
):
    fetched_component_pairs, grouped_datachunks, params = _prepare_inputs_for_crosscorrelating(
        crosscorrelation_params_id=crosscorrelation_params_id,
        starttime=starttime,
        endtime=endtime,
        station_codes=station_codes,
        component_code_pairs=component_code_pairs
    )

    logger.info("Starting crosscorrelation process.")
    xcorrs = []
    for timespan_id, groupped_processed_chunks in grouped_datachunks.items():
        try:
            xcorr = crosscorrelate_for_timespan(
                timespan_id=timespan_id,
                params=params,
                groupped_processed_chunks=groupped_processed_chunks,
                component_pairs=fetched_component_pairs
            )
        except CorruptedDataException as e:
            if raise_errors:
                logger.error(f"Cought error {e}. Finishing execution.")
                raise CorruptedDataException(e)
            else:
                logger.error(f"Cought error {e}. Skipping to next timespan.")
                continue
        except InconsistentDataException as e:
            if raise_errors:
                logger.error(f"Cought error {e}. Finishing execution.")
                raise InconsistentDataException(e)
            else:
                logger.error(f"Cought error {e}. Skipping to next timespan.")
                continue

        xcorrs.extend(xcorr)
        logger.debug(f"Correlations for timespan_id {timespan_id} done")

    logger.info(f"There were {len(xcorrs)} crosscorrelations performed.")

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


def _prepare_inputs_for_crosscorrelating(crosscorrelation_params_id, starttime, endtime, station_codes,
                                         component_code_pairs):

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_timespans_ids = extract_object_ids(fetched_timespans)
    logger.info(f"There are {len(fetched_timespans_ids)} timespan to process")

    if component_code_pairs is not None:
        component_code_pairs = validate_component_code_pairs(
            component_pairs=validate_to_tuple(component_code_pairs, str)
        )
    fetched_component_pairs: List[ComponentPair] = fetch_componentpairs(
        station_codes_a=station_codes,
        accepted_component_code_pairs=component_code_pairs,
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

    return fetched_component_pairs, grouped_datachunks, params


def crosscorrelate_for_timespan(
        timespan_id: int,
        params: CrosscorrelationParams,
        groupped_processed_chunks: Dict[int, ProcessedDatachunk],
        component_pairs: Collection[ComponentPair]
) -> List[Crosscorrelation]:
    logger.debug(f"Loading data for timespan {timespan_id}")
    try:
        streams = load_data_for_chunks(chunks=groupped_processed_chunks)
    except CorruptedDataException as e:
        logger.error(e)
        raise CorruptedDataException(e)
    xcorrs = []
    for pair in component_pairs:
        cmp_a_id = pair.component_a_id
        cmp_b_id = pair.component_b_id

        if cmp_a_id not in groupped_processed_chunks.keys() or cmp_b_id not in groupped_processed_chunks.keys():
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

        xcorr = Crosscorrelation(
            crosscorrelation_params_id=params.id,
            componentpair_id=pair.id,
            timespan_id=timespan_id,
            ccf=ccf_data,
        )

        xcorrs.append(xcorr)
    return xcorrs
