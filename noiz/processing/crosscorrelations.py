import numpy as np
import datetime
import logging

from sqlalchemy.orm import subqueryload
from sqlalchemy import and_

from collections import defaultdict
from typing import Tuple, Iterable, Dict
from obspy.signal.cross_correlation import correlate
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from noiz.models import (
    DataChunk,
    ProcessedDatachunk,
    ProcessingParams,
    Component,
    Timespan,
    ComponentPair,
    Crosscorrelation,
)
from noiz.database import db
from noiz.processing.time_utils import get_year_doy


def get_time_vector_ccf(max_lag: float, sampling_rate: float) -> np.array:
    step = 1 / sampling_rate
    start = -max_lag
    stop = max_lag + step

    return np.arange(start=start, stop=stop, step=step)


def group_chunks_by_timespanid_componentid(
    processed_datachunks: Iterable[Tuple[ProcessedDatachunk, int, int]]
) -> Dict[int, Dict[int, ProcessedDatachunk]]:
    groupped_chunks = defaultdict(dict)
    for chunk, component_id, timespan_id in processed_datachunks:
        groupped_chunks[timespan_id][component_id] = chunk
    return groupped_chunks


def group_componentpairs_by_componenta_componentb(
    component_pairs: Iterable[Tuple[ComponentPair, int, int]]
) -> Dict[int, Dict[int, ComponentPair]]:
    groupped_componentpairs = defaultdict(dict)
    for component_pair, component_a_id, component_b_id in component_pairs:
        groupped_componentpairs[component_a_id][component_b_id] = component_pair
    return groupped_componentpairs


def fetch_processeddatachunks_a_day(
    date: datetime.date
) -> Iterable[Tuple[ProcessedDatachunk, int, int]]:
    year, day_of_year = get_year_doy(date)
    processed_datachunks_day = (
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


def find_correlations_in_chunks(chunks, groupped_componentpairs):
    local_corrs = defaultdict(dict)
    for component_a_id, chunk_a in chunks.items():
        if groupped_componentpairs.get(component_a_id) is None:
            #             logging.debug(f'Component {component_a_id} was not found on position `a` in channelpairs')
            continue

        for component_b_id, chunk_b in chunks.items():
            found_pair = groupped_componentpairs.get(component_a_id).get(component_b_id)

            if found_pair is None:
                #                 logging.debug(f'Pair {component_a_id}-{component_b_id} does not exist')
                continue
            else:
                #                 logging.debug(f'Pair {component_a_id}-{component_b_id} found. Adding to joblist')
                if (
                    found_pair.component_a == chunk_a.datachunk.component
                    and found_pair.component_b == chunk_b.datachunk.component
                ):
                    local_corrs[component_a_id][component_b_id] = found_pair

                else:
                    raise ValueError(
                        f"The found correlation has wrong relations with chunks! \
                                     Was expecting on `a`: {found_pair.component_a} got {chunk_a.datachunk.component};\
                                     on `b`: {found_pair.component_b} got {chunk_b.datachunk.component}"
                    )
    return local_corrs


def load_all_chunks(chunks):
    streams = {}
    for cmp_id, proc_chunk in chunks.items():
        streams[cmp_id] = proc_chunk.load_data()
    return streams


def split_component_pairs_to_components(component_pairs):
    total = []
    for x in component_pairs:
        total.extend(list(x))

    return tuple(set(total))


def bulk_add_crosscorrelations(crosscorrelations):
    db.session.add_all(crosscorrelations)
    db.session.commit()
    return


def upsert_crosscorrelations(crosscorrelations):
    no_xcorrs = len(crosscorrelations)
    logging.info(f"There are {no_xcorrs} correlations to be upserted")
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
        logging.info(f"Upsert {i+1}/{no_xcorrs} done")
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
        streams = load_all_chunks(chunks)

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
