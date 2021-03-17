from collections import defaultdict

from loguru import logger
from sqlalchemy.orm import subqueryload
from typing import Union, Collection, Optional, List, Tuple, Generator
from sqlalchemy.dialects.postgresql import insert, Insert

import datetime

from noiz.api.component import fetch_components
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_sequentially, \
    _run_calculate_and_upsert_on_dask
from noiz.api.qc import fetch_qcone_config_single
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.api.type_aliases import BeamformingRunnerInputs
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Timespan, Datachunk, QCOneResults
from noiz.models.beamforming import BeamformingResult
from noiz.models.processing_params import BeamformingParams
from noiz.processing.beamforming import calculate_beamforming_results_wrapper


def fetch_beamforming_params_by_id(id: int) -> BeamformingParams:
    """
    Fetches a BeamformingParams objects by its ID.

    :param id: ID of beamforming params to be fetched
    :type id: int
    :return: fetched BeamformingParams object
    :rtype: Optional[BeamformingParams]
    """
    fetched_params = BeamformingParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"BeamformingParams object of id {id} does not exist.")

    return fetched_params


def run_beamforming(
        beamforming_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        skip_existing: bool = True,
        batch_size: int = 2500,
        parallel: bool = True,
        raise_errors: bool = True,
):
    calculation_inputs = _prepare_inputs_for_beamforming_runner(
        beamforming_params_id=beamforming_params_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
        skip_existing=skip_existing
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_beamforming_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_beamforming,
            with_file=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_beamforming_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_beamforming,
            raise_errors=raise_errors,
            with_file=True,
        )
    return


def _prepare_inputs_for_beamforming_runner(
        beamforming_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        skip_existing: bool = True,
) -> Generator[BeamformingRunnerInputs, None, None]:

    logger.debug(f"Fetching BeamformingParams with id {beamforming_params_id}")
    params = fetch_beamforming_params_by_id(beamforming_params_id)
    logger.debug(f"Fetching ProcessedDatachunkParams successful. {params}")

    logger.debug(f"Fetching QCOneConfig with id {params.qcone_config_id}")
    qcone_config = fetch_qcone_config_single(params.qcone_config_id)
    logger.debug(f"Fetching QCOneConfig successful. {qcone_config}")

    logger.debug(f"Fetching timespans for {starttime} - {endtime}")
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.debug(f"Fetched {len(fetched_timespans)} timespans")

    logger.debug("Fetching components")
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )
    logger.debug(f"Fetched {len(fetched_components)} components")

    selection = (
        db.session.query(Timespan, Datachunk, QCOneResults)
                  .select_from(Timespan)
                  .join(Datachunk, Datachunk.timespan_id == Timespan.id)
                  .join(QCOneResults, Datachunk.id == QCOneResults.datachunk_id)
                  .filter(Timespan.id.in_(extract_object_ids(fetched_timespans)))  # type: ignore
                  .filter(Datachunk.component_id.in_(extract_object_ids(fetched_components)))
                  .filter(QCOneResults.qcone_config_id == qcone_config.id)
                  .options(subqueryload(Datachunk.component))
                  .all()
    )

    grouped_by_tid = defaultdict(list)

    for sel in selection:
        grouped_by_tid[sel[0]].append(sel[1:])

    if skip_existing:
        raise NotImplementedError("Not yet implemented")

    for ts, group in grouped_by_tid.items():
        group: List[Tuple[Datachunk, QCOneResults]]  # type: ignore
        passing_chunks = [chunk for chunk, qcres in group if qcres.is_passing()]

        db.session.expunge_all()
        yield BeamformingRunnerInputs(
            beamforming_params=params,
            timespan=ts,
            datachunks=tuple(passing_chunks),
        )


def _prepare_upsert_command_beamforming(results: BeamformingResult) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.Insert` for
    :py:class:`~noiz.models.beamforming.BeamformingResult` to be upserted to db.
    Postgres specific because it's upsert.

    :param results: Instance which is to be upserted
    :type results: noiz.models.beamforming.BeamformingResult
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.Insert
    """
    insert_command = (
        insert(BeamformingResult)
        .values(
            beamforming_params_id=results.beamforming_params_id,
            timespan_id=results.timespan_id,
            mean_relative_relpow=results.mean_relative_relpow,
            std_relative_relpow=results.std_relative_relpow,
            mean_absolute_relpow=results.mean_absolute_relpow,
            std_absolute_relpow=results.std_absolute_relpow,
            mean_backazimuth=results.mean_backazimuth,
            std_backazimuth=results.std_backazimuth,
            mean_slowness=results.mean_slowness,
            std_slowness=results.std_slowness,
            used_component_count=results.used_component_count,
        )
        .on_conflict_do_update(
            constraint="unique_beam_per_config_per_timespan",
            set_=dict(
                mean_relative_relpow=results.mean_relative_relpow,
                std_relative_relpow=results.std_relative_relpow,
                mean_absolute_relpow=results.mean_absolute_relpow,
                std_absolute_relpow=results.std_absolute_relpow,
                mean_backazimuth=results.mean_backazimuth,
                std_backazimuth=results.std_backazimuth,
                mean_slowness=results.mean_slowness,
                std_slowness=results.std_slowness,
                used_component_count=results.used_component_count,
            ),
        )
    )
    return insert_command
