# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import itertools
import os
from dataclasses import dataclass
from loguru import logger
import more_itertools
from pathlib import Path
from sqlalchemy.sql import Insert
from sqlalchemy.orm import joinedload
from sqlalchemy import func

from noiz.api.helpers import (
    _run_calculate_and_upsert_on_dask,
    _run_calculate_and_upsert_sequentially,
    bulk_add_or_upsert_objects,
    create_process_dask_client,
)
from noiz.models.type_aliases import StackingInputs, StackingParallelInputs, StackingCylindricalInputs
from noiz.exceptions import MissingProcessingStepError
from noiz.validation_helpers import numpy_to_python
from obspy import UTCDateTime
from sqlalchemy.dialects.postgresql import insert
from typing import Collection, Union, List, Optional, Tuple, Generator, Dict, Iterable

from noiz.api.component_pair import (
    fetch_componentpairs_cartesian,
    fetch_componentpairs_cartesian_by_id,
    fetch_componentpairs_cylindrical,
)
from noiz.api.qc import fetch_qctwo_config_single, count_qctwo_results
from noiz.database import db
from noiz.models import (
    CrosscorrelationCartesian,
    CrosscorrelationCylindrical,
    StackingTimespan,
    Timespan,
    CCFStack,
    CCFStackCylindrical,
    QCTwoResults,
    ComponentPairCartesian,
    ComponentPairCylindrical,
    StackingSchema,
    Component,
)
from noiz.api.processing_config import fetch_stacking_schema_by_id
from noiz.processing.stacking import (
    _generate_stacking_timespans,
    do_linear_stack_of_crosscorrelations_cartesian,
    do_linear_stack_of_crosscorrelations_cylindrical,
)


_STACKING_WORKER_APP = None


@dataclass
class StackingMemoryEstimation:
    recommended_workers: int
    default_workers: int
    max_ccfs_per_stack: int
    ccf_samples: int
    estimated_memory_per_task_bytes: float
    usable_ram_bytes: float


def fetch_stacking_timespans(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime, UTCDateTime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime, UTCDateTime]] = None,
) -> List[StackingTimespan]:
    if starttime is not None:
        if isinstance(starttime, UTCDateTime):
            starttime = starttime.datetime
        elif not isinstance(starttime, (datetime.date, datetime.datetime)):
            raise ValueError(
                f"And starttime was expecting either "
                f"datetime.date, datetime.datetime or UTCDateTime objects."
                f"Got instance of {type(starttime)}"
            )

    if endtime is not None:
        if isinstance(endtime, UTCDateTime):
            endtime = endtime.datetime
        elif not isinstance(endtime, (datetime.date, datetime.datetime)):
            raise ValueError(
                f"And endtime was expecting either "
                f"datetime.date, datetime.datetime or UTCDateTime objects."
                f"Got instance of {type(endtime)}"
            )

    filters = []

    filters.append(StackingTimespan.stacking_schema_id == stacking_schema_id)
    if starttime is not None:
        filters.append(StackingTimespan.starttime >= starttime)
    if endtime is not None:
        filters.append(StackingTimespan.endtime <= endtime)

    return StackingTimespan.query.filter(*filters).all()


def create_stacking_timespans_add_to_db(
    stacking_schema_id: int,
    bulk_insert: bool = True,
) -> None:
    """
    Fetches a :py:class:`~noiz.models.stacking.StackingSchema` with provided
    :paramref:`noiz.api.stacking.create_stacking_timespans_add_to_db.stacking_schema_id` and based on it, creates
    all possible StackingTimespans.
    After creation, it tries to simply add all of them to database, in case of failure, an Upsert operation is
    attempted.

    :param stacking_schema_id: Id of existing StackingSchema object.
    :type stacking_schema_id: int
    :param bulk_insert: If a bulk insert should be attempted
    :type bulk_insert: bool
    :return:
    :rtype:
    """
    logger.info(f"Fetching stacking schema with id {stacking_schema_id}")
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    logger.info("Stacking schema fetched")

    logger.info("Generating StackingTimespan objects.")
    stacking_timespans = list(_generate_stacking_timespans(stacking_schema=stacking_schema))
    logger.info(f"There were {len(stacking_timespans)} StackingTimespan generated.")

    logger.info("Inserting/Upserting them to database.")
    _insert_upsert_stacking_timespans_into_db(timespans=stacking_timespans, bulk_insert=bulk_insert)
    logger.info("All objects successfully added to database.")
    return


def _insert_upsert_stacking_timespans_into_db(
    timespans: Collection[StackingTimespan],
    bulk_insert: bool = True,
) -> None:
    """
    Inserts a collection of  :py:class:`~noiz.models.stacking.StackingSchema` objects to database.
    By default it attempts to add all of the objects in the bulk insert action.
    If the bulk_insert param is false, it tries to upsert all the objects one by one.

    :param timespans: StackingTimespans to be added to db
    :type timespans: Collection[StackingTimespan]
    :param bulk_insert: If the bullk insert should be attempted.
    :type bulk_insert: bool
    :return: None
    :rtype: NoneType
    """
    # FIXME Make bulk_insert path try to perform it but then in case of exception perform upsert. noiz#176
    if bulk_insert:
        db.session.bulk_save_objects(timespans)
        db.session.commit()
    else:
        con = db.session.connection()
        for ts in timespans:
            update_dict = {
                "starttime": ts.starttime,
                "midtime": ts.midtime,
                "endtime": ts.endtime,
                "stacking_schema_id": ts.stacking_schema_id,
            }
            insert_command = (
                insert(StackingTimespan)
                .values(
                    starttime=ts.starttime,
                    midtime=ts.midtime,
                    endtime=ts.endtime,
                    stacking_schema_id=ts.stacking_schema_id,
                )
                .on_conflict_do_update(constraint="unique_stack_starttime", set_=update_dict)
                .on_conflict_do_update(constraint="unique_stack_midtime", set_=update_dict)
                .on_conflict_do_update(constraint="unique_stack_endtime", set_=update_dict)
                .on_conflict_do_update(constraint="unique_stack_times", set_=update_dict)
            )
            con.execute(insert_command)


def stack_crosscorrelation_cartesian(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
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
    batch_size: Optional[int] = None,
    parallel: bool = True,
    ram_safety_factor: float = 0.5,
) -> None:
    total_inputs = None
    recommended_n_workers = None
    if parallel and batch_size is None:
        total_inputs = _count_inputs_for_stacking_ccfs(
            stacking_schema_id=stacking_schema_id,
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
        memory_estimation = _estimate_stacking_parallel_resources(
            stacking_schema_id=stacking_schema_id,
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
            ram_safety_factor=ram_safety_factor,
        )
        recommended_n_workers = memory_estimation.recommended_workers
        logger.info(
            f"Stacking RAM policy selected {recommended_n_workers} Dask worker(s) "
            f"(default would be {memory_estimation.default_workers})."
        )

    if parallel:
        parallel_calculation_inputs = _prepare_inputs_for_stacking_ccfs_parallel(
            stacking_schema_id=stacking_schema_id,
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
        _run_stacking_on_dask(
            batch_size=batch_size,
            total_inputs=total_inputs,
            inputs=parallel_calculation_inputs,
            upserter_callable=_generate_ccfstack_upsert_command,
            raise_errors=raise_errors,
            n_workers=recommended_n_workers,
        )
    else:
        sequential_calculation_inputs = _prepare_inputs_for_stacking_ccfs(
            stacking_schema_id=stacking_schema_id,
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
        effective_batch_size = 1000 if batch_size is None else batch_size
        _run_calculate_and_upsert_sequentially(
            batch_size=effective_batch_size,
            inputs=sequential_calculation_inputs,
            calculation_task=_validate_and_stack_ccfs_wrapper,  # type: ignore
            upserter_callable=_generate_ccfstack_upsert_command,
            raise_errors=raise_errors,
        )

    return


def _prepare_stacking_parallel_task_inputs(
    input_batch: Tuple[StackingParallelInputs, ...],
    per_task_chunk_size: int,
) -> List[Tuple[StackingParallelInputs, ...]]:
    return [tuple(chunk) for chunk in more_itertools.chunked(input_batch, per_task_chunk_size)]


def _run_stacking_on_dask(
    inputs: Iterable[StackingParallelInputs],
    upserter_callable,
    batch_size: Optional[int] = None,
    total_inputs: Optional[int] = None,
    raise_errors: bool = False,
    n_workers: Optional[int] = None,
) -> None:
    from dask.distributed import wait

    client, cluster, created_worker_count = create_process_dask_client(n_workers=n_workers)
    scheduler_worker_count = len(client.scheduler_info().get("workers", {}))
    effective_worker_count = max(1, scheduler_worker_count, len(getattr(cluster, "workers", {})), created_worker_count)

    if batch_size is None:
        if total_inputs is not None:
            resolved_total_inputs = total_inputs
        else:
            materialized_inputs = tuple(inputs)
            inputs = materialized_inputs
            resolved_total_inputs = len(materialized_inputs)

        batch_size = max(1, resolved_total_inputs // effective_worker_count)
        logger.info(
            f"Batch size was not provided. Using floor(total_inputs / n_workers) = "
            f"floor({resolved_total_inputs} / {effective_worker_count}) = {batch_size}."
        )

    logger.info(f"Processing will be executed in batches. The chunks size is {batch_size}")

    total_processed = 0

    for batch_num, input_batch_chunk in enumerate(more_itertools.chunked(inputs, batch_size)):
        input_batch = tuple(input_batch_chunk)
        if not input_batch:
            continue

        per_task_chunk_size = max(1, len(input_batch) // effective_worker_count)
        task_inputs = _prepare_stacking_parallel_task_inputs(
            input_batch=input_batch,
            per_task_chunk_size=per_task_chunk_size,
        )

        logger.info(f"Starting processing of chunk no.{batch_num}")
        logger.info(
            f"Submitting {len(input_batch)} stacking inputs as {len(task_inputs)} Dask tasks "
            f"(~{per_task_chunk_size} stack computations per task, effective_workers={effective_worker_count})"
        )

        futures = [
            client.submit(_validate_and_stack_ccfs_by_id_batch_wrapper, task_input) for task_input in task_inputs
        ]
        wait(futures)

        finished_futures = [future for future in futures if future.status == "finished"]
        non_finished_futures = [future for future in futures if future.status != "finished"]

        if non_finished_futures:
            statuses = [future.status for future in non_finished_futures]
            for future in non_finished_futures:
                try:
                    logger.error(f"Stacking task {future.key} ended with status {future.status}: {future.exception()}")
                except Exception as exception_error:
                    logger.error(
                        f"Stacking task {future.key} ended with status {future.status} and exception could not be read: "
                        f"{exception_error}"
                    )
            if raise_errors or non_finished_futures:
                raise RuntimeError(f"Dask returned cancelled or failed stacking tasks. Statuses: {statuses}")

        results_nested = client.gather(finished_futures)
        results = [result for result in more_itertools.flatten(results_nested) if result is not None]

        logger.info(f"Batch {batch_num}: Upserting {len(results)} stacking results...")
        if results:
            bulk_add_or_upsert_objects(
                objects_to_add=results,
                upserter_callable=upserter_callable,
                bulk_insert=True,
            )
        total_processed += len(results)
        logger.info(f"Batch {batch_num}: Upsert completed. Total processed: {total_processed}")

    logger.info("Shutting down Dask cluster (this may take a few seconds)...")
    client.close(timeout=30)
    cluster.close(timeout=30)
    logger.info("Dask cluster shut down.")


def _count_inputs_for_stacking_ccfs(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
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
) -> int:
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    qctwo_config = fetch_qctwo_config_single(id=stacking_schema.qctwo_config_id)
    componentpairs_cartesian = fetch_componentpairs_cartesian(
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

    componentpair_ids = [componentpair.id for componentpair in componentpairs_cartesian]
    if not componentpair_ids:
        return 0

    total_inputs = 0
    for stacking_timespan in stacking_timespans:
        total_inputs += len(
            _fetch_existing_componentpair_ids_for_stacking_timespan(
                qctwo_config_id=qctwo_config.id,
                componentpair_ids=componentpair_ids,
                stacking_timespan=stacking_timespan,
            )
        )

    logger.info(f"Precomputed {total_inputs} stacking input(s) for automatic batch sizing.")
    return total_inputs


def _estimate_recommended_stacking_workers(
    max_ccfs_per_stack: int,
    ccf_samples: int,
    available_ram_bytes: int,
    ram_safety_factor: float,
    default_worker_count: int,
) -> StackingMemoryEstimation:
    bytes_per_float = 8
    base_overhead = 500 * 1024**2
    per_task_python_overhead = 64 * 1024**2
    per_ccf_object_overhead = 4096

    usable_ram = max(100 * 1024**2, (available_ram_bytes - base_overhead) * ram_safety_factor)
    estimated_memory_per_task = (
        (max_ccfs_per_stack * (ccf_samples * bytes_per_float + per_ccf_object_overhead))
        + (ccf_samples * bytes_per_float)
        + per_task_python_overhead
    )

    recommended_workers = max(1, min(default_worker_count, int(usable_ram // estimated_memory_per_task)))

    return StackingMemoryEstimation(
        recommended_workers=recommended_workers,
        default_workers=default_worker_count,
        max_ccfs_per_stack=max_ccfs_per_stack,
        ccf_samples=ccf_samples,
        estimated_memory_per_task_bytes=estimated_memory_per_task,
        usable_ram_bytes=usable_ram,
    )


def _estimate_stacking_parallel_resources(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
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
    ram_safety_factor: float = 0.5,
) -> StackingMemoryEstimation:
    import psutil

    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    componentpairs_cartesian = fetch_componentpairs_cartesian(
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
    componentpair_ids = [componentpair.id for componentpair in componentpairs_cartesian]

    default_worker_count = max(1, (os.cpu_count() or 4) - 2)
    if not stacking_timespans or not componentpair_ids:
        return StackingMemoryEstimation(
            recommended_workers=1,
            default_workers=default_worker_count,
            max_ccfs_per_stack=0,
            ccf_samples=0,
            estimated_memory_per_task_bytes=0.0,
            usable_ram_bytes=0.0,
        )

    max_ccfs_per_stack = 0
    for stacking_timespan in stacking_timespans:
        max_for_timespan = (
            db.session.query(func.count(CrosscorrelationCartesian.id))
            .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
            .join(Timespan, CrosscorrelationCartesian.timespan_id == Timespan.id)
            .filter(
                QCTwoResults.qctwo_config_id == stacking_schema.qctwo_config_id,
                CrosscorrelationCartesian.componentpair_id.in_(componentpair_ids),
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
            .group_by(CrosscorrelationCartesian.componentpair_id)
            .order_by(func.count(CrosscorrelationCartesian.id).desc())
            .limit(1)
            .scalar()
        )
        if max_for_timespan is not None:
            max_ccfs_per_stack = max(max_ccfs_per_stack, int(max_for_timespan))

    ccf_samples = 2 * stacking_schema.crosscorrelation_cartesian_params.correlation_max_lag_samples + 1
    memory = psutil.virtual_memory()
    estimation = _estimate_recommended_stacking_workers(
        max_ccfs_per_stack=max(1, max_ccfs_per_stack),
        ccf_samples=ccf_samples,
        available_ram_bytes=memory.available,
        ram_safety_factor=ram_safety_factor,
        default_worker_count=default_worker_count,
    )
    logger.info(
        "Stacking RAM estimation:\n"
        f"  - Available RAM: {memory.available / 1024**3:.2f}GB\n"
        f"  - Usable RAM: {estimation.usable_ram_bytes / 1024**3:.2f}GB (safety factor: {ram_safety_factor})\n"
        f"  - Worst-case CCFs per stack task: {estimation.max_ccfs_per_stack}\n"
        f"  - Samples per CCF: {estimation.ccf_samples}\n"
        f"  - Estimated memory per stack task: {estimation.estimated_memory_per_task_bytes / 1024**2:.1f}MB\n"
        f"  - Recommended workers: {estimation.recommended_workers} / {estimation.default_workers}"
    )
    return estimation


def _fetch_existing_componentpair_ids_for_stacking_timespan(
    qctwo_config_id: int,
    componentpair_ids: Collection[int],
    stacking_timespan: StackingTimespan,
) -> Tuple[int, ...]:
    return tuple(
        row[0]
        for row in (
            db.session.query(CrosscorrelationCartesian.componentpair_id)
            .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
            .join(Timespan, CrosscorrelationCartesian.timespan_id == Timespan.id)
            .filter(
                QCTwoResults.qctwo_config_id == qctwo_config_id,
                CrosscorrelationCartesian.componentpair_id.in_(componentpair_ids),
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
            .distinct()
            .order_by(CrosscorrelationCartesian.componentpair_id)
            .all()
        )
    )


def _prepare_inputs_for_stacking_ccfs_parallel(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
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
) -> Generator[StackingParallelInputs, None, None]:
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    logger.info(f"There are {len(stacking_timespans)} timespans to stack for")
    qctwo_config = fetch_qctwo_config_single(id=stacking_schema.qctwo_config_id)
    componentpairs_cartesian = fetch_componentpairs_cartesian(
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
    logger.info(f"There are {len(componentpairs_cartesian)} componentpairs_cartesian to stack for")

    if count_qctwo_results(qctwo_config=qctwo_config) == 0:
        raise MissingProcessingStepError(
            "There are no QCTwo results for that QCTwoConfig. Are you sure you ran QCTwo before?"
        )

    componentpair_ids = [componentpair.id for componentpair in componentpairs_cartesian]
    for stacking_timespan in stacking_timespans:
        existing_pair_ids = _fetch_existing_componentpair_ids_for_stacking_timespan(
            qctwo_config_id=qctwo_config.id,
            componentpair_ids=componentpair_ids,
            stacking_timespan=stacking_timespan,
        )

        for componentpair_id in existing_pair_ids:
            db.session.expunge_all()
            yield StackingParallelInputs(
                qctwo_config_id=qctwo_config.id,
                componentpair_cartesian_id=componentpair_id,
                stacking_schema_id=stacking_schema.id,
                stacking_timespan_id=stacking_timespan.id,
            )


def _prepare_inputs_for_stacking_ccfs(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
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
) -> Generator[StackingInputs, None, None]:
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    no_timespans = len(stacking_timespans)
    logger.info(f"There are {no_timespans} timespans to stack for")
    qctwo_config = fetch_qctwo_config_single(id=stacking_schema.qctwo_config_id)
    componentpairs_cartesian = fetch_componentpairs_cartesian(
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
    logger.info(f"There are {len(componentpairs_cartesian)} componentpairs_cartesian to stack for")

    if count_qctwo_results(qctwo_config=qctwo_config) == 0:
        raise MissingProcessingStepError(
            "There are no QCTwo results for that QCTwoConfig. Are you sure you ran QCTwo before?"
        )

    for stacking_timespan, componentpair_cartesian in itertools.product(stacking_timespans, componentpairs_cartesian):
        fetched_qc_ccfs = (
            db.session.query(QCTwoResults, CrosscorrelationCartesian)
            .filter(QCTwoResults.qctwo_config_id == qctwo_config.id)
            .join(
                CrosscorrelationCartesian, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id
            )
            .join(Timespan, CrosscorrelationCartesian.timespan_id == Timespan.id)
            .filter(
                CrosscorrelationCartesian.componentpair_id == componentpair_cartesian.id,
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
            .all()
        )

        if len(fetched_qc_ccfs) == 0:
            logger.debug("There were no ccfs for that stack")
            continue

        db.session.expunge_all()
        yield StackingInputs(
            qctwo_ccfs_container=fetched_qc_ccfs,
            componentpair_cartesian=componentpair_cartesian,
            stacking_schema=stacking_schema,
            stacking_timespan=stacking_timespan,
        )


def _validate_and_stack_ccfs_wrapper(
    inputs: StackingInputs,
) -> Tuple[Optional[CCFStack], ...]:
    return (
        _validate_and_stack_ccfs(
            qctwo_ccfs_container=inputs["qctwo_ccfs_container"],
            componentpair_cartesian=inputs["componentpair_cartesian"],
            stacking_schema=inputs["stacking_schema"],
            stacking_timespan=inputs["stacking_timespan"],
        ),
    )


def _get_stacking_worker_app():
    global _STACKING_WORKER_APP
    if _STACKING_WORKER_APP is None:
        from noiz.app import create_app

        _STACKING_WORKER_APP = create_app()
    return _STACKING_WORKER_APP


def _validate_and_stack_ccfs_by_id_batch_wrapper(
    inputs_batch: Tuple[StackingParallelInputs, ...],
) -> Tuple[Optional[CCFStack], ...]:
    app = _get_stacking_worker_app()
    with app.app_context():
        try:
            results = []
            for inputs in inputs_batch:
                results.append(_validate_and_stack_ccfs_by_id(inputs))
            return tuple(results)
        finally:
            db.session.remove()


def _validate_and_stack_ccfs_by_id(
    inputs: StackingParallelInputs,
) -> Optional[CCFStack]:
    stacking_schema = fetch_stacking_schema_by_id(id=inputs["stacking_schema_id"])
    stacking_timespan = db.session.query(StackingTimespan).filter_by(id=inputs["stacking_timespan_id"]).one()
    componentpair_cartesian = fetch_componentpairs_cartesian_by_id(inputs["componentpair_cartesian_id"])[0]
    qctwo_ccfs_container = (
        db.session.query(QCTwoResults, CrosscorrelationCartesian)
        .filter(QCTwoResults.qctwo_config_id == inputs["qctwo_config_id"])
        .join(
            CrosscorrelationCartesian,
            QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id,
        )
        .join(Timespan, CrosscorrelationCartesian.timespan_id == Timespan.id)
        .filter(
            CrosscorrelationCartesian.componentpair_id == componentpair_cartesian.id,
            Timespan.starttime >= stacking_timespan.starttime,
            Timespan.endtime <= stacking_timespan.endtime,
        )
        .all()
    )
    return _validate_and_stack_ccfs(
        qctwo_ccfs_container=qctwo_ccfs_container,
        componentpair_cartesian=componentpair_cartesian,
        stacking_schema=stacking_schema,
        stacking_timespan=stacking_timespan,
    )


def _validate_and_stack_ccfs(
    qctwo_ccfs_container: List[Tuple[QCTwoResults, CrosscorrelationCartesian]],
    componentpair_cartesian: ComponentPairCartesian,
    stacking_schema: StackingSchema,
    stacking_timespan: StackingTimespan,
) -> Optional[CCFStack]:
    """
    Takes container of tuples with QCTwoResults and CrosscorrelationCartesian (the same crosscorrelation_cartesian_id),
    verifies if CrosscorrelationCartesian is passing the QCTwo and if yes, it stacks it.

    Before stacking it verifies if there is enough CrosscorrelationCartesians to be stacked, it can be adjusted by setting
    a value of :paramref:`noiz.models.stacking.StackingSchema.minimum_ccf_count`.

    It returns an instance of :py:class:`~noiz.models.stacking.CCFStack` that is ready to be inserted to the database.

    :param qctwo_ccfs_container: CrosscorrelationCartesians to be stacked together with associated QCTwoResult instances
    :type qctwo_ccfs_container: List[Tuple[QCTwoResults, CrosscorrelationCartesian]]
    :param componentpair_cartesian: ComponentPairCartesian for which the stack is done
    :type componentpair_cartesian: ComponentPairCartesian
    :param stacking_schema: StackingSchema defining that stack
    :type stacking_schema: StackingSchema
    :param stacking_timespan: StackingTimespan that is defining that stack
    :type stacking_timespan: StackingTimespan
    :return: Returns None if CrosscorrelationCartesians cannot be stacked or CCFStack if they can
    :rtype: Optional[CCFStack]
    """

    valid_ccfs = _validate_crosscorrelations_cartesian_with_qctwo(qctwo_ccfs_container)

    no_ccfs = len(valid_ccfs)
    logger.debug(f"There are {no_ccfs} valid ccfs for that stack")
    if no_ccfs < stacking_schema.minimum_ccf_count:
        logger.debug(
            f"There only {no_ccfs} ccfs to be stacked. "
            f"The minimum number of ccfs for stack to be valid is {stacking_schema.minimum_ccf_count}."
            f" Skipping."
        )
        return None

    logger.debug(f"Calculating linear stack for {componentpair_cartesian} {stacking_schema} {stacking_timespan}")
    mean_ccf = do_linear_stack_of_crosscorrelations_cartesian(ccfs=valid_ccfs)

    stack = CCFStack(
        stacking_timespan_id=stacking_timespan.id,
        stacking_schema_id=stacking_schema.id,
        stack=numpy_to_python(mean_ccf),
        componentpair_id=componentpair_cartesian.id,
        no_ccfs=no_ccfs,
    )
    return stack


def _validate_crosscorrelations_cartesian_with_qctwo(
    qctwo_ccfs_container: Collection[Tuple[QCTwoResults, CrosscorrelationCartesian]],
) -> Tuple[CrosscorrelationCartesian, ...]:
    """
    Checks if which CrosscorrelationCartesians are passing QCTwo.
    It accepts as input a Collection of Tuples with QCTwoResults and CrosscorrelationCartesian.
    It outputs a tuple containing only those CrosscorrelationCartesian objects that are passing QCTwo.

    :param qctwo_ccfs_container: Container of tuples with QCTwoResults and CrosscorrelationCartesians to be verified
    :type qctwo_ccfs_container: Collection[Tuple[QCTwoResults, CrosscorrelationCartesian]]
    :return: Valid CrosscorrelationCartesian objects
    :rtype: Tuple[CrosscorrelationCartesian, ...]
    """

    valid_ccfs = []
    for qcres, ccf in qctwo_ccfs_container:
        if not qcres.is_passing():
            continue
        valid_ccfs.append(ccf)

    return tuple(valid_ccfs)


def _generate_ccfstack_upsert_command(stack: CCFStack) -> Insert:
    """
    Generates Upsert commands for provided CCFStacks

    :param ccfstacks: Stacks to have upsert commands prepared for
    :type ccfstacks: Collection[CCFStack]
    :return: Yields Postgres-specific upsert command, ready to be executed.
    :rtype: Generator[insert_type, None, None]
    """

    insert_command = (
        insert(CCFStack)
        .values(
            stacking_timespan_id=stack.stacking_timespan_id,
            stacking_schema_id=stack.stacking_schema_id,
            componentpair_id=stack.componentpair_id,
            stack=stack.stack,
            no_ccfs=stack.no_ccfs,
        )
        .on_conflict_do_update(
            constraint="unique_stack_per_pair_per_config",
            set_={
                "stack": stack.stack,
                "no_ccfs": stack.no_ccfs,
            },
        )
    )
    return insert_command


def fetch_ccfstacks(
    stacking_schema_id: int,
    stacking_timespan_id: Optional[int] = None,
    componentpair_ids: Optional[Collection[int]] = None,
    load_componentpair: bool = False,
) -> List[CCFStack]:
    """
    Fetch CCFStack objects from the database.

    :param stacking_schema_id: ID of the stacking schema
    :type stacking_schema_id: int
    :param stacking_timespan_id: Optional ID of specific stacking timespan
    :type stacking_timespan_id: Optional[int]
    :param componentpair_ids: Optional collection of component pair IDs to filter
    :type componentpair_ids: Optional[Collection[int]]
    :param load_componentpair: Whether to eagerly load component pair relationships
    :type load_componentpair: bool
    :return: List of CCFStack objects
    :rtype: List[CCFStack]
    """
    query = db.session.query(CCFStack).filter(CCFStack.stacking_schema_id == stacking_schema_id)

    if stacking_timespan_id is not None:
        query = query.filter(CCFStack.stacking_timespan_id == stacking_timespan_id)

    if componentpair_ids is not None:
        query = query.filter(CCFStack.componentpair_id.in_(componentpair_ids))

    if load_componentpair:
        query = query.options(
            joinedload(CCFStack.stacking_timespan),
            joinedload(CCFStack.stacking_schema),
        )

    return query.all()


def export_stacks_to_h5_files(
    stacking_schema_id: int,
    station_code_to_reject,
    output_dir: Path,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
) -> List[Path]:
    """
    Export stacked cross-correlations to HDF5 files, one file per stacking timespan.

    The output files are saved in the specified output_dir with filenames containing
    the start and end times of each stacking window.

    HDF5 File Structure:
        - global_stacks: (n_couples, n_time_samples) - Time-domain cross-correlations
        - time_axis_corr: (n_time_samples,) - Time axis in seconds (odd samples, contains 0)
        - f_intervals: (2, 1) - Frequency band: [[fmin], [fmax]]
        - id_couples: (2, n_couples) - Station pair indices: [i_rec; j_rec]
        - position_matrix: (2, n_couples) - Separation vectors: [xj-xi; yj-yi] (meters)
        - loc_matrix: (2, n_stations) - Station coordinates: [x; y] (meters)

    :param stacking_schema_id: ID of the stacking schema
    :type stacking_schema_id: int
    :param output_dir: Directory to save H5 files
    :type output_dir: Path
    :param starttime: Optional start time filter
    :type starttime: Optional[Union[datetime.date, datetime.datetime]]
    :param endtime: Optional end time filter
    :type endtime: Optional[Union[datetime.date, datetime.datetime]]
    :return: List of paths to created H5 files
    :rtype: List[Path]
    """
    from noiz.processing.h5_export import export_stacks_to_h5_full
    import numpy as np

    logger.info(f"Exporting stacks to H5 files for stacking_schema_id={stacking_schema_id}")

    # Fetch stacking schema and timespans
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema_id,
        starttime=starttime,
        endtime=endtime,
    )

    if len(stacking_timespans) == 0:
        logger.warning("No stacking timespans found for export")
        return []

    # Get frequency information from the processing chain
    ccparams = stacking_schema.crosscorrelation_cartesian_params
    proc_params = ccparams.processed_datachunk_params
    # datachunk_params = proc_params.datachunk_params

    # Get frequency band from processing params
    fmin = proc_params.filtering_low  # getattr(proc_params, 'prefiltering_low', 0.1)
    fmax = proc_params.filtering_high  # getattr(proc_params, 'prefiltering_high', 10.0)

    # Get the time axis from crosscorrelation params
    time_axis = np.array(ccparams.correlation_time_vector, dtype=np.float64)

    # Build component pairs info dictionary
    # Fetch all unique component pair IDs from stacks
    all_stacks = fetch_ccfstacks(stacking_schema_id=stacking_schema_id)
    # unique_cpair_ids = list(set(stack.componentpair_id for stack in all_stacks))
    unique_cpair_ids = list({stack.componentpair_id for stack in all_stacks})

    # Fetch component pairs with their components using the by_id function
    component_pairs_info: Dict[int, Tuple[Component, Component]] = {}
    if unique_cpair_ids:
        component_pairs = fetch_componentpairs_cartesian_by_id(
            component_pair_cartesian_id=tuple(unique_cpair_ids),
        )
        # Build the component pairs info dict
        for cpair in component_pairs:
            component_pairs_info[cpair.id] = (cpair.component_a, cpair.component_b)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    created_files: List[Path] = []

    for stacking_timespan in stacking_timespans:
        # Fetch stacks for this timespan
        stacks = fetch_ccfstacks(
            stacking_schema_id=stacking_schema_id,
            stacking_timespan_id=stacking_timespan.id,
        )

        if len(stacks) == 0:
            logger.debug(f"No stacks found for timespan {stacking_timespan.id}, skipping")
            continue

        # Export to H5
        filepath = export_stacks_to_h5_full(
            stacks=stacks,
            stacking_timespan=stacking_timespan,
            output_dir=output_dir,
            time_axis=time_axis,
            station_to_reject=station_code_to_reject,
            fmin=fmin,
            fmax=fmax,
            component_pairs_info=component_pairs_info,
        )
        created_files.append(filepath)

    logger.info(f"Exported {len(created_files)} H5 files to {output_dir}")
    return created_files


# =============================================================================
# Cylindrical CCF Stacking Functions
# =============================================================================


def stack_crosscorrelation_cylindrical(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    raise_errors: bool = False,
    batch_size: int = 5000,
    parallel: bool = True,
    crosscorrelation_cylindrical_params_id: Optional[int] = None,
) -> None:
    """
    Stack cylindrical cross-correlations for given parameters.

    Unlike cartesian CCF stacking, cylindrical CCF stacking does not use QCTwo validation.
    All cylindrical CCFs within a stacking timespan are stacked directly.

    :param stacking_schema_id: ID of the stacking schema to use
    :param starttime: Optional start time filter
    :param endtime: Optional end time filter
    :param network_codes_a: Network codes for station A
    :param station_codes_a: Station codes for station A
    :param network_codes_b: Network codes for station B
    :param station_codes_b: Station codes for station B
    :param accepted_component_code_pairs_cylindrical: Cylindrical component pair codes (e.g., "RR", "TT", "ZR")
    :param raise_errors: Whether to raise errors on failure
    :param batch_size: Number of stacks per batch
    :param parallel: Whether to use parallel processing
    :param crosscorrelation_cylindrical_params_id: Optional ID of cylindrical CCF params to filter by
    """
    calculation_inputs = _prepare_inputs_for_stacking_ccfs_cylindrical(
        stacking_schema_id=stacking_schema_id,
        starttime=starttime,
        endtime=endtime,
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        crosscorrelation_cylindrical_params_id=crosscorrelation_cylindrical_params_id,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_validate_and_stack_ccfs_cylindrical_wrapper,  # type: ignore
            upserter_callable=_generate_ccfstack_cylindrical_upsert_command,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_validate_and_stack_ccfs_cylindrical_wrapper,  # type: ignore
            upserter_callable=_generate_ccfstack_cylindrical_upsert_command,
            raise_errors=raise_errors,
        )

    return


def _prepare_inputs_for_stacking_ccfs_cylindrical(
    stacking_schema_id: int,
    starttime: Optional[Union[datetime.date, datetime.datetime]] = None,
    endtime: Optional[Union[datetime.date, datetime.datetime]] = None,
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    crosscorrelation_cylindrical_params_id: Optional[int] = None,
) -> Generator[StackingCylindricalInputs, None, None]:
    """
    Prepare inputs for cylindrical CCF stacking.

    This function fetches stacking timespans and cylindrical component pairs,
    then for each combination, fetches the corresponding cylindrical CCFs.
    """
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    no_timespans = len(stacking_timespans)
    logger.info(f"There are {no_timespans} timespans to stack for (cylindrical)")

    # Fetch cylindrical component pairs
    componentpairs_cylindrical = fetch_componentpairs_cylindrical(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        starttime=starttime,
        endtime=endtime,
    )
    logger.info(f"There are {len(componentpairs_cylindrical)} componentpairs_cylindrical to stack for")

    for stacking_timespan, componentpair_cylindrical in itertools.product(
        stacking_timespans, componentpairs_cylindrical
    ):
        # Build query for cylindrical CCFs
        query = (
            db.session.query(CrosscorrelationCylindrical)
            .join(Timespan, CrosscorrelationCylindrical.timespan_id == Timespan.id)
            .filter(
                CrosscorrelationCylindrical.componentpair_cylindrical_id == componentpair_cylindrical.id,
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
        )

        # Optionally filter by crosscorrelation_cylindrical_params_id
        if crosscorrelation_cylindrical_params_id is not None:
            query = query.filter(
                CrosscorrelationCylindrical.crosscorrelation_cylindrical_params_id
                == crosscorrelation_cylindrical_params_id
            )

        fetched_ccfs = query.all()

        if len(fetched_ccfs) == 0:
            logger.debug("There were no cylindrical ccfs for that stack")
            continue

        db.session.expunge_all()
        yield StackingCylindricalInputs(
            ccfs_cylindrical=fetched_ccfs,
            componentpair_cylindrical=componentpair_cylindrical,
            stacking_schema=stacking_schema,
            stacking_timespan=stacking_timespan,
        )


def _validate_and_stack_ccfs_cylindrical_wrapper(
    inputs: StackingCylindricalInputs,
) -> Tuple[Optional[CCFStackCylindrical], ...]:
    return (
        _validate_and_stack_ccfs_cylindrical(
            ccfs_cylindrical=inputs["ccfs_cylindrical"],
            componentpair_cylindrical=inputs["componentpair_cylindrical"],
            stacking_schema=inputs["stacking_schema"],
            stacking_timespan=inputs["stacking_timespan"],
        ),
    )


def _validate_and_stack_ccfs_cylindrical(
    ccfs_cylindrical: List[CrosscorrelationCylindrical],
    componentpair_cylindrical: ComponentPairCylindrical,
    stacking_schema: StackingSchema,
    stacking_timespan: StackingTimespan,
) -> Optional[CCFStackCylindrical]:
    """
    Stack cylindrical CCFs for a given component pair and timespan.

    Unlike cartesian CCF stacking, no QCTwo validation is performed.
    All provided cylindrical CCFs are stacked directly.

    :param ccfs_cylindrical: List of cylindrical CCFs to stack
    :param componentpair_cylindrical: The cylindrical component pair
    :param stacking_schema: The stacking schema
    :param stacking_timespan: The stacking timespan
    :return: CCFStackCylindrical object or None if not enough CCFs
    """
    no_ccfs = len(ccfs_cylindrical)
    logger.debug(f"There are {no_ccfs} cylindrical ccfs for that stack")

    if no_ccfs < stacking_schema.minimum_ccf_count:
        logger.debug(
            f"There only {no_ccfs} cylindrical ccfs to be stacked. "
            f"The minimum number of ccfs for stack to be valid is {stacking_schema.minimum_ccf_count}. "
            f"Skipping."
        )
        return None

    logger.debug(
        f"Calculating linear stack for cylindrical {componentpair_cylindrical} {stacking_schema} {stacking_timespan}"
    )
    mean_ccf = do_linear_stack_of_crosscorrelations_cylindrical(ccfs=ccfs_cylindrical)

    stack = CCFStackCylindrical(
        stacking_timespan_id=stacking_timespan.id,
        stacking_schema_id=stacking_schema.id,
        stack=numpy_to_python(mean_ccf),
        componentpair_cylindrical_id=componentpair_cylindrical.id,
        no_ccfs=no_ccfs,
        ccfs=list(ccfs_cylindrical),
    )
    return stack


def _generate_ccfstack_cylindrical_upsert_command(stack: CCFStackCylindrical) -> Insert:
    """
    Generates Upsert command for provided CCFStackCylindrical.

    :param stack: Stack to have upsert command prepared for
    :return: Postgres-specific upsert command, ready to be executed.
    """
    insert_command = (
        insert(CCFStackCylindrical)
        .values(
            stacking_timespan_id=stack.stacking_timespan_id,
            stacking_schema_id=stack.stacking_schema_id,
            componentpair_cylindrical_id=stack.componentpair_cylindrical_id,
            stack=stack.stack,
            no_ccfs=stack.no_ccfs,
        )
        .on_conflict_do_update(
            constraint="unique_stack_cylindrical_per_pair_per_config",
            set_={
                "stack": stack.stack,
                "no_ccfs": stack.no_ccfs,
            },
        )
    )
    return insert_command


def fetch_ccfstacks_cylindrical(
    stacking_schema_id: int,
    stacking_timespan_id: Optional[int] = None,
    componentpair_cylindrical_ids: Optional[Collection[int]] = None,
    load_componentpair: bool = False,
) -> List[CCFStackCylindrical]:
    """
    Fetch CCFStackCylindrical objects from the database.

    :param stacking_schema_id: ID of the stacking schema
    :param stacking_timespan_id: Optional ID of specific stacking timespan
    :param componentpair_cylindrical_ids: Optional collection of cylindrical component pair IDs to filter
    :param load_componentpair: Whether to eagerly load component pair relationships
    :return: List of CCFStackCylindrical objects
    """
    query = db.session.query(CCFStackCylindrical).filter(CCFStackCylindrical.stacking_schema_id == stacking_schema_id)

    if stacking_timespan_id is not None:
        query = query.filter(CCFStackCylindrical.stacking_timespan_id == stacking_timespan_id)

    if componentpair_cylindrical_ids is not None:
        query = query.filter(CCFStackCylindrical.componentpair_cylindrical_id.in_(componentpair_cylindrical_ids))

    if load_componentpair:
        query = query.options(
            joinedload(CCFStackCylindrical.stacking_timespan),
            joinedload(CCFStackCylindrical.stacking_schema),
            joinedload(CCFStackCylindrical.componentpair_cylindrical),
        )

    return query.all()
