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
from typing import Any, Collection, Union, List, Optional, Tuple, Generator, Dict, Iterable, cast

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
    skip_componentpair_ids: Optional[Collection[int]] = None,
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
        if skip_componentpair_ids:
            skip_set = set(skip_componentpair_ids)
            parallel_calculation_inputs = (
                inp for inp in parallel_calculation_inputs if inp["componentpair_cartesian_id"] not in skip_set
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
        if skip_componentpair_ids:
            skip_set_seq = set(skip_componentpair_ids)
            sequential_calculation_inputs = (
                inp for inp in sequential_calculation_inputs if inp["componentpair_cartesian"].id not in skip_set_seq
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


def _convergence_load_ccfs_worker(
    cpair_id: int,
    qctwo_config_id: int,
    timespan_ids: List[int],
) -> Dict[int, list]:
    """Dask worker: load CCFs for one pair, return {timespan_id: ccf_array_as_list}."""
    import numpy as np

    app = _get_stacking_worker_app()
    with app.app_context():
        try:
            result: Dict[int, list] = {}
            # Process in chunks to limit memory pressure
            chunk_size = 500
            for i in range(0, len(timespan_ids), chunk_size):
                chunk_ts_ids = timespan_ids[i : i + chunk_size]
                rows = (
                    db.session.query(CrosscorrelationCartesian)
                    .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
                    .filter(
                        QCTwoResults.qctwo_config_id == qctwo_config_id,
                        CrosscorrelationCartesian.componentpair_id == cpair_id,
                        CrosscorrelationCartesian.timespan_id.in_(chunk_ts_ids),
                    )
                    .all()
                )
                for ccf in rows:
                    result[ccf.timespan_id] = list(ccf.ccf)
                db.session.expunge_all()
            return result
        finally:
            db.session.remove()


def _convergence_save_stack_worker(
    cpair_id: int,
    qctwo_config_id: int,
    stacking_schema_id: int,
    stacking_timespan_id: int,
    timespan_ids: List[int],
    minimum_ccf_count: int,
) -> Optional[Dict]:
    """Dask worker: compute final stack for one (pair, stacking_timespan), return dict for CCFStack."""
    import numpy as np

    app = _get_stacking_worker_app()
    with app.app_context():
        try:
            rows = (
                db.session.query(CrosscorrelationCartesian)
                .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
                .filter(
                    QCTwoResults.qctwo_config_id == qctwo_config_id,
                    CrosscorrelationCartesian.componentpair_id == cpair_id,
                    CrosscorrelationCartesian.timespan_id.in_(timespan_ids),
                )
                .all()
            )
            no_ccfs = len(rows)
            if no_ccfs < minimum_ccf_count:
                return None
            stack_array = np.mean([np.array(row.ccf) for row in rows], axis=0)
            return {
                "stacking_timespan_id": stacking_timespan_id,
                "stacking_schema_id": stacking_schema_id,
                "componentpair_id": cpair_id,
                "stack": list(stack_array),
                "no_ccfs": no_ccfs,
            }
        finally:
            db.session.remove()


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
            ccf_params_id=ccparams.id,
            stacking_schema_id=stacking_schema_id,
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


def run_convergence_study(
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
    increment_hours: float = 24.0,
    availability_threshold: float = 0.7,
    output_dir: Optional[Path] = None,
    batch_size: int = 50,
    parallel: bool = True,
    frequency_bands: Optional[List[Tuple[float, float]]] = None,
    force_reload: bool = False,
    per_station_plots: bool = False,
    skip_gathers: bool = False,
    skip_psd_gathers: bool = False,
    plot_pcolors: bool = False,
    plot_gathers_for_all: bool = False,
    skip_stability: bool = False,
    stability_window_days: float = 7.0,
    stability_overlap: float = 0.5,
    overwrite_stacks: bool = False,
    only_psd_vs_raw: bool = False,
    only_psd_vs_gathers: bool = False,
    notch_whitening: Optional[List[Tuple[float, float]]] = None,
    notch_cut: Optional[List[Tuple[float, float]]] = None,
    notch_interpolate: Optional[List[Tuple[float, float]]] = None,
    notch_factor: float = 1.0,
    max_lag_seconds: Optional[float] = None,
) -> List[int]:
    """Run a convergence study on cartesian CCF stacking.

    Only timespans common to all eligible station pairs are used.  Pairs with
    availability below ``availability_threshold`` (fraction of requested interval)
    are discarded.  The stacking is performed incrementally with steps of
    ``increment_hours`` and RMS relative differences are computed between successive
    intermediate stacks.  Two PNG plots are produced.

    The final full stacks are upserted to the database as CCFStack records.
    Returns the list of component pair IDs whose stacks were computed and saved,
    so that subsequent normal stacking can skip them.
    """
    import numpy as np
    from collections import defaultdict
    from noiz.processing.stacking import (
        compute_rms_relative_difference,
        plot_convergence_global,
        plot_convergence_pcolor,
        plot_convergence_gather,
        bandpass_filter_ccf,
        spectral_whitening,
        spectral_notch_cut,
        spectral_notch_interpolate,
        _planck_taper,
    )

    if output_dir is None:
        output_dir = Path(os.environ.get("PROCESSED_DATA_DIR", ".")) / "convergence_study"
    output_dir.mkdir(parents=True, exist_ok=True)

    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)
    qctwo_config = fetch_qctwo_config_single(id=stacking_schema.qctwo_config_id)

    # Determine frequency bands for the convergence analysis
    cc_params = stacking_schema.crosscorrelation_cartesian_params
    sampling_rate = cc_params.sampling_rate
    nominal_low = cc_params.processed_datachunk_params.filtering_low
    nominal_high = cc_params.processed_datachunk_params.filtering_high

    # Top-level param directory disambiguates different crosscorrelation configs
    param_dir = output_dir / f"p{cc_params.id}"
    param_dir.mkdir(parents=True, exist_ok=True)

    # CCF cache under param directory
    ccf_cache_dir = param_dir / "ccf_cache"
    ccf_cache_dir.mkdir(parents=True, exist_ok=True)
    ccf_cache_path = ccf_cache_dir / "all_pairs_ccfs.npz"

    if frequency_bands is None or len(frequency_bands) == 0:
        bands = [(nominal_low, nominal_high)]
    else:
        bands = list(frequency_bands)
        for f_low, f_high in bands:
            if f_low < nominal_low or f_high > nominal_high:
                logger.warning(
                    f"Requested band {f_low}-{f_high} Hz extends beyond nominal "
                    f"CCF band {nominal_low}-{nominal_high} Hz"
                )

    band_labels = [f"{f_low:.3g}-{f_high:.3g}Hz" for f_low, f_high in bands]
    logger.info(f"Convergence study: {len(bands)} frequency band(s): {', '.join(band_labels)}")

    # When only_psd_vs_raw or only_psd_vs_gathers, skip everything except the target section
    _skip_other_plots = only_psd_vs_raw or only_psd_vs_gathers
    if _skip_other_plots:
        skip_psd_gathers = True
        skip_stability = False  # need stability windows
        plot_pcolors = False
        if only_psd_vs_raw:
            skip_gathers = True
            logger.info("Only PSD vs raw mode: skipping convergence/stability plots, gathers, DB write")
        else:
            skip_gathers = True
            logger.info("Only PSD vs gather mode: skipping convergence/stability plots, DB write")

    # Fetch all timespans in the requested range
    all_timespans = (
        db.session.query(Timespan)
        .filter(
            *([Timespan.starttime >= starttime] if starttime else []),
            *([Timespan.endtime <= endtime] if endtime else []),
        )
        .order_by(Timespan.starttime)
        .all()
    )
    if not all_timespans:
        logger.warning("No timespans found in the requested range.")
        return []

    timespan_ids = [ts.id for ts in all_timespans]
    total_timespans = len(timespan_ids)
    logger.info(f"Convergence study: {total_timespans} timespans in requested range")

    # Fetch component pairs
    componentpairs = fetch_componentpairs_cartesian(
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
    if not componentpairs:
        logger.warning("No component pairs found.")
        return []

    # For each pair, find which timespans have valid CCFs passing QCTwo
    pair_available_timespans: Dict[int, set] = {}
    pair_labels: Dict[int, str] = {}

    for cpair in componentpairs:
        available_ts_ids = {
            row[0]
            for row in (
                db.session.query(Timespan.id)
                .join(CrosscorrelationCartesian, CrosscorrelationCartesian.timespan_id == Timespan.id)
                .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
                .filter(
                    QCTwoResults.qctwo_config_id == qctwo_config.id,
                    CrosscorrelationCartesian.componentpair_id == cpair.id,
                    cast(Any, Timespan.id).in_(timespan_ids),
                )
                .distinct()
                .all()
            )
        }
        pair_available_timespans[cpair.id] = available_ts_ids
        pair_labels[cpair.id] = str(cpair)

    # Discard pairs with no data at all
    pairs_with_data = {cpid for cpid, ts in pair_available_timespans.items() if len(ts) > 0}
    if not pairs_with_data:
        logger.warning("No pairs have any available CCFs.")
        return []

    # Determine study period from quantiles of per-pair first/last timespan dates
    ts_by_id = {ts.id: ts for ts in all_timespans}
    pair_first_dates = []
    pair_last_dates = []
    for cpair_id in pairs_with_data:
        avail_ts = pair_available_timespans[cpair_id]
        avail_starttimes = sorted(ts_by_id[tid].starttime for tid in avail_ts if tid in ts_by_id)
        if avail_starttimes:
            pair_first_dates.append(avail_starttimes[0])
            pair_last_dates.append(avail_starttimes[-1])

    pair_first_dates.sort()
    pair_last_dates.sort()
    # q90 of first dates, q10 of last dates
    idx_q90_first = int(len(pair_first_dates) * 0.9)
    idx_q10_last = int(len(pair_last_dates) * 0.1)
    idx_q90_first = min(idx_q90_first, len(pair_first_dates) - 1)
    study_start = pair_first_dates[idx_q90_first]
    study_end = pair_last_dates[idx_q10_last]

    if study_start >= study_end:
        logger.warning(f"Study period is empty: q90(first)={study_start} >= q10(last)={study_end}")
        return []

    logger.info(f"Convergence study period: {study_start} to {study_end}")

    # Build top-level output directories
    day_start = (
        study_start.strftime("%Y%m%d") if hasattr(study_start, "strftime") else str(study_start)[:10].replace("-", "")
    )
    day_stop = study_end.strftime("%Y%m%d") if hasattr(study_end, "strftime") else str(study_end)[:10].replace("-", "")
    inc_label = f"{increment_hours:.0f}h" if increment_hours >= 1 else f"{increment_hours * 60:.0f}min"
    overlap_pct = int(stability_overlap * 100)
    if notch_whitening:
        wh_bands_str = "_".join(f"{lo:.3g}-{hi:.3g}" for lo, hi in notch_whitening)
        factor_str = f"_x{notch_factor:.3g}" if notch_factor != 1.0 else ""
        wh_tag = f"_whitened_{wh_bands_str}Hz{factor_str}"
    elif notch_cut:
        nc_bands_str = "_".join(f"{lo:.3g}-{hi:.3g}" for lo, hi in notch_cut)
        wh_tag = f"_notchcut_{nc_bands_str}Hz"
    elif notch_interpolate:
        ni_bands_str = "_".join(f"{lo:.3g}-{hi:.3g}" for lo, hi in notch_interpolate)
        wh_tag = f"_notchinterp_{ni_bands_str}Hz"
    else:
        wh_tag = ""
    lag_tag = f"_lag{max_lag_seconds:.0f}s" if max_lag_seconds is not None else ""
    suffix_tag = f"{wh_tag}{lag_tag}"
    conv_dir = param_dir / f"convergence_{inc_label}_{day_start}_{day_stop}{suffix_tag}"
    conv_dir.mkdir(parents=True, exist_ok=True)
    stab_dir = (
        param_dir / f"stability_{stability_window_days:.0f}d_{overlap_pct}pct_{day_start}_{day_stop}{suffix_tag}"
    )
    stab_dir.mkdir(parents=True, exist_ok=True)
    # Label suffix for PDF filenames
    pdf_conv_suffix = f"convergence_{inc_label}_{day_start}_{day_stop}{suffix_tag}"
    pdf_stab_suffix = f"stability_{stability_window_days:.0f}d_{overlap_pct}pct_{day_start}_{day_stop}{suffix_tag}"

    # Filter timespans to the study period
    study_timespans = [ts for ts in all_timespans if ts.starttime >= study_start and ts.endtime <= study_end]
    if len(study_timespans) < 2:
        logger.warning(f"Only {len(study_timespans)} timespans in study period.")
        return []

    study_ts_ids = {ts.id for ts in study_timespans}
    total_study_timespans = len(study_timespans)
    logger.info(f"Convergence study: {total_study_timespans} timespans in study period")

    # Apply availability threshold relative to the study period
    # All pairs with data in the study period are loaded; threshold only filters plot outputs
    all_pair_ids = []
    eligible_pair_ids = []
    pair_availability: Dict[int, float] = {}
    for cpair_id in sorted(pairs_with_data):
        avail_in_study = pair_available_timespans[cpair_id] & study_ts_ids
        fraction = len(avail_in_study) / total_study_timespans
        if fraction == 0:
            continue  # no data in study period, skip entirely
        pair_availability[cpair_id] = fraction
        all_pair_ids.append(cpair_id)
        if fraction >= availability_threshold:
            eligible_pair_ids.append(cpair_id)
        else:
            logger.info(
                f"Pair {pair_labels[cpair_id]}: availability "
                f"{fraction:.1%} < {availability_threshold:.0%} (will be loaded but excluded from plots)"
            )

    if not all_pair_ids:
        logger.warning("No pairs have any available CCFs.")
        return []

    logger.info(
        f"Convergence study: {len(all_pair_ids)} pairs with data, "
        f"{len(eligible_pair_ids)} eligible after availability filter"
    )

    # Build distance lookup and ordered label list for all pairs
    cpair_by_id = {cp.id: cp for cp in componentpairs}
    pair_distances: Dict[int, float] = {}
    all_pair_labels: list = []
    for cpair_id in all_pair_ids:
        pair_distances[cpair_id] = cpair_by_id[cpair_id].distance
        all_pair_labels.append(pair_labels[cpair_id])

    # Eligible pair labels for plots
    eligible_pair_labels: list = [pair_labels[cpid] for cpid in eligible_pair_ids]

    # Get time axis for gather plots
    time_axis = np.array(
        stacking_schema.crosscorrelation_cartesian_params.correlation_time_vector,
        dtype=np.float64,
    )

    # Determine timespan duration and build time-based increments
    ts_duration = study_timespans[1].starttime - study_timespans[0].starttime
    ts_duration_hours = ts_duration.total_seconds() / 3600.0
    timespans_per_increment = max(1, int(round(increment_hours / ts_duration_hours)))
    logger.info(
        f"Convergence study: increment={increment_hours}h, "
        f"timespan duration~{ts_duration_hours:.1f}h, "
        f"{timespans_per_increment} timespans per increment"
    )

    n_study = len(study_timespans)
    increment_boundaries = list(range(timespans_per_increment, n_study + 1, timespans_per_increment))
    if not increment_boundaries or increment_boundaries[-1] != n_study:
        increment_boundaries.append(n_study)

    n_increments = len(increment_boundaries)
    n_pairs = len(all_pair_ids)

    # Precompute cumulative durations from actual timestamps
    cumulative_durations = []
    for end_pos in increment_boundaries:
        elapsed = study_timespans[end_pos - 1].endtime - study_timespans[0].starttime
        cumulative_durations.append(elapsed.total_seconds() / 86400.0)

    # Timespan IDs per increment (for PSD vs raw queries)
    increment_ts_ids: List[List[int]] = []
    prev_end = 0
    for end_pos in increment_boundaries:
        increment_ts_ids.append([study_timespans[i].id for i in range(prev_end, end_pos)])
        prev_end = end_pos

    # Per-pair list of available timespan IDs within study period (ordered)
    study_ts_id_list = [ts.id for ts in study_timespans]
    pair_study_ts_ids: Dict[int, List[int]] = {}
    for cpair_id in all_pair_ids:
        pair_study_ts_ids[cpair_id] = [tid for tid in study_ts_id_list if tid in pair_available_timespans[cpair_id]]

    # Per-band storage for RMS and stacks
    per_band_rms: Dict[int, np.ndarray] = {
        b_idx: np.full((n_pairs, n_increments), np.nan) for b_idx in range(len(bands))
    }
    per_band_increment_stacks: Dict[int, Dict[int, Dict[int, np.ndarray]]] = {
        b_idx: defaultdict(dict) for b_idx in range(len(bands))
    }
    # Unfiltered (broadband) cumulative increment stacks for convergence plots
    unfiltered_increment_stacks: Dict[int, Dict[int, np.ndarray]] = defaultdict(dict)
    # Per-increment sums and counts (non-cumulative) for stability analysis
    increment_sums: Dict[int, Dict[int, np.ndarray]] = defaultdict(dict)  # [inc_idx][cpair_id]
    increment_counts: Dict[int, Dict[int, int]] = defaultdict(dict)  # [inc_idx][cpair_id]

    # Try loading increment data from cache
    loaded_from_cache = False
    if not force_reload and ccf_cache_path.exists():
        import json as _json

        logger.info(f"Convergence study: loading increment data from cache {ccf_cache_path.name}")
        cached = np.load(str(ccf_cache_path), allow_pickle=False)

        # Check cache consistency with all pairs (not just eligible)
        cached_pair_ids = set()
        for key in cached.files:
            if key.startswith("incr_sums_"):
                cached_pair_ids.add(int(key.split("_", 2)[2]))

        all_set = set(all_pair_ids)
        missing_from_cache = all_set - cached_pair_ids
        extra_in_cache = cached_pair_ids - all_set
        if missing_from_cache or extra_in_cache:
            msg = (
                f"Cache is inconsistent with current pair selection: "
                f"{len(missing_from_cache)} pairs missing from cache, "
                f"{len(extra_in_cache)} extra pairs in cache. "
                f"Use --force_reload to re-download CCFs from the database."
            )
            raise RuntimeError(msg)

        # Check increment count consistency
        sample_key = f"incr_sums_{all_pair_ids[0]}"
        if sample_key in cached.files and cached[sample_key].shape[0] != n_increments:
            msg = (
                f"Cache has {cached[sample_key].shape[0]} increments but current config expects "
                f"{n_increments}. Use --force_reload to recompute."
            )
            raise RuntimeError(msg)

        # Load per-increment sums and counts, derive cumulative stacks
        for cpair_id in all_pair_ids:
            sums_matrix = cached[f"incr_sums_{cpair_id}"]
            counts_vec = cached[f"incr_counts_{cpair_id}"]
            cumul_sum = np.zeros_like(sums_matrix[0])
            cumul_count = 0
            for inc_idx in range(n_increments):
                increment_sums[inc_idx][cpair_id] = sums_matrix[inc_idx]
                increment_counts[inc_idx][cpair_id] = int(counts_vec[inc_idx])
                cumul_sum = cumul_sum + sums_matrix[inc_idx]
                cumul_count += int(counts_vec[inc_idx])
                if cumul_count > 0:
                    unfiltered_increment_stacks[inc_idx][cpair_id] = cumul_sum / cumul_count

        logger.info(f"Convergence study: loaded {len(all_pair_ids)} pairs x {n_increments} increments from cache")
        loaded_from_cache = True

    if not loaded_from_cache:
        # Load CCFs from DB and compute incremental stacks for ALL pairs
        for batch_start in range(0, n_pairs, batch_size):
            batch_end = min(batch_start + batch_size, n_pairs)
            batch_pair_ids = all_pair_ids[batch_start:batch_end]
            logger.info(
                f"Convergence study: processing pair batch {batch_start // batch_size + 1} "
                f"({len(batch_pair_ids)} pairs)"
            )

            batch_ccfs: Dict[int, Dict[int, np.ndarray]] = defaultdict(dict)
            if parallel:
                from dask.distributed import wait as dask_wait, as_completed

                client, cluster, created_worker_count = create_process_dask_client()
                n_workers = max(1, len(client.scheduler_info().get("workers", {})), created_worker_count)
                pair_i_global = 0
                for wave_start in range(0, len(batch_pair_ids), n_workers):
                    wave_ids = batch_pair_ids[wave_start : wave_start + n_workers]
                    futures = {
                        cpair_id: client.submit(
                            _convergence_load_ccfs_worker,
                            cpair_id,
                            qctwo_config.id,
                            pair_study_ts_ids[cpair_id],
                        )
                        for cpair_id in wave_ids
                    }
                    dask_wait(list(futures.values()))
                    for cpair_id in wave_ids:
                        result = futures[cpair_id].result()
                        batch_ccfs[cpair_id] = {ts_id: np.array(arr) for ts_id, arr in result.items()}
                        pair_i_global += 1
                        logger.info(
                            f"Convergence study: loaded CCFs for pair {batch_start + pair_i_global}/{n_pairs} "
                            f"({len(result)} timespans)"
                        )
                    del futures
                client.close(timeout=30)
                cluster.close(timeout=30)
            else:
                for pair_i, cpair_id in enumerate(batch_pair_ids):
                    ts_ids_to_load = pair_study_ts_ids[cpair_id]
                    if not ts_ids_to_load:
                        continue
                    rows = (
                        db.session.query(CrosscorrelationCartesian)
                        .join(QCTwoResults, QCTwoResults.crosscorrelation_cartesian_id == CrosscorrelationCartesian.id)
                        .filter(
                            QCTwoResults.qctwo_config_id == qctwo_config.id,
                            CrosscorrelationCartesian.componentpair_id == cpair_id,
                            CrosscorrelationCartesian.timespan_id.in_(ts_ids_to_load),
                        )
                        .all()
                    )
                    for ccf in rows:
                        batch_ccfs[cpair_id][ccf.timespan_id] = np.array(ccf.ccf)
                    db.session.expunge_all()
                    logger.info(
                        f"Convergence study: loaded CCFs for pair {batch_start + pair_i + 1}/{n_pairs} "
                        f"({len(rows)} timespans)"
                    )

            # Per-increment stacking (non-cumulative sums/counts per increment)
            uf_prev_end_pos = 0
            for inc_idx, end_pos in enumerate(increment_boundaries):
                new_ts_ids = {study_timespans[j].id for j in range(uf_prev_end_pos, end_pos)}
                for cpair_id in batch_pair_ids:
                    inc_sum = None
                    inc_count = 0
                    for ts_id in new_ts_ids:
                        if ts_id in batch_ccfs[cpair_id]:
                            if inc_sum is None:
                                inc_sum = batch_ccfs[cpair_id][ts_id].copy()
                            else:
                                inc_sum += batch_ccfs[cpair_id][ts_id]
                            inc_count += 1
                    if inc_sum is not None:
                        increment_sums[inc_idx][cpair_id] = inc_sum
                        increment_counts[inc_idx][cpair_id] = inc_count
                uf_prev_end_pos = end_pos

            # Derive cumulative stacks from per-increment data
            for cpair_id in batch_pair_ids:
                cumul_sum = None
                cumul_count = 0
                for inc_idx in range(n_increments):
                    if cpair_id in increment_sums[inc_idx]:
                        if cumul_sum is None:
                            cumul_sum = increment_sums[inc_idx][cpair_id].copy()
                        else:
                            cumul_sum += increment_sums[inc_idx][cpair_id]
                        cumul_count += increment_counts[inc_idx][cpair_id]
                    if cumul_sum is not None and cumul_count > 0:
                        unfiltered_increment_stacks[inc_idx][cpair_id] = cumul_sum / cumul_count

            del batch_ccfs
            db.session.expunge_all()
            import gc

            gc.collect()

        # Save per-increment sums and counts to cache
        import json as _json

        cache_arrays = {}
        metadata = {}
        for cpair_id in all_pair_ids:
            cp = cpair_by_id[cpair_id]
            # Build per-increment sum matrix (n_increments x n_samples) and count vector
            sum_rows = []
            count_vec = []
            for inc_idx in range(n_increments):
                if cpair_id in increment_sums[inc_idx]:
                    sum_rows.append(increment_sums[inc_idx][cpair_id])
                    count_vec.append(increment_counts[inc_idx][cpair_id])
                else:
                    sample_len = len(next(iter(unfiltered_increment_stacks[n_increments - 1].values())))
                    sum_rows.append(np.zeros(sample_len))
                    count_vec.append(0)
            cache_arrays[f"incr_sums_{cpair_id}"] = np.array(sum_rows)
            cache_arrays[f"incr_counts_{cpair_id}"] = np.array(count_vec, dtype=np.int32)
            metadata[str(cpair_id)] = {
                "label": pair_labels[cpair_id],
                "component_a": {
                    "network": cp.component_a.network,
                    "station": cp.component_a.station,
                    "component": cp.component_a.component,
                    "lat": cp.component_a.lat,
                    "lon": cp.component_a.lon,
                    "x": cp.component_a.x,
                    "y": cp.component_a.y,
                    "elevation": cp.component_a.elevation,
                },
                "component_b": {
                    "network": cp.component_b.network,
                    "station": cp.component_b.station,
                    "component": cp.component_b.component,
                    "lat": cp.component_b.lat,
                    "lon": cp.component_b.lon,
                    "x": cp.component_b.x,
                    "y": cp.component_b.y,
                    "elevation": cp.component_b.elevation,
                },
                "distance": cp.distance,
                "azimuth": cp.azimuth,
                "backazimuth": cp.backazimuth,
                "arcdistance": cp.arcdistance,
                "component_code_pair": cp.component_code_pair,
                "autocorrelation": cp.autocorrelation,
                "intracorrelation": cp.intracorrelation,
            }
        cache_arrays["metadata_json"] = np.array([_json.dumps(metadata)])
        cache_arrays["cumulative_durations"] = np.array(cumulative_durations)
        logger.info(f"Convergence study: saving cache for {len(metadata)} pairs to {ccf_cache_path.name}")
        np.savez_compressed(str(ccf_cache_path), **cast(Any, cache_arrays))
        logger.info(f"Convergence study: cache saved ({ccf_cache_path.stat().st_size / 1e6:.1f} MB)")

    # Truncate CCFs to max_lag_seconds (after cache save, so cache keeps full length)
    if max_lag_seconds is not None:
        half_samples = int(max_lag_seconds * sampling_rate)
        for inc_idx in range(n_increments):
            for cpair_id in list(increment_sums[inc_idx].keys()):
                arr = increment_sums[inc_idx][cpair_id]
                center = len(arr) // 2
                increment_sums[inc_idx][cpair_id] = arr[center - half_samples : center + half_samples + 1]
            for cpair_id in list(unfiltered_increment_stacks[inc_idx].keys()):
                arr = unfiltered_increment_stacks[inc_idx][cpair_id]
                center = len(arr) // 2
                unfiltered_increment_stacks[inc_idx][cpair_id] = arr[center - half_samples : center + half_samples + 1]
        logger.info(f"Truncated CCFs to +/-{max_lag_seconds}s ({2 * half_samples + 1} samples)")
        # Recompute time_axis to match truncated length
        time_axis = np.linspace(-max_lag_seconds, max_lag_seconds, 2 * half_samples + 1)

    # Apply time-domain Planck taper once to all stacks (before any freq-domain ops)
    _taper_cache = {}
    for inc_idx in range(n_increments):
        for cpair_id in list(unfiltered_increment_stacks[inc_idx].keys()):
            arr = unfiltered_increment_stacks[inc_idx][cpair_id]
            n_arr = len(arr)
            if n_arr not in _taper_cache:
                _taper_cache[n_arr] = _planck_taper(n_arr, epsilon=0.05)
            unfiltered_increment_stacks[inc_idx][cpair_id] = arr * _taper_cache[n_arr]
        for cpair_id in list(increment_sums[inc_idx].keys()):
            arr = increment_sums[inc_idx][cpair_id]
            n_arr = len(arr)
            if n_arr not in _taper_cache:
                _taper_cache[n_arr] = _planck_taper(n_arr, epsilon=0.05)
            increment_sums[inc_idx][cpair_id] = arr * _taper_cache[n_arr]

    # Apply notch cut AFTER taper (taper leaks energy into zeroed bands)
    if notch_cut:
        logger.info(f"Applying notch cut in {len(notch_cut)} band(s) to convergence stacks")
        for inc_idx in range(n_increments):
            for cpair_id in list(unfiltered_increment_stacks[inc_idx].keys()):
                stack = unfiltered_increment_stacks[inc_idx][cpair_id]
                unfiltered_increment_stacks[inc_idx][cpair_id] = spectral_notch_cut(
                    stack, sampling_rate, notch_bands=notch_cut
                )
            for cpair_id in list(increment_sums[inc_idx].keys()):
                stack = increment_sums[inc_idx][cpair_id]
                increment_sums[inc_idx][cpair_id] = spectral_notch_cut(stack, sampling_rate, notch_bands=notch_cut)

    # Apply notch interpolate AFTER taper (same position as notch_cut)
    if notch_interpolate:
        logger.info(f"Applying notch interpolate in {len(notch_interpolate)} band(s) to convergence stacks")
        for inc_idx in range(n_increments):
            for cpair_id in list(unfiltered_increment_stacks[inc_idx].keys()):
                stack = unfiltered_increment_stacks[inc_idx][cpair_id]
                unfiltered_increment_stacks[inc_idx][cpair_id] = spectral_notch_interpolate(
                    stack, sampling_rate, notch_bands=notch_interpolate
                )
            for cpair_id in list(increment_sums[inc_idx].keys()):
                stack = increment_sums[inc_idx][cpair_id]
                increment_sums[inc_idx][cpair_id] = spectral_notch_interpolate(
                    stack, sampling_rate, notch_bands=notch_interpolate
                )

    # Apply spectral whitening to all cumulative stacks if requested
    if notch_whitening:
        logger.info(f"Applying notch whitening in {len(notch_whitening)} band(s) to convergence stacks")
        for inc_idx in range(n_increments):
            for cpair_id in list(unfiltered_increment_stacks[inc_idx].keys()):
                stack = unfiltered_increment_stacks[inc_idx][cpair_id]
                unfiltered_increment_stacks[inc_idx][cpair_id] = spectral_whitening(
                    stack, sampling_rate, whiten_bands=notch_whitening, notch_factor=notch_factor
                )

    # Compute per-band filtered stacks and RMS from unfiltered increment stacks
    for b_idx, (f_low, f_high) in enumerate(bands if not _skip_other_plots else []):
        is_nominal = len(bands) == 1 and frequency_bands is None
        if not is_nominal:
            logger.info(f"Convergence study: filtering band {band_labels[b_idx]}")

        for pair_idx, cpair_id in enumerate(all_pair_ids):
            if (pair_idx + 1) % 50 == 0 or pair_idx == n_pairs - 1:
                logger.info(f"  band RMS computation: pair {pair_idx + 1}/{n_pairs}")
            prev_stack = None
            for inc_idx in range(n_increments):
                if cpair_id not in unfiltered_increment_stacks[inc_idx]:
                    continue
                raw_stack = unfiltered_increment_stacks[inc_idx][cpair_id]
                if is_nominal:
                    current_stack = raw_stack
                else:
                    current_stack = bandpass_filter_ccf(raw_stack, sampling_rate, f_low, f_high)
                per_band_increment_stacks[b_idx][inc_idx][cpair_id] = current_stack

                # Normalize by max for shape-based RMS comparison
                max_abs = float(np.max(np.abs(current_stack)))
                normed = current_stack / max_abs if max_abs > 0 else current_stack
                if prev_stack is not None:
                    rms = compute_rms_relative_difference(normed, prev_stack)
                    per_band_rms[b_idx][pair_idx, inc_idx] = rms
                prev_stack = normed

    # Compute PSD window bounds per pair from the LAST increment (for consistent windowing)
    from noiz.processing.stacking import _get_window_bounds

    last_inc_stacks = unfiltered_increment_stacks.get(n_increments - 1, {})
    pair_window_bounds: Dict[int, Tuple[int, int]] = {}
    for cpair_id, stack in last_inc_stacks.items():
        pair_window_bounds[cpair_id] = _get_window_bounds(stack, sampling_rate, nominal_low)
    logger.info(
        f"Convergence study: computed PSD window bounds from last increment for {len(pair_window_bounds)} pairs"
    )

    # Convert to time values for gather plot markers
    psd_window_times: Dict[int, Tuple[float, float]] = {}
    for cpair_id, (left, right) in pair_window_bounds.items():
        t_start = time_axis[left] if left < len(time_axis) else time_axis[-1]
        t_end = time_axis[right] if right < len(time_axis) else time_axis[-1]
        psd_window_times[cpair_id] = (t_start, t_end)

    # Build index mapping: position in all_pair_ids -> eligible mask
    eligible_set = set(eligible_pair_ids)
    eligible_mask = np.array([cpid in eligible_set for cpid in all_pair_ids])
    n_eligible = len(eligible_pair_ids)

    # Build station -> eligible pair indices and pair IDs mappings
    station_pair_indices: Dict[str, set] = defaultdict(set)
    station_pair_ids: Dict[str, List[int]] = defaultdict(list)
    for elig_idx, cpair_id in enumerate(eligible_pair_ids):
        cp = cpair_by_id[cpair_id]
        station_pair_indices[cp.component_a.station].add(elig_idx)
        station_pair_indices[cp.component_b.station].add(elig_idx)
        station_pair_ids[cp.component_a.station].append(cpair_id)
        station_pair_ids[cp.component_b.station].append(cpair_id)

    # For each pair, map to the "other" station name given a reference station
    pair_other_station: Dict[int, Dict[str, str]] = defaultdict(dict)
    station_component_ids: Dict[str, set] = defaultdict(set)
    for cpair_id in eligible_pair_ids:
        cp = cpair_by_id[cpair_id]
        sta_a, sta_b = cp.component_a.station, cp.component_b.station
        pair_other_station[cpair_id][sta_a] = sta_b
        pair_other_station[cpair_id][sta_b] = sta_a
        station_component_ids[sta_a].add(cp.component_a.id)
        station_component_ids[sta_b].add(cp.component_b.id)

    # Select stations for gather plots (max 10 uniformly covering the area)
    all_station_names = sorted(station_pair_ids.keys())
    max_gather_stations = 10

    # Get station coordinates for map and selection
    station_coords: Dict[str, Tuple[float, float]] = {}
    for cpair_id in eligible_pair_ids:
        cp = cpair_by_id[cpair_id]
        for comp in (cp.component_a, cp.component_b):
            if comp.station not in station_coords:
                x = comp.x if comp.x is not None else comp.lon
                y = comp.y if comp.y is not None else comp.lat
                if x is not None and y is not None:
                    station_coords[comp.station] = (float(x), float(y))

    if plot_gathers_for_all or len(all_station_names) <= max_gather_stations:
        gather_stations = set(all_station_names)
    else:
        # Greedy farthest-point sampling for uniform coverage
        available = [s for s in all_station_names if s in station_coords]
        if len(available) <= max_gather_stations:
            gather_stations = set(all_station_names)
        else:
            coords_arr = np.array([station_coords[s] for s in available])
            selected_indices = [0]
            for _ in range(max_gather_stations - 1):
                sel_coords = coords_arr[selected_indices]
                dists = np.min(
                    np.linalg.norm(coords_arr[:, None, :] - sel_coords[None, :, :], axis=2),
                    axis=1,
                )
                dists[selected_indices] = -1
                selected_indices.append(int(np.argmax(dists)))
            gather_stations = {available[i] for i in selected_indices}
        logger.info(
            f"Convergence study: selected {len(gather_stations)}/{len(all_station_names)} "
            f"stations for gather plots: {sorted(gather_stations)}"
        )

    # Plot network map with highlighted gather stations
    if station_coords:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 8))
        all_x = [station_coords[s][0] for s in all_station_names if s in station_coords]
        all_y = [station_coords[s][1] for s in all_station_names if s in station_coords]
        all_names = [s for s in all_station_names if s in station_coords]
        ax.scatter(all_x, all_y, marker="^", c="grey", s=60, zorder=2, label="All stations")
        # Highlight selected gather stations
        sel_x = [station_coords[s][0] for s in all_names if s in gather_stations]
        sel_y = [station_coords[s][1] for s in all_names if s in gather_stations]
        sel_names = [s for s in all_names if s in gather_stations]
        ax.scatter(sel_x, sel_y, marker="^", c="red", s=100, zorder=3, label=f"Gather stations ({len(sel_names)})")
        for sx, sy, sn in zip(sel_x, sel_y, sel_names):
            ax.annotate(sn, (sx, sy), textcoords="offset points", xytext=(4, 4), fontsize=7, color="red")
        for ax_x, ax_y, an in zip(all_x, all_y, all_names):
            if an not in gather_stations:
                ax.annotate(
                    an, (ax_x, ax_y), textcoords="offset points", xytext=(4, 4), fontsize=6, color="grey", alpha=0.7
                )
        ax.set_xlabel("X (m)" if cpair_by_id[eligible_pair_ids[0]].component_a.x is not None else "Longitude")
        ax.set_ylabel("Y (m)" if cpair_by_id[eligible_pair_ids[0]].component_a.y is not None else "Latitude")
        ax.set_title(f"Station network ({len(all_names)} stations)")
        ax.legend(loc="upper right", fontsize=8)
        ax.set_aspect("equal")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        map_path = conv_dir / "station_map.png"
        fig.savefig(str(map_path), dpi=150)
        plt.close(fig)
        logger.info(f"Convergence study: station map saved to {map_path}")

    # Generate plots per frequency band
    for b_idx, (_f_low, _f_high) in enumerate(bands if not _skip_other_plots else []):
        band_label = band_labels[b_idx]
        band_dir = conv_dir / band_label
        band_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Convergence study: generating plots for band {band_label}")

        per_pair_rms = per_band_rms[b_idx][eligible_mask, :]
        increment_stacks = per_band_increment_stacks[b_idx]

        # Global RMS: RMS of raw per-pair values at each increment
        global_rms = []
        for inc_idx in range(n_increments):
            vals = per_pair_rms[:, inc_idx]
            valid = vals[~np.isnan(vals)]
            if len(valid) > 0:
                global_rms.append(np.sqrt(np.mean(valid**2)))
            else:
                global_rms.append(float("nan"))

        # Normalized RMS for per-station-excluded comparison
        with np.errstate(invalid="ignore"):
            pair_mean_rms = np.nanmean(per_pair_rms, axis=1)
        pair_mean_rms[pair_mean_rms == 0] = np.nan
        normalized_rms = per_pair_rms / pair_mean_rms[:, np.newaxis]

        plot_durations = cumulative_durations[1:]
        plot_global_rms = global_rms[1:]
        plot_per_pair_rms = per_pair_rms[:, 1:]

        plot_convergence_global(
            plot_durations,
            plot_global_rms,
            band_dir / "convergence_global_rms.png",
            per_pair_rms=plot_per_pair_rms,
        )

        # Per-station-excluded global RMS (optional)
        if per_station_plots:
            n_stations = len(station_pair_indices)
            for sta_i, (excluded_station, excluded_indices) in enumerate(sorted(station_pair_indices.items())):
                kept_mask = np.ones(n_eligible, dtype=bool)
                kept_mask[list(excluded_indices)] = False
                if not np.any(kept_mask):
                    continue
                subset_rms = normalized_rms[kept_mask, :]
                subset_raw_rms = per_pair_rms[kept_mask, :]
                station_global_rms = []
                for inc_idx in range(n_increments):
                    vals = subset_rms[:, inc_idx]
                    valid = vals[~np.isnan(vals)]
                    if len(valid) > 0:
                        station_global_rms.append(np.sqrt(np.mean(valid**2)))
                    else:
                        station_global_rms.append(float("nan"))
                plot_convergence_global(
                    cumulative_durations[1:],
                    station_global_rms[1:],
                    band_dir / f"convergence_global_rms_without_{excluded_station}.png",
                    per_pair_rms=subset_raw_rms[:, 1:],
                )
                logger.info(f"  per-station RMS plot {sta_i + 1}/{n_stations}: without {excluded_station}")

        if plot_pcolors:
            logger.info(f"Convergence study: plotting pcolor for band {band_label}")
            plot_convergence_pcolor(
                plot_durations,
                plot_per_pair_rms,
                band_dir / "convergence_per_pair_rms.png",
                pair_labels=eligible_pair_labels,
            )

        # Gather plots per reference station, per increment
        if not skip_gathers:
            gather_dir = band_dir / "gathers"
            gather_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Convergence study: generating gather plots for {len(station_pair_ids)} stations")

            from matplotlib.backends.backend_pdf import PdfPages
            import matplotlib.image as mpimg
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            for sta_i, (ref_station, ref_pair_ids) in enumerate(sorted(station_pair_ids.items())):
                if ref_station not in gather_stations:
                    continue
                ref_pair_set = set(ref_pair_ids)
                station_gather_dir = gather_dir / ref_station
                station_gather_dir.mkdir(parents=True, exist_ok=True)
                station_png_paths = []
                logger.info(f"  gathers station {sta_i + 1}/{len(station_pair_ids)}: {ref_station}")

                for inc_idx, _end_pos in enumerate(increment_boundaries):
                    cumul_days = cumulative_durations[inc_idx]
                    stacks_at_inc = increment_stacks.get(inc_idx, {})
                    station_stacks = {pid: s for pid, s in stacks_at_inc.items() if pid in ref_pair_set}
                    if not station_stacks:
                        continue
                    gather_path = station_gather_dir / f"gather_increment_{inc_idx + 1:03d}_{cumul_days:.0f}days.png"
                    trace_labels = {pid: pair_other_station[pid].get(ref_station, "") for pid in station_stacks}
                    plot_convergence_gather(
                        time_axis=time_axis,
                        stacks=station_stacks,
                        pair_distances=pair_distances,
                        pair_labels=pair_labels,
                        cumul_days=cumul_days,
                        output_path=gather_path,
                        psd_window_times=psd_window_times,
                        pair_trace_labels=trace_labels,
                        ref_station=ref_station,
                    )
                    station_png_paths.append(gather_path)

                if station_png_paths:
                    pdf_path = station_gather_dir / f"gathers_{ref_station}_{pdf_conv_suffix}_{band_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for png_path in station_png_paths:
                            img = mpimg.imread(str(png_path))
                            fig, ax = plt.subplots(figsize=(8, 8))
                            ax.imshow(img)
                            ax.axis("off")
                            fig.tight_layout(pad=0)
                            pdf.savefig(fig, dpi=150)
                            plt.close(fig)

            logger.info(f"Gather plots per station for band {band_label} saved to {gather_dir}")
        else:
            logger.info(f"Convergence study: skipping gather plots for band {band_label}")

        logger.info(f"Convergence plots for band {band_label} saved to {band_dir}")

    del per_band_rms, per_band_increment_stacks

    # PSD plots on unfiltered (broadband) stacks
    from noiz.processing.stacking import (
        plot_convergence_psd_gather,
        plot_convergence_psd_pcolor,
        plot_convergence_psd_stats,
    )
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    from matplotlib.backends.backend_pdf import PdfPages

    psd_dir = conv_dir / "psd"
    psd_dir.mkdir(parents=True, exist_ok=True)

    # PSD gathers per station
    if not skip_psd_gathers:
        psd_gather_dir = psd_dir / "gathers"
        psd_gather_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Convergence study: generating PSD gather plots for {len(station_pair_ids)} stations")
        for sta_i, (ref_station, ref_pair_ids) in enumerate(sorted(station_pair_ids.items())):
            if ref_station not in gather_stations:
                continue
            ref_pair_set = set(ref_pair_ids)
            station_psd_dir = psd_gather_dir / ref_station
            station_psd_dir.mkdir(parents=True, exist_ok=True)
            station_png_paths = []
            logger.info(f"  PSD gathers station {sta_i + 1}/{len(station_pair_ids)}: {ref_station}")

            for inc_idx in range(n_increments):
                cumul_days = cumulative_durations[inc_idx]
                stacks_at_inc = unfiltered_increment_stacks.get(inc_idx, {})
                station_stacks = {pid: s for pid, s in stacks_at_inc.items() if pid in ref_pair_set}
                if not station_stacks:
                    continue
                png_path = station_psd_dir / f"psd_gather_{inc_idx + 1:03d}_{cumul_days:.0f}days.png"
                plot_convergence_psd_gather(
                    stacks=station_stacks,
                    pair_distances=pair_distances,
                    pair_labels=pair_labels,
                    sampling_rate=sampling_rate,
                    f_low=nominal_low,
                    f_high=nominal_high,
                    cumul_days=cumul_days,
                    output_path=png_path,
                    pair_window_bounds=pair_window_bounds,
                )
                station_png_paths.append(png_path)

            if station_png_paths:
                pdf_path = station_psd_dir / f"psd_gathers_{ref_station}_{pdf_conv_suffix}_{band_label}.pdf"
                with PdfPages(str(pdf_path)) as pdf:
                    for png_path in station_png_paths:
                        img = mpimg.imread(str(png_path))
                        fig, ax = plt.subplots(figsize=(8, 8))
                        ax.imshow(img)
                        ax.axis("off")
                        fig.tight_layout(pad=0)
                        pdf.savefig(fig, dpi=150)
                        plt.close(fig)

        logger.info(f"PSD gather plots saved to {psd_gather_dir}")
    else:
        logger.info("Convergence study: skipping PSD gather plots")

    # PSD pcolor (all pairs, per increment)
    if plot_pcolors:
        psd_pcolor_dir = psd_dir / "pcolor"
        psd_pcolor_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Convergence study: generating PSD pcolor plots for {n_increments} increments")
        pcolor_png_paths = []
        for inc_idx in range(n_increments):
            cumul_days = cumulative_durations[inc_idx]
            stacks_at_inc = unfiltered_increment_stacks.get(inc_idx, {})
            if not stacks_at_inc:
                continue
            png_path = psd_pcolor_dir / f"psd_pcolor_{inc_idx + 1:03d}_{cumul_days:.0f}days.png"
            plot_convergence_psd_pcolor(
                stacks=stacks_at_inc,
                eligible_pair_ids=eligible_pair_ids,
                pair_labels=eligible_pair_labels,
                sampling_rate=sampling_rate,
                f_low=nominal_low,
                f_high=nominal_high,
                cumul_days=cumul_days,
                output_path=png_path,
                pair_window_bounds=pair_window_bounds,
            )
            pcolor_png_paths.append(png_path)

        if pcolor_png_paths:
            pdf_path = psd_pcolor_dir / f"psd_pcolor_{pdf_conv_suffix}_{band_label}.pdf"
            with PdfPages(str(pdf_path)) as pdf:
                for png_path in pcolor_png_paths:
                    img = mpimg.imread(str(png_path))
                    fig, ax = plt.subplots(figsize=(10, 8))
                    ax.imshow(img)
                    ax.axis("off")
                    fig.tight_layout(pad=0)
                    pdf.savefig(fig, dpi=150)
                    plt.close(fig)

        logger.info(f"PSD pcolor plots saved to {psd_pcolor_dir}")

        # PSD pcolor normalized (each pair relative to its max)
        psd_pcolor_norm_dir = psd_dir / "pcolor_normalized"
        psd_pcolor_norm_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Convergence study: generating normalized PSD pcolor plots for {n_increments} increments")
        pcolor_norm_png_paths = []
        for inc_idx in range(n_increments):
            cumul_days = cumulative_durations[inc_idx]
            stacks_at_inc = unfiltered_increment_stacks.get(inc_idx, {})
            if not stacks_at_inc:
                continue
            png_path = psd_pcolor_norm_dir / f"psd_pcolor_norm_{inc_idx + 1:03d}_{cumul_days:.0f}days.png"
            plot_convergence_psd_pcolor(
                stacks=stacks_at_inc,
                eligible_pair_ids=eligible_pair_ids,
                pair_labels=eligible_pair_labels,
                sampling_rate=sampling_rate,
                f_low=nominal_low,
                f_high=nominal_high,
                cumul_days=cumul_days,
                output_path=png_path,
                pair_window_bounds=pair_window_bounds,
                normalize_rows=True,
            )
            pcolor_norm_png_paths.append(png_path)

        if pcolor_norm_png_paths:
            pdf_path = psd_pcolor_norm_dir / f"psd_pcolor_normalized_{pdf_conv_suffix}_{band_label}.pdf"
            with PdfPages(str(pdf_path)) as pdf:
                for png_path in pcolor_norm_png_paths:
                    img = mpimg.imread(str(png_path))
                    fig, ax = plt.subplots(figsize=(10, 8))
                    ax.imshow(img)
                    ax.axis("off")
                    fig.tight_layout(pad=0)
                    pdf.savefig(fig, dpi=150)
                    plt.close(fig)

        logger.info(f"Normalized PSD pcolor plots saved to {psd_pcolor_norm_dir}")

    # PSD statistics (mean, median, Q10, Q90 per increment)
    psd_stats_dir = psd_dir / "stats"
    psd_stats_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Convergence study: generating PSD statistics plots for {n_increments} increments")
    stats_png_paths = []
    for inc_idx in range(n_increments if not _skip_other_plots else 0):
        cumul_days = cumulative_durations[inc_idx]
        stacks_at_inc = unfiltered_increment_stacks.get(inc_idx, {})
        if not stacks_at_inc:
            continue
        png_path = psd_stats_dir / f"psd_stats_{inc_idx + 1:03d}_{cumul_days:.0f}days.png"
        plot_convergence_psd_stats(
            stacks=stacks_at_inc,
            eligible_pair_ids=eligible_pair_ids,
            sampling_rate=sampling_rate,
            f_low=nominal_low,
            f_high=nominal_high,
            cumul_days=cumul_days,
            output_path=png_path,
            pair_window_bounds=pair_window_bounds,
        )
        stats_png_paths.append(png_path)

    if stats_png_paths:
        pdf_path = psd_stats_dir / f"psd_stats_{pdf_conv_suffix}_{band_label}.pdf"
        with PdfPages(str(pdf_path)) as pdf:
            for png_path in stats_png_paths:
                img = mpimg.imread(str(png_path))
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.imshow(img)
                ax.axis("off")
                fig.tight_layout(pad=0)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)

    logger.info(f"PSD statistics plots saved to {psd_stats_dir}")

    # ========== STABILITY STUDY ==========
    if not skip_stability:
        logger.info("Convergence study: starting stability analysis")

        # Define running windows in terms of increment indices
        window_n_increments = max(1, int(round(stability_window_days * 24.0 / increment_hours)))
        step_n_increments = max(1, int(round(window_n_increments * (1.0 - stability_overlap))))
        window_starts = list(range(0, n_increments - window_n_increments + 1, step_n_increments))
        n_windows = len(window_starts)
        if n_windows < 2:
            logger.warning(
                f"Stability: only {n_windows} window(s) with {stability_window_days}d window "
                f"and {stability_overlap:.0%} overlap over {n_increments} increments. Skipping."
            )
        else:
            logger.info(
                f"Stability: {n_windows} windows of {window_n_increments} increments "
                f"(step {step_n_increments}), window={stability_window_days}d, overlap={stability_overlap:.0%}"
            )

            # Window center durations (calendar days)
            window_center_durations = []
            for ws in window_starts:
                center_idx = ws + window_n_increments // 2
                window_center_durations.append(cumulative_durations[min(center_idx, n_increments - 1)])

            # Compute windowed stacks from per-increment sums/counts
            stability_stacks: Dict[int, Dict[int, np.ndarray]] = defaultdict(dict)  # [win_idx][cpair_id]
            for win_idx, ws in enumerate(window_starts):
                we = ws + window_n_increments
                for cpair_id in all_pair_ids:
                    win_sum = None
                    win_count = 0
                    for inc_idx in range(ws, we):
                        if cpair_id in increment_sums[inc_idx]:
                            if win_sum is None:
                                win_sum = increment_sums[inc_idx][cpair_id].copy()
                            else:
                                win_sum += increment_sums[inc_idx][cpair_id]
                            win_count += increment_counts[inc_idx][cpair_id]
                    if win_sum is not None and win_count > 0:
                        stability_stacks[win_idx][cpair_id] = win_sum / win_count

            # Apply spectral whitening to stability stacks if requested
            if notch_whitening:
                logger.info(f"Applying notch whitening in {len(notch_whitening)} band(s) to stability stacks")
                for win_idx in range(n_windows):
                    for cpair_id in list(stability_stacks[win_idx].keys()):
                        stack = stability_stacks[win_idx][cpair_id]
                        stability_stacks[win_idx][cpair_id] = spectral_whitening(
                            stack, sampling_rate, whiten_bands=notch_whitening, notch_factor=notch_factor
                        )

            # Per-band RMS and filtered stacks for stability
            stab_per_band_rms: Dict[int, np.ndarray] = {
                b_idx: np.full((n_pairs, n_windows), np.nan) for b_idx in range(len(bands))
            }
            stab_per_band_stacks: Dict[int, Dict[int, Dict[int, np.ndarray]]] = {
                b_idx: defaultdict(dict) for b_idx in range(len(bands))
            }

            for b_idx, (f_low, f_high) in enumerate(bands if not only_psd_vs_raw else []):
                is_nominal = len(bands) == 1 and frequency_bands is None
                if not is_nominal:
                    logger.info(f"Stability: filtering band {band_labels[b_idx]}")

                for pair_idx, cpair_id in enumerate(all_pair_ids):
                    prev_stack = None
                    for win_idx in range(n_windows):
                        if cpair_id not in stability_stacks[win_idx]:
                            continue
                        raw_stack = stability_stacks[win_idx][cpair_id]
                        if is_nominal:
                            current_stack = raw_stack
                        else:
                            current_stack = bandpass_filter_ccf(raw_stack, sampling_rate, f_low, f_high)
                        # Store unnormalized for PSD/gather plots
                        stab_per_band_stacks[b_idx][win_idx][cpair_id] = current_stack
                        # Normalize for shape-based RMS comparison only
                        max_abs = np.max(np.abs(current_stack))
                        if max_abs > 0:
                            norm_stack = current_stack / max_abs
                        else:
                            norm_stack = current_stack
                        if prev_stack is not None:
                            rms = compute_rms_relative_difference(norm_stack, prev_stack)
                            stab_per_band_rms[b_idx][pair_idx, win_idx] = rms
                        prev_stack = norm_stack

            # PSD window bounds for stability (from last window)
            last_win_stacks = stability_stacks.get(n_windows - 1, {})
            stab_pair_window_bounds: Dict[int, Tuple[int, int]] = {}
            for cpair_id, stack in last_win_stacks.items():
                stab_pair_window_bounds[cpair_id] = _get_window_bounds(stack, sampling_rate, nominal_low)
            stab_psd_window_times: Dict[int, Tuple[float, float]] = {}
            for cpair_id, (left, right) in stab_pair_window_bounds.items():
                t_start = time_axis[left] if left < len(time_axis) else time_axis[-1]
                t_end = time_axis[right] if right < len(time_axis) else time_axis[-1]
                stab_psd_window_times[cpair_id] = (t_start, t_end)

            # Generate stability plots per frequency band
            for b_idx, (_f_low, _f_high) in enumerate(bands if not _skip_other_plots else []):
                band_label = band_labels[b_idx]
                stab_band_dir = stab_dir / band_label
                stab_band_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Stability: generating plots for band {band_label}")

                stab_rms = stab_per_band_rms[b_idx][eligible_mask, :]
                stab_stacks = stab_per_band_stacks[b_idx]

                # Global RMS
                stab_global_rms = []
                for win_idx in range(n_windows):
                    vals = stab_rms[:, win_idx]
                    valid = vals[~np.isnan(vals)]
                    if len(valid) > 0:
                        stab_global_rms.append(np.sqrt(np.mean(valid**2)))
                    else:
                        stab_global_rms.append(float("nan"))

                plot_durations = window_center_durations[1:]
                plot_global_rms = stab_global_rms[1:]
                plot_per_pair_rms = stab_rms[:, 1:]

                plot_convergence_global(
                    plot_durations,
                    plot_global_rms,
                    stab_band_dir / "stability_global_rms.png",
                    per_pair_rms=plot_per_pair_rms,
                    title=f"Stability - {stability_window_days:.0f}d window, {stability_overlap:.0%} overlap",
                )

                # Per-station excluded RMS (optional)
                if per_station_plots:
                    with np.errstate(invalid="ignore"):
                        stab_pair_mean = np.nanmean(stab_rms, axis=1)
                    stab_pair_mean[stab_pair_mean == 0] = np.nan
                    stab_norm_rms = stab_rms / stab_pair_mean[:, np.newaxis]
                    for excluded_station, excluded_indices in sorted(station_pair_indices.items()):
                        kept_mask_sta = np.ones(n_eligible, dtype=bool)
                        kept_mask_sta[list(excluded_indices)] = False
                        if not np.any(kept_mask_sta):
                            continue
                        subset_rms = stab_norm_rms[kept_mask_sta, :]
                        subset_raw = stab_rms[kept_mask_sta, :]
                        sta_global = []
                        for win_idx in range(n_windows):
                            vals = subset_rms[:, win_idx]
                            valid = vals[~np.isnan(vals)]
                            sta_global.append(np.sqrt(np.mean(valid**2)) if len(valid) > 0 else float("nan"))
                        plot_convergence_global(
                            window_center_durations[1:],
                            sta_global[1:],
                            stab_band_dir / f"stability_global_rms_without_{excluded_station}.png",
                            per_pair_rms=subset_raw[:, 1:],
                            title=f"Stability without {excluded_station} - {stability_window_days:.0f}d window",
                        )

                # Pcolor
                if plot_pcolors:
                    plot_convergence_pcolor(
                        plot_durations,
                        plot_per_pair_rms,
                        stab_band_dir / "stability_per_pair_rms.png",
                        pair_labels=eligible_pair_labels,
                    )

                # Gather plots
                if not skip_gathers:
                    stab_gather_dir = stab_band_dir / "gathers"
                    stab_gather_dir.mkdir(parents=True, exist_ok=True)
                    for ref_station, ref_pair_ids in sorted(station_pair_ids.items()):
                        if ref_station not in gather_stations:
                            continue
                        ref_pair_set = set(ref_pair_ids)
                        sta_gather_dir = stab_gather_dir / ref_station
                        sta_gather_dir.mkdir(parents=True, exist_ok=True)
                        sta_pngs = []
                        for win_idx in range(n_windows):
                            cumul_days = window_center_durations[win_idx]
                            stacks_at_win = stab_stacks.get(win_idx, {})
                            station_stacks = {pid: s for pid, s in stacks_at_win.items() if pid in ref_pair_set}
                            if not station_stacks:
                                continue
                            gpath = sta_gather_dir / f"gather_window_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                            trace_labels = {
                                pid: pair_other_station[pid].get(ref_station, "") for pid in station_stacks
                            }
                            plot_convergence_gather(
                                time_axis=time_axis,
                                stacks=station_stacks,
                                pair_distances=pair_distances,
                                pair_labels=pair_labels,
                                cumul_days=cumul_days,
                                output_path=gpath,
                                psd_window_times=stab_psd_window_times,
                                title=f"Stability - {stability_window_days:.0f}d window, center={cumul_days:.1f}d",
                                pair_trace_labels=trace_labels,
                                ref_station=ref_station,
                            )
                            sta_pngs.append(gpath)
                        if sta_pngs:
                            pdf_path = sta_gather_dir / f"gathers_{ref_station}_{pdf_stab_suffix}_{band_label}.pdf"
                            with PdfPages(str(pdf_path)) as pdf:
                                for png_path in sta_pngs:
                                    img = mpimg.imread(str(png_path))
                                    fig, ax = plt.subplots(figsize=(8, 8))
                                    ax.imshow(img)
                                    ax.axis("off")
                                    fig.tight_layout(pad=0)
                                    pdf.savefig(fig, dpi=150)
                                    plt.close(fig)

                logger.info(f"Stability plots for band {band_label} saved to {stab_band_dir}")

            del stab_per_band_rms

            # PSD plots for stability
            stab_psd_dir = stab_dir / "psd"
            stab_psd_dir.mkdir(parents=True, exist_ok=True)

            # PSD gathers per station
            if not skip_psd_gathers:
                stab_psd_gather_dir = stab_psd_dir / "gathers"
                stab_psd_gather_dir.mkdir(parents=True, exist_ok=True)
                for ref_station, ref_pair_ids in sorted(station_pair_ids.items()):
                    if ref_station not in gather_stations:
                        continue
                    ref_pair_set = set(ref_pair_ids)
                    sta_psd_dir = stab_psd_gather_dir / ref_station
                    sta_psd_dir.mkdir(parents=True, exist_ok=True)
                    sta_pngs = []
                    for win_idx in range(n_windows):
                        cumul_days = window_center_durations[win_idx]
                        stacks_at_win = stability_stacks.get(win_idx, {})
                        station_stacks = {pid: s for pid, s in stacks_at_win.items() if pid in ref_pair_set}
                        if not station_stacks:
                            continue
                        png_path = sta_psd_dir / f"psd_gather_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                        plot_convergence_psd_gather(
                            stacks=station_stacks,
                            pair_distances=pair_distances,
                            pair_labels=pair_labels,
                            sampling_rate=sampling_rate,
                            f_low=nominal_low,
                            f_high=nominal_high,
                            cumul_days=cumul_days,
                            output_path=png_path,
                            pair_window_bounds=stab_pair_window_bounds,
                        )
                        sta_pngs.append(png_path)
                    if sta_pngs:
                        nominal_label = f"{nominal_low:.3g}-{nominal_high:.3g}Hz"
                        pdf_path = sta_psd_dir / f"psd_gathers_{ref_station}_{pdf_stab_suffix}_{nominal_label}.pdf"
                        with PdfPages(str(pdf_path)) as pdf:
                            for png_path in sta_pngs:
                                img = mpimg.imread(str(png_path))
                                fig, ax = plt.subplots(figsize=(8, 8))
                                ax.imshow(img)
                                ax.axis("off")
                                fig.tight_layout(pad=0)
                                pdf.savefig(fig, dpi=150)
                                plt.close(fig)

            # PSD pcolor
            if plot_pcolors:
                stab_pcolor_dir = stab_psd_dir / "pcolor"
                stab_pcolor_dir.mkdir(parents=True, exist_ok=True)
                stab_pcolor_pngs = []
                for win_idx in range(n_windows):
                    cumul_days = window_center_durations[win_idx]
                    stacks_at_win = stability_stacks.get(win_idx, {})
                    if not stacks_at_win:
                        continue
                    png_path = stab_pcolor_dir / f"psd_pcolor_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                    plot_convergence_psd_pcolor(
                        stacks=stacks_at_win,
                        eligible_pair_ids=eligible_pair_ids,
                        pair_labels=eligible_pair_labels,
                        sampling_rate=sampling_rate,
                        f_low=nominal_low,
                        f_high=nominal_high,
                        cumul_days=cumul_days,
                        output_path=png_path,
                        pair_window_bounds=stab_pair_window_bounds,
                    )
                    stab_pcolor_pngs.append(png_path)
                if stab_pcolor_pngs:
                    nominal_label = f"{nominal_low:.3g}-{nominal_high:.3g}Hz"
                    pdf_path = stab_pcolor_dir / f"psd_pcolor_{pdf_stab_suffix}_{nominal_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for png_path in stab_pcolor_pngs:
                            img = mpimg.imread(str(png_path))
                            fig, ax = plt.subplots(figsize=(10, 8))
                            ax.imshow(img)
                            ax.axis("off")
                            fig.tight_layout(pad=0)
                            pdf.savefig(fig, dpi=150)
                            plt.close(fig)

                # PSD pcolor normalized
                stab_pcolor_norm_dir = stab_psd_dir / "pcolor_normalized"
                stab_pcolor_norm_dir.mkdir(parents=True, exist_ok=True)
                stab_pcolor_norm_pngs = []
                for win_idx in range(n_windows):
                    cumul_days = window_center_durations[win_idx]
                    stacks_at_win = stability_stacks.get(win_idx, {})
                    if not stacks_at_win:
                        continue
                    png_path = stab_pcolor_norm_dir / f"psd_pcolor_norm_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                    plot_convergence_psd_pcolor(
                        stacks=stacks_at_win,
                        eligible_pair_ids=eligible_pair_ids,
                        pair_labels=eligible_pair_labels,
                        sampling_rate=sampling_rate,
                        f_low=nominal_low,
                        f_high=nominal_high,
                        cumul_days=cumul_days,
                        output_path=png_path,
                        pair_window_bounds=stab_pair_window_bounds,
                        normalize_rows=True,
                    )
                    stab_pcolor_norm_pngs.append(png_path)
                if stab_pcolor_norm_pngs:
                    nominal_label = f"{nominal_low:.3g}-{nominal_high:.3g}Hz"
                    pdf_path = stab_pcolor_norm_dir / f"psd_pcolor_normalized_{pdf_stab_suffix}_{nominal_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for png_path in stab_pcolor_norm_pngs:
                            img = mpimg.imread(str(png_path))
                            fig, ax = plt.subplots(figsize=(10, 8))
                            ax.imshow(img)
                            ax.axis("off")
                            fig.tight_layout(pad=0)
                            pdf.savefig(fig, dpi=150)
                            plt.close(fig)

            # PSD statistics
            stab_stats_dir = stab_psd_dir / "stats"
            stab_stats_dir.mkdir(parents=True, exist_ok=True)
            stab_stats_pngs = []
            for win_idx in range(n_windows if not _skip_other_plots else 0):
                cumul_days = window_center_durations[win_idx]
                stacks_at_win = stability_stacks.get(win_idx, {})
                if not stacks_at_win:
                    continue
                png_path = stab_stats_dir / f"psd_stats_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                plot_convergence_psd_stats(
                    stacks=stacks_at_win,
                    eligible_pair_ids=eligible_pair_ids,
                    sampling_rate=sampling_rate,
                    f_low=nominal_low,
                    f_high=nominal_high,
                    cumul_days=cumul_days,
                    output_path=png_path,
                    pair_window_bounds=stab_pair_window_bounds,
                )
                stab_stats_pngs.append(png_path)
            if stab_stats_pngs:
                nominal_label = f"{nominal_low:.3g}-{nominal_high:.3g}Hz"
                pdf_path = stab_stats_dir / f"psd_stats_{pdf_stab_suffix}_{nominal_label}.pdf"
                with PdfPages(str(pdf_path)) as pdf:
                    for png_path in stab_stats_pngs:
                        img = mpimg.imread(str(png_path))
                        fig, ax = plt.subplots(figsize=(10, 5))
                        ax.imshow(img)
                        ax.axis("off")
                        fig.tight_layout(pad=0)
                        pdf.savefig(fig, dpi=150)
                        plt.close(fig)

            # ---- PSD vs Raw PSD comparison ----
            from noiz.processing.stacking import plot_psd_vs_raw, _compute_psd_db
            from noiz.models import PPSDResult, PPSDParams, Datachunk

            psd_vs_raw_dir = stab_dir / "psd" / "study_vs_raw_psd"
            if not only_psd_vs_gathers:
                psd_vs_raw_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Stability: generating PSD vs raw PSD comparison plots")

            cached_raw_psd: Dict[Tuple[int, str], Tuple[Optional[np.ndarray], list]] = {}
            ppsd_freqs = np.array([])

            # Find available PPSDParams (use first one found)
            ppsd_params_obj = db.session.query(PPSDParams).first()
            if ppsd_params_obj is None:
                logger.warning("No PPSDParams found in DB; skipping PSD vs raw comparison.")
            else:
                ppsd_freqs = (
                    ppsd_params_obj.resampled_frequency_vector
                    if ppsd_params_obj.resample
                    else ppsd_params_obj.expected_fft_freq
                )

                # Map timespan_id -> starttime for date axis
                study_ts_starttime = {ts.id: ts.starttime for ts in study_timespans}

                # For each window, gather timespan IDs
                window_ts_ids: List[List[int]] = []
                for _win_idx, ws in enumerate(window_starts):
                    we = ws + window_n_increments
                    win_ts = []
                    for inc_idx in range(ws, we):
                        win_ts.extend(increment_ts_ids[inc_idx] if inc_idx < len(increment_ts_ids) else [])
                    window_ts_ids.append(win_ts)

                # Per-window, per-station plots
                all_win_sta_pngs: List[List[Tuple[str, str]]] = []  # [win_idx] -> [(station, png_path)]
                for win_idx in range(n_windows):
                    cumul_days = window_center_durations[win_idx]
                    win_label = f"window {win_idx + 1}, center={cumul_days:.1f}d"
                    ts_ids_in_win = window_ts_ids[win_idx]
                    stacks_at_win = stability_stacks.get(win_idx, {})
                    win_sta_pngs: List[Tuple[str, str]] = []

                    for ref_station in sorted(gather_stations):
                        sta_dir = psd_vs_raw_dir / ref_station
                        if not only_psd_vs_gathers:
                            sta_dir.mkdir(parents=True, exist_ok=True)

                        # Query raw PPSDs for this station's components in this window
                        comp_ids = list(station_component_ids.get(ref_station, set()))
                        raw_psd_matrix = None
                        raw_psd_dates = []
                        if comp_ids and ts_ids_in_win:
                            ppsd_results = (
                                db.session.query(PPSDResult)
                                .join(Datachunk, Datachunk.id == PPSDResult.datachunk_id)
                                .filter(
                                    PPSDResult.ppsd_params_id == ppsd_params_obj.id,
                                    Datachunk.component_id.in_(comp_ids),
                                    PPSDResult.timespan_id.in_(ts_ids_in_win),
                                )
                                .all()
                            )
                            if ppsd_results:
                                psd_rows = []
                                psd_dates_list = []
                                for pr in ppsd_results:
                                    try:
                                        data = pr.load_data()
                                        fft_mean = data["fft_mean"]
                                        psd_rows.append(fft_mean)
                                        psd_dates_list.append(
                                            study_ts_starttime.get(pr.timespan_id, pr.timespan.starttime)
                                        )
                                    except Exception:
                                        continue
                                if psd_rows:
                                    # Sort by date
                                    sort_idx = np.argsort(psd_dates_list)
                                    raw_psd_matrix = np.array(psd_rows)[sort_idx]
                                    raw_psd_dates = [psd_dates_list[i] for i in sort_idx]
                            db.session.expunge_all()

                        cached_raw_psd[(win_idx, ref_station)] = (raw_psd_matrix, raw_psd_dates)

                        # Compute CCF PSD stats for pairs involving this station
                        ref_pair_set = set(station_pair_ids.get(ref_station, []))
                        station_stacks_win = {pid: s for pid, s in stacks_at_win.items() if pid in ref_pair_set}
                        ccf_psd_freqs = np.array([])
                        ccf_psd_values = {}
                        if station_stacks_win and not only_psd_vs_gathers:
                            all_psd_db = []
                            for _pid, stack in station_stacks_win.items():
                                freqs_i, psd_db_i = _compute_psd_db(stack, sampling_rate, nominal_low, nominal_high)
                                all_psd_db.append(psd_db_i)
                                ccf_psd_freqs = freqs_i
                            if all_psd_db:
                                arr = np.array(all_psd_db)
                                with np.errstate(invalid="ignore"):
                                    ccf_psd_values = {
                                        "mean": np.nanmean(arr, axis=0),
                                        "median": np.nanmedian(arr, axis=0),
                                        "q10": np.nanpercentile(arr, 10, axis=0),
                                        "q90": np.nanpercentile(arr, 90, axis=0),
                                    }

                        if not only_psd_vs_gathers:
                            png_path = sta_dir / f"psd_vs_raw_{win_idx + 1:03d}_{cumul_days:.0f}days.png"
                            plot_psd_vs_raw(
                                raw_psd_matrix=raw_psd_matrix,
                                raw_psd_dates=raw_psd_dates,
                                raw_psd_freqs=ppsd_freqs,
                                ccf_psd_freqs=ccf_psd_freqs,
                                ccf_psd_values=ccf_psd_values,
                                station=ref_station,
                                window_label=win_label,
                                output_path=png_path,
                                f_low=nominal_low,
                                f_high=nominal_high,
                                notch_bands=notch_whitening or notch_cut or notch_interpolate,
                                notch_color="violet" if notch_interpolate else "red",
                            )
                            win_sta_pngs.append((ref_station, str(png_path)))
                    all_win_sta_pngs.append(win_sta_pngs)
                    if not only_psd_vs_gathers:
                        logger.info(f"  PSD vs raw: window {win_idx + 1}/{n_windows}")
                    elif win_idx == 0:
                        logger.info(f"  Caching raw PPSD for {n_windows} windows...")

                # Generate PDF: one page per window, all stations on that page
                if not only_psd_vs_gathers:
                    nominal_label = f"{nominal_low:.3g}-{nominal_high:.3g}Hz"
                    pdf_path = psd_vs_raw_dir / f"psd_vs_raw_{pdf_stab_suffix}_{nominal_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for win_idx, win_sta_pngs in enumerate(all_win_sta_pngs):
                            if not win_sta_pngs:
                                continue
                            n_sta = len(win_sta_pngs)
                            fig, axes = plt.subplots(n_sta, 1, figsize=(12, 4 * n_sta))
                            if n_sta == 1:
                                axes = [axes]
                            for ax, (_sta_name, png_p) in zip(axes, win_sta_pngs):
                                img = mpimg.imread(png_p)
                                ax.imshow(img)
                                ax.axis("off")
                            fig.suptitle(
                                f"Window {win_idx + 1} - center={window_center_durations[win_idx]:.1f}d", fontsize=11
                            )
                            fig.tight_layout(pad=0.5)
                            pdf.savefig(fig, dpi=100)
                            plt.close(fig)

                if not only_psd_vs_gathers:
                    logger.info(f"PSD vs raw comparison saved to {psd_vs_raw_dir}")

            # ---- Raw PSD vs band-filtered gathers ----
            if (not only_psd_vs_raw or only_psd_vs_gathers) and cached_raw_psd:
                from noiz.processing.stacking import plot_raw_psd_vs_gather, _compute_psd_db as _rpg_compute_psd_db

                logger.info("Stability: generating raw PSD vs gather plots per band")
                for b_idx, (f_low, f_high) in enumerate(bands):
                    band_label = band_labels[b_idx]
                    rpg_dir = stab_dir / band_label / "raw_psd_vs_gathers"
                    rpg_dir.mkdir(parents=True, exist_ok=True)

                    all_win_rpg_pngs: List[List[Tuple[str, str]]] = []
                    for win_idx in range(n_windows):
                        cumul_days = window_center_durations[win_idx]
                        win_label = f"window {win_idx + 1}, center={cumul_days:.1f}d"
                        win_rpg_pngs: List[Tuple[str, str]] = []

                        for ref_station in sorted(gather_stations):
                            raw_psd_matrix, raw_psd_dates = cached_raw_psd.get((win_idx, ref_station), (None, []))
                            ref_pair_set = set(station_pair_ids.get(ref_station, []))
                            band_stacks_win = {
                                pid: s
                                for pid, s in stab_per_band_stacks[b_idx].get(win_idx, {}).items()
                                if pid in ref_pair_set
                            }
                            trace_labels = {
                                pid: pair_other_station[pid].get(ref_station, "") for pid in band_stacks_win
                            }

                            sta_rpg_dir = rpg_dir / ref_station
                            sta_rpg_dir.mkdir(parents=True, exist_ok=True)
                            png_path = sta_rpg_dir / f"rpg_{win_idx + 1:03d}_{cumul_days:.0f}days.png"

                            # Compute CCF PSD stats on the band-filtered stacks
                            rpg_psd_freqs = np.array([])
                            rpg_psd_values = {}
                            if band_stacks_win:
                                all_psd_db = []
                                for _pid, stack in band_stacks_win.items():
                                    freqs_i, psd_db_i = _rpg_compute_psd_db(stack, sampling_rate, f_low, f_high)
                                    all_psd_db.append(psd_db_i)
                                    rpg_psd_freqs = freqs_i
                                if all_psd_db:
                                    arr = np.array(all_psd_db)
                                    with np.errstate(invalid="ignore"):
                                        rpg_psd_values = {
                                            "mean": np.nanmean(arr, axis=0),
                                            "median": np.nanmedian(arr, axis=0),
                                            "q10": np.nanpercentile(arr, 10, axis=0),
                                            "q90": np.nanpercentile(arr, 90, axis=0),
                                        }

                            plot_raw_psd_vs_gather(
                                raw_psd_matrix=raw_psd_matrix,
                                raw_psd_dates=raw_psd_dates,
                                raw_psd_freqs=ppsd_freqs,
                                time_axis=time_axis,
                                stacks=band_stacks_win,
                                pair_distances=pair_distances,
                                pair_trace_labels=trace_labels,
                                station=ref_station,
                                window_label=win_label,
                                output_path=png_path,
                                f_low=nominal_low,
                                f_high=nominal_high,
                                band_low=f_low,
                                band_high=f_high,
                                ccf_psd_freqs=rpg_psd_freqs,
                                ccf_psd_values=rpg_psd_values,
                                notch_bands=notch_whitening or notch_cut or notch_interpolate,
                                notch_color="violet" if notch_interpolate else "red",
                            )
                            win_rpg_pngs.append((ref_station, str(png_path)))
                        all_win_rpg_pngs.append(win_rpg_pngs)

                    # PDF per band
                    pdf_path = rpg_dir / f"raw_psd_vs_gather_{pdf_stab_suffix}_{band_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for win_idx, win_rpg_pngs in enumerate(all_win_rpg_pngs):
                            if not win_rpg_pngs:
                                continue
                            n_sta = len(win_rpg_pngs)
                            fig, axes = plt.subplots(n_sta, 1, figsize=(14, 5 * n_sta))
                            if n_sta == 1:
                                axes = [axes]
                            for ax, (_sta_name, png_p) in zip(axes, win_rpg_pngs):
                                img = mpimg.imread(png_p)
                                ax.imshow(img)
                                ax.axis("off")
                            fig.suptitle(
                                f"{band_label} - Window {win_idx + 1} - "
                                f"center={window_center_durations[win_idx]:.1f}d",
                                fontsize=11,
                            )
                            fig.tight_layout(pad=0.5)
                            pdf.savefig(fig, dpi=100)
                            plt.close(fig)
                    logger.info(f"  Raw PSD vs gather for band {band_label} saved to {rpg_dir}")

                del stab_per_band_stacks

            logger.info("Stability study plots saved")
            del stability_stacks
    else:
        logger.info("Convergence study: skipping stability analysis")

    # Plot final full-stack gathers (always, using last increment cumulative stacks)
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    from matplotlib.backends.backend_pdf import PdfPages

    last_inc_all_stacks = unfiltered_increment_stacks.get(n_increments - 1, {})
    if last_inc_all_stacks and not _skip_other_plots:
        total_days = cumulative_durations[-1]
        logger.info(f"Generating final full-stack gathers for {len(bands)} bands")
        for b_idx, (f_low, f_high) in enumerate(bands):
            band_label = band_labels[b_idx]
            logger.info(f"  Final gathers: band {band_label}")
            band_dir = conv_dir / band_label
            gather_dir = band_dir / "gathers"
            is_nominal = len(bands) == 1 and frequency_bands is None

            if is_nominal:
                final_stacks_for_band = last_inc_all_stacks
            else:
                final_stacks_for_band = {
                    pid: bandpass_filter_ccf(s, sampling_rate, f_low, f_high) for pid, s in last_inc_all_stacks.items()
                }

            for ref_station, ref_pair_ids in sorted(station_pair_ids.items()):
                if ref_station not in gather_stations:
                    continue
                ref_pair_set = set(ref_pair_ids)
                station_stacks = {pid: s for pid, s in final_stacks_for_band.items() if pid in ref_pair_set}
                if not station_stacks:
                    continue

                station_gather_dir = gather_dir / ref_station
                station_gather_dir.mkdir(parents=True, exist_ok=True)
                final_path = station_gather_dir / "gather_FINAL_full_stack.png"
                trace_labels = {pid: pair_other_station[pid].get(ref_station, "") for pid in station_stacks}
                plot_convergence_gather(
                    time_axis=time_axis,
                    stacks=station_stacks,
                    pair_distances=pair_distances,
                    pair_labels=pair_labels,
                    cumul_days=total_days,
                    output_path=final_path,
                    psd_window_times=psd_window_times,
                    pair_trace_labels=trace_labels,
                    ref_station=ref_station,
                )

                # Rebuild PDF with all PNGs including the final gather
                all_pngs = sorted(station_gather_dir.glob("gather_*.png"))
                if all_pngs:
                    pdf_path = station_gather_dir / f"gathers_{ref_station}_{pdf_conv_suffix}_{band_label}.pdf"
                    with PdfPages(str(pdf_path)) as pdf:
                        for png_path in all_pngs:
                            img = mpimg.imread(str(png_path))
                            fig, ax = plt.subplots(figsize=(8, 8))
                            ax.imshow(img)
                            ax.axis("off")
                            fig.tight_layout(pad=0)
                            pdf.savefig(fig, dpi=150)
                            plt.close(fig)

        logger.info("Final full-stack gathers saved")

    # Pre-compute final stacks and total CCF counts before freeing memory
    final_full_stacks: Dict[int, np.ndarray] = {}
    pair_total_ccf_counts: Dict[int, int] = {}
    if not _skip_other_plots:
        for cpair_id in all_pair_ids:
            if cpair_id in unfiltered_increment_stacks.get(n_increments - 1, {}):
                final_full_stacks[cpair_id] = unfiltered_increment_stacks[n_increments - 1][cpair_id]
            total_count = sum(increment_counts[inc_idx].get(cpair_id, 0) for inc_idx in range(n_increments))
            if total_count > 0:
                pair_total_ccf_counts[cpair_id] = total_count

    # Export convergence H5 per frequency sub-band
    if final_full_stacks:
        from noiz.processing.h5_export import export_convergence_stacks_to_h5

        component_pairs_for_h5 = {
            cpid: (cpair_by_id[cpid].component_a, cpair_by_id[cpid].component_b)
            for cpid in final_full_stacks
            if cpid in cpair_by_id
        }
        h5_dir = param_dir / "h5_outputs"
        # Base tag matching the convergence folder naming
        h5_base_tag = f"{inc_label}_{day_start}_{day_stop}{suffix_tag}"

        for b_idx, (f_low, f_high) in enumerate(bands):
            band_label = band_labels[b_idx]
            is_nominal = len(bands) == 1 and frequency_bands is None
            if is_nominal:
                band_stacks = final_full_stacks
            else:
                band_stacks = {
                    pid: bandpass_filter_ccf(s, sampling_rate, f_low, f_high) for pid, s in final_full_stacks.items()
                }
            h5_path = h5_dir / f"convergence_{band_label}_{h5_base_tag}.h5"
            export_convergence_stacks_to_h5(
                stacks=band_stacks,
                time_axis=time_axis,
                fmin=f_low,
                fmax=f_high,
                component_pairs=component_pairs_for_h5,
                output_path=h5_path,
            )
        logger.info(f"Convergence H5 files exported to {h5_dir}")

    del unfiltered_increment_stacks, increment_sums, increment_counts

    # Upsert final stacks to DB using pre-computed stacks
    if _skip_other_plots:
        logger.info("Only-mode active: skipping DB write")
        return eligible_pair_ids

    logger.info(f"Convergence study: preparing DB upsert for {len(final_full_stacks)} stacks")
    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema_id,
        starttime=starttime,
        endtime=endtime,
    )
    if not stacking_timespans:
        logger.warning("No stacking timespans found; cannot save convergence stacks to DB.")
        return eligible_pair_ids

    # Check for existing stacks; skip unless overwrite requested
    existing_count = (
        db.session.query(CCFStack)
        .filter(
            CCFStack.stacking_schema_id == stacking_schema.id,
            CCFStack.stacking_timespan_id.in_([st.id for st in stacking_timespans]),
        )
        .count()
    )
    if existing_count > 0 and not overwrite_stacks:
        logger.info(
            f"Convergence study: {existing_count} stacks already exist in DB, skipping write. "
            f"Use --overwrite_stacks to overwrite."
        )
        return eligible_pair_ids
    if existing_count > 0:
        logger.info(f"Convergence study: overwriting {existing_count} existing stacks in DB.")

    saved_count = 0

    for stacking_timespan in stacking_timespans:
        stack_objs = []
        for cpair_id, stack_array in final_full_stacks.items():
            no_ccfs = pair_total_ccf_counts.get(cpair_id, 0)
            if no_ccfs < stacking_schema.minimum_ccf_count:
                continue
            stack_objs.append(
                CCFStack(
                    stacking_timespan_id=stacking_timespan.id,
                    stacking_schema_id=stacking_schema.id,
                    stack=numpy_to_python(stack_array),
                    componentpair_id=cpair_id,
                    no_ccfs=no_ccfs,
                )
            )
        if stack_objs:
            bulk_add_or_upsert_objects(
                objects_to_add=stack_objs,
                upserter_callable=_generate_ccfstack_upsert_command,
                bulk_insert=True,
            )
            saved_count += len(stack_objs)

    logger.info(f"Convergence study: saved {saved_count} final stacks to database")

    return eligible_pair_ids
