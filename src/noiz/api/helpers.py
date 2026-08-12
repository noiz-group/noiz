# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
import gc
import more_itertools
import pandas as pd
from time import sleep
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.orm import Query
from sqlalchemy.exc import IntegrityError, InvalidRequestError, NoInspectionAvailable
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.sql import Insert
from sqlalchemy import text, tuple_
from typing import Iterable, Union, List, Tuple, Any, Collection, Callable, get_args, Dict, TypeVar, Optional

from noiz.database import db
from noiz.exceptions import CorruptedDataException, InconsistentDataException, ObspyError
from noiz.models.type_aliases import BulkAddableObjects, InputsForMassCalculations, BulkAddableFileObjects


CCF_PREDELETE_CHUNK_SIZE = 1000


def extract_object_ids(
    instances: Iterable[Any],
) -> List[int]:
    """
    Extracts parameter .id from all provided instances of objects.
    It can either be a single object or iterable of them.

    :param instances: instances of objects to be checked
    :type instances:
    :return: ids of objects
    :rtype: List[int]
    """
    iterable_instances = _coerce_instances_to_iterable(instances)
    ids = [_extract_object_id(x) for x in iterable_instances]
    return ids


def extract_object_ids_keep_objects(
    instances: Iterable[Any],
) -> Dict[int, Any]:
    """
    Extracts parameter .id from all provided instances of objects.
    It can either be a single object or iterable of them.

    :param instances: instances of objects to be checked
    :type instances:
    :return: ids of objects
    :rtype: List[int]
    """
    iterable_instances = _coerce_instances_to_iterable(instances)
    return {_extract_object_id(x): x for x in iterable_instances}


def _coerce_instances_to_iterable(instances: Any) -> Iterable[Any]:
    if isinstance(instances, Iterable) and not isinstance(instances, (str, bytes)):
        return instances
    return (instances,)


def _extract_object_id(instance: Any) -> int:
    try:
        inspection = sqlalchemy_inspect(instance)
    except NoInspectionAvailable:
        return instance.id

    identity = getattr(inspection, "identity", None)
    if identity is not None and len(identity) > 0:
        return identity[0]

    return instance.id


def bulk_add_objects(objects_to_add: Collection[BulkAddableObjects]) -> None:
    """
    Tries to perform bulk insert of objects to database.

    :param objects_to_add: Objects to be inserted to the db
    :type objects_to_add: BulkAddableObjects
    :return: None
    :rtype: None
    """
    logger.debug("Performing bulk add_all operation")
    db.session.add_all(objects_to_add)
    logger.debug("Committing")
    db.session.commit()
    return


def bulk_merge_objects(objects_to_merge: Collection[BulkAddableObjects]) -> None:
    """
    Tries to perform bulk merge of objects to database.

    :param objects_to_merge: Objects to be inserted to the db
    :type objects_to_merge: BulkAddableObjects
    :return: None
    :rtype: None
    """
    logger.debug("Performing bulk merge operation")
    for ob in objects_to_merge:
        db.session.merge(ob)
    logger.debug("Committing")
    db.session.commit()
    return


def bulk_add_or_upsert_objects(
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]],
    upserter_callable: Callable[[BulkAddableObjects], Insert],
    bulk_insert: bool = True,
    overwrite: bool = True,
) -> None:
    """
    Adds in bulk or upserts provided Collection of objects to DB.

    This is the unified function that handles all bulk insert/upsert operations.
    It supports both the original behavior (when overwrite=True, default) and
    skip-existing behavior (when overwrite=False).

    For CrosscorrelationCartesian objects specifically:
    - overwrite=True: Pre-delete conflicting records before insert (fast)
    - overwrite=False: Filter out objects that already exist (skip existing)

    For other object types, the overwrite parameter is currently ignored and
    the original fallback-to-upsert behavior is used.

    :param objects_to_add: Objects to be added to database
    :type objects_to_add: Collection[BulkAddableObjects]
    :param upserter_callable: Callable with upsert method to be used in case of bulk add failure
    :type upserter_callable: Callable[[Collection[BulkAddableObjects]], None]
    :param bulk_insert: If bulk add should be even attempted
    :type bulk_insert: bool
    :param overwrite: If True (default), delete existing conflicting records before insert.
                      If False, filter out objects that already exist in DB (skip existing).
                      Only applicable to CrosscorrelationCartesian objects currently.
    :type overwrite: bool
    :return: None
    :rtype: NoneType
    """
    import time as time_module  # For timing debug

    if isinstance(objects_to_add, Collection):
        valid_objects = objects_to_add
    else:
        valid_objects = (objects_to_add,)

    if not valid_objects:
        logger.debug("No objects to add, returning early")
        return

    # Check if these are CrosscorrelationCartesian objects (have the specific attributes)
    first_obj = next(iter(valid_objects), None)
    is_ccf_cartesian_objects = (
        first_obj is not None
        and hasattr(first_obj, "timespan_id")
        and hasattr(first_obj, "componentpair_id")
        and hasattr(first_obj, "crosscorrelation_cartesian_params_id")
    )

    # Check if these are CrosscorrelationCylindrical objects
    is_ccf_cylindrical_objects = (
        first_obj is not None
        and hasattr(first_obj, "timespan_id")
        and hasattr(first_obj, "componentpair_cylindrical_id")
        and hasattr(first_obj, "crosscorrelation_cylindrical_params_id")
    )

    from noiz.models.beamforming import BeamformingResult as BeamformingResultModel

    is_beamforming_results = isinstance(first_obj, BeamformingResultModel)

    if bulk_insert:
        logger.debug("Trying to do bulk insert")

        # Apply CCF-specific pre-processing if applicable
        if is_ccf_cartesian_objects:
            if overwrite:
                # Pre-delete conflicting records to avoid slow upsert fallback
                t_start = time_module.time()
                _delete_conflicting_ccf_records(valid_objects)
                logger.debug(f"DEBUG_TIMING: _delete_conflicting_ccf_records took {time_module.time() - t_start:.2f}s")
            else:
                # Filter out objects that already exist in DB (skip existing)
                t_start = time_module.time()
                valid_objects = _filter_existing_ccf_records(valid_objects)
                logger.debug(f"DEBUG_TIMING: _filter_existing_ccf_records took {time_module.time() - t_start:.2f}s")
                if not valid_objects:
                    logger.info("All objects already exist in DB, skipping insert (overwrite=False)")
                    return
        elif is_ccf_cylindrical_objects:
            if overwrite:
                # Pre-delete conflicting cylindrical CCF records
                t_start = time_module.time()
                _delete_conflicting_ccf_cylindrical_records(valid_objects)
                logger.debug(
                    f"DEBUG_TIMING: _delete_conflicting_ccf_cylindrical_records took {time_module.time() - t_start:.2f}s"
                )
            else:
                # Filter out cylindrical CCF objects that already exist in DB
                t_start = time_module.time()
                valid_objects = _filter_existing_ccf_cylindrical_records(valid_objects)
                logger.debug(
                    f"DEBUG_TIMING: _filter_existing_ccf_cylindrical_records took {time_module.time() - t_start:.2f}s"
                )
                if not valid_objects:
                    logger.info("All cylindrical CCF objects already exist in DB, skipping insert (overwrite=False)")
                    return
        elif is_beamforming_results:
            if overwrite:
                t_start = time_module.time()
                _delete_conflicting_beamforming_records(valid_objects)
                logger.debug(
                    f"DEBUG_TIMING: _delete_conflicting_beamforming_records took {time_module.time() - t_start:.2f}s"
                )
            else:
                t_start = time_module.time()
                valid_objects = _filter_existing_beamforming_records(valid_objects)
                logger.debug(
                    f"DEBUG_TIMING: _filter_existing_beamforming_records took {time_module.time() - t_start:.2f}s"
                )
                if not valid_objects:
                    logger.info("All beamforming objects already exist in DB, skipping insert (overwrite=False)")
                    return

        try:
            t_start = time_module.time()
            bulk_add_objects(valid_objects)
            logger.debug(
                f"DEBUG_TIMING: bulk_add_objects took {time_module.time() - t_start:.2f}s for {len(valid_objects)} objects"
            )
        except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning(f"Retrying with upsert for {len(valid_objects)} objects...")
            _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
            logger.debug("Upsert completed successfully.")
    else:
        logger.info("Starting to perform careful upsert")
        _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    return


def _get_dask_worker_thread_env() -> Dict[str, str]:
    return {
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "VECLIB_MAXIMUM_THREADS": "1",
    }


def create_process_dask_client(n_workers: Optional[int] = None):
    from dask.distributed import Client, LocalCluster
    import os
    import multiprocessing

    logger.debug(f"os.cpu_count() = {os.cpu_count()}")
    logger.debug(f"multiprocessing.cpu_count() = {multiprocessing.cpu_count()}")
    try:
        import psutil

        logger.debug(f"psutil.cpu_count(logical=True) = {psutil.cpu_count(logical=True)}")
        logger.debug(f"psutil.cpu_count(logical=False) = {psutil.cpu_count(logical=False)}")
    except ImportError:
        pass

    env_vars = _get_dask_worker_thread_env()
    for env_name, env_value in env_vars.items():
        os.environ[env_name] = env_value

    resolved_n_workers = n_workers if n_workers is not None else max(1, (os.cpu_count() or 4) - 2)
    cluster = LocalCluster(
        n_workers=resolved_n_workers,
        threads_per_worker=1,
        processes=True,
        memory_limit="auto",
        env=env_vars,
    )
    client = Client(cluster)
    cluster_worker_count = len(getattr(cluster, "workers", {}))
    scheduler_worker_count = len(client.scheduler_info().get("workers", {}))
    logger.info(
        f"Dask client started with requested={resolved_n_workers}, cluster={cluster_worker_count}, scheduler={scheduler_worker_count} process workers "
        f"(BLAS threads=1). Dashboard: {client.dashboard_link}"
    )
    return client, cluster, resolved_n_workers


def recreate_process_dask_client(
    client,
    cluster,
    n_workers: Optional[int] = None,
    sleep_seconds: int = 2,
    max_create_attempts: int = 3,
    retry_backoff_seconds: int = 5,
):
    sleep(sleep_seconds)
    try:
        client.close(timeout=10)
    except Exception as e:
        logger.debug(f"Client close warning during recreation: {e}")
    if cluster is not None:
        try:
            cluster.close(timeout=10)
        except Exception as e:
            logger.debug(f"Cluster close warning during recreation: {e}")

    client = None
    cluster = None
    gc.collect()

    last_error: Optional[Exception] = None
    for attempt in range(1, max_create_attempts + 1):
        try:
            return create_process_dask_client(n_workers=n_workers)
        except Exception as exc:
            last_error = exc
            logger.warning(f"Dask client recreation attempt {attempt}/{max_create_attempts} failed: {exc}")
            gc.collect()
            if attempt < max_create_attempts:
                sleep(retry_backoff_seconds * attempt)

    assert last_error is not None
    raise last_error


# =============================================================================
# DEPRECATED: Original bulk_add_or_upsert_objects (commented out for reference)
# This was replaced by the unified version above that includes overwrite support.
# =============================================================================
# def bulk_add_or_upsert_objects_ORIGINAL(
#     objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]],
#     upserter_callable: Callable[[BulkAddableObjects], Insert],
#     bulk_insert: bool = True,
# ) -> None:
#     """
#     Adds in bulk or upserts provided Collection of objects to DB.
#
#     :param objects_to_add: Objects to be added to database
#     :type objects_to_add: Collection[BulkAddableObjects]
#     :param upserter_callable: Callable with upsert method to be used in case of bulk add failure
#     :type upserter_callable: Callable[[Collection[BulkAddableObjects]], None]
#     :param bulk_insert: If bulk add should be even attempted
#     :type bulk_insert: bool
#     :return: None
#     :rtype: NoneType
#     """
#
#     if isinstance(objects_to_add, Collection):
#         valid_objects = objects_to_add
#     else:
#         valid_objects = (objects_to_add,)
#
#     if bulk_insert:
#         logger.debug("Trying to do bulk insert")
#         try:
#             bulk_add_objects(valid_objects)
#         except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
#             logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
#             db.session.rollback()
#
#             logger.warning("Retrying with upsert")
#             _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
#     else:
#         logger.info("Starting to perform careful upsert")
#         _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
#     return


# =============================================================================
# DEPRECATED: bulk_add_or_upsert_objects_ccf (commented out for reference)
# This functionality is now integrated into the unified bulk_add_or_upsert_objects above.
# The unified function auto-detects CrosscorrelationCartesian objects and applies
# the appropriate pre-delete or filter logic based on the overwrite parameter.
# =============================================================================
# def bulk_add_or_upsert_objects_ccf(
#     objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]],
#     upserter_callable: Callable[[BulkAddableObjects], Insert],
#     bulk_insert: bool = True,
#     overwrite: bool = True,
# ) -> None:
#     """
#     Adds in bulk or upserts provided Collection of objects to DB.
#
#     :param objects_to_add: Objects to be added to database
#     :type objects_to_add: Collection[BulkAddableObjects]
#     :param upserter_callable: Callable with upsert method to be used in case of bulk add failure
#     :type upserter_callable: Callable[[Collection[BulkAddableObjects]], None]
#     :param bulk_insert: If bulk add should be even attempted
#     :type bulk_insert: bool
#     :param overwrite: If True, delete existing conflicting records before insert.
#                       If False, filter out objects that already exist in DB.
#     :type overwrite: bool
#     :return: None
#     :rtype: NoneType
#     """
#     import time as time_module  # DEBUG_TIMING
#
#     if isinstance(objects_to_add, Collection):
#         valid_objects = objects_to_add
#     else:
#         valid_objects = (objects_to_add,)
#
#     if bulk_insert:
#         logger.debug("Trying to do bulk insert")
#
#         if overwrite:
#             # Pre-delete conflicting records to avoid slow upsert fallback
#             t_start = time_module.time()  # DEBUG_TIMING
#             _delete_conflicting_ccf_records(valid_objects)
#             logger.debug(f"DEBUG_TIMING: _delete_conflicting_ccf_records took {time_module.time() - t_start:.2f}s")
#         else:
#             # Filter out objects that already exist in DB (skip existing)
#             t_start = time_module.time()  # DEBUG_TIMING
#             valid_objects = _filter_existing_ccf_records(valid_objects)
#             logger.debug(f"DEBUG_TIMING: _filter_existing_ccf_records took {time_module.time() - t_start:.2f}s")
#             if not valid_objects:
#                 logger.info("All objects already exist in DB, skipping insert (overwrite=False)")
#                 return
#
#         try:
#             t_start = time_module.time()  # DEBUG_TIMING
#             bulk_add_objects(valid_objects)
#             logger.debug(f"DEBUG_TIMING: bulk_add_objects took {time_module.time() - t_start:.2f}s for {len(valid_objects)} objects")
#         except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
#             logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
#             db.session.rollback()
#             logger.warning("DEBUG_UPSERT: Rollback complete.")
#
#             logger.warning(f"DEBUG_UPSERT: Retrying with upsert for {len(valid_objects)} objects...")
#             _run_upsert_commands_ccf(objects_to_add=valid_objects, upserter_callable=upserter_callable)
#             logger.warning("DEBUG_UPSERT: Upsert completed successfully.")
#     else:
#         logger.info("Starting to perform careful upsert")
#         _run_upsert_commands_ccf(objects_to_add=valid_objects, upserter_callable=upserter_callable)
#     return


def _extract_exact_ccf_conflict_keys(
    objects_to_add: Collection[BulkAddableObjects],
) -> Tuple[Tuple[int, int, int], ...]:
    """
    Extract exact unique conflict keys for cartesian cross-correlation rows.

    Returned keys match the unique constraint tuple:
    ``(timespan_id, componentpair_id, crosscorrelation_cartesian_params_id)``.
    """

    conflict_keys = set()
    for obj in objects_to_add:
        if all(
            hasattr(obj, attr)
            for attr in (
                "timespan_id",
                "componentpair_id",
                "crosscorrelation_cartesian_params_id",
            )
        ):
            conflict_keys.add(
                (
                    obj.timespan_id,
                    obj.componentpair_id,
                    obj.crosscorrelation_cartesian_params_id,
                )
            )

    return tuple(conflict_keys)


def _delete_conflicting_ccf_records(objects_to_add: Collection[BulkAddableObjects]) -> None:
    """
    Pre-delete existing CrosscorrelationCartesian records that would conflict with the new batch.
    This avoids the slow one-by-one upsert fallback.

    Conflicts are based on the unique constraint: (timespan_id, componentpair_id, crosscorrelation_cartesian_params_id)
    """
    from noiz.models import CrosscorrelationCartesian

    if not objects_to_add:
        return

    # Check if these are CrosscorrelationCartesian objects
    first_obj = next(iter(objects_to_add), None)
    if first_obj is None or not hasattr(first_obj, "timespan_id") or not hasattr(first_obj, "componentpair_id"):
        logger.debug("DEBUG_PREDELETE: Objects are not CrosscorrelationCartesian, skipping pre-delete")
        return

    conflict_keys = _extract_exact_ccf_conflict_keys(objects_to_add)

    if not conflict_keys:
        return

    timespan_ids = {timespan_id for timespan_id, _, _ in conflict_keys}
    componentpair_ids = {componentpair_id for _, componentpair_id, _ in conflict_keys}
    params_ids = {params_id for _, _, params_id in conflict_keys}

    logger.warning(
        f"DEBUG_PREDELETE: Checking for existing records to delete: "
        f"{len(timespan_ids)} timespan(s), {len(componentpair_ids)} componentpair(s), {len(params_ids)} params"
    )

    try:
        deleted_count = 0
        for conflict_key_batch in more_itertools.chunked(conflict_keys, CCF_PREDELETE_CHUNK_SIZE):
            delete_query = db.session.query(CrosscorrelationCartesian).filter(
                tuple_(
                    CrosscorrelationCartesian.timespan_id,
                    CrosscorrelationCartesian.componentpair_id,
                    CrosscorrelationCartesian.crosscorrelation_cartesian_params_id,
                ).in_(tuple(conflict_key_batch)),
            )
            deleted_count += delete_query.delete(synchronize_session=False)

        if deleted_count > 0:
            logger.warning(f"DEBUG_PREDELETE: Deleted {deleted_count} existing records before insert")
            db.session.commit()
        else:
            logger.debug("DEBUG_PREDELETE: No existing records found, proceeding with insert")
    except Exception as e:
        logger.error(f"DEBUG_PREDELETE: Error during pre-delete: {e}")
        db.session.rollback()
        # Don't raise - let the normal upsert fallback handle it
        return


def _filter_existing_ccf_records(objects_to_add: Collection[BulkAddableObjects]) -> Collection[BulkAddableObjects]:
    """
    Filter out CrosscorrelationCartesian records that already exist in the database.
    This allows skipping existing records instead of overwriting them.

    Returns a filtered collection containing only objects that don't exist in DB.
    """
    from noiz.models import CrosscorrelationCartesian

    if not objects_to_add:
        return objects_to_add

    # Check if these are CrosscorrelationCartesian objects
    first_obj = next(iter(objects_to_add), None)
    if first_obj is None or not hasattr(first_obj, "timespan_id") or not hasattr(first_obj, "componentpair_id"):
        logger.debug("DEBUG_FILTER: Objects are not CrosscorrelationCartesian, skipping filter")
        return objects_to_add

    conflict_keys = _extract_exact_ccf_conflict_keys(objects_to_add)

    if not conflict_keys:
        return objects_to_add

    timespan_ids = {timespan_id for timespan_id, _, _ in conflict_keys}
    componentpair_ids = {componentpair_id for _, componentpair_id, _ in conflict_keys}
    params_ids = {params_id for _, _, params_id in conflict_keys}

    logger.info(
        f"DEBUG_FILTER: Checking for existing records to skip: "
        f"{len(timespan_ids)} timespan(s), {len(componentpair_ids)} componentpair(s), {len(params_ids)} params"
    )

    try:
        # Query existing records that match our batch criteria
        existing_query = db.session.query(
            CrosscorrelationCartesian.timespan_id,
            CrosscorrelationCartesian.componentpair_id,
            CrosscorrelationCartesian.crosscorrelation_cartesian_params_id,
        ).filter(
            tuple_(
                CrosscorrelationCartesian.timespan_id,
                CrosscorrelationCartesian.componentpair_id,
                CrosscorrelationCartesian.crosscorrelation_cartesian_params_id,
            ).in_(conflict_keys),
        )

        # Build a set of existing (timespan_id, componentpair_id, params_id) tuples
        existing_keys = set()
        for row in existing_query.all():
            existing_keys.add((row.timespan_id, row.componentpair_id, row.crosscorrelation_cartesian_params_id))

        if not existing_keys:
            logger.debug("DEBUG_FILTER: No existing records found, proceeding with all objects")
            return objects_to_add

        # Filter out objects that already exist
        original_count = len(objects_to_add)
        filtered_objects = []
        for obj in objects_to_add:
            key = (obj.timespan_id, obj.componentpair_id, obj.crosscorrelation_cartesian_params_id)
            if key not in existing_keys:
                filtered_objects.append(obj)

        skipped_count = original_count - len(filtered_objects)
        logger.info(
            f"DEBUG_FILTER: Skipping {skipped_count} existing records, inserting {len(filtered_objects)} new records (overwrite=False)"
        )

        return filtered_objects

    except Exception as e:
        logger.error(f"DEBUG_FILTER: Error during filter query: {e}")
        db.session.rollback()
        # Return original objects and let normal error handling take over
        return objects_to_add


def _delete_conflicting_ccf_cylindrical_records(objects_to_add: Collection[BulkAddableObjects]) -> None:
    """
    Pre-delete existing CrosscorrelationCylindrical records that would conflict with the new batch.
    This avoids the slow one-by-one upsert fallback.

    Conflicts are based on the unique constraint: (timespan_id, componentpair_cylindrical_id, crosscorrelation_cylindrical_params_id)
    """
    from noiz.models import CrosscorrelationCylindrical

    if not objects_to_add:
        return

    # Check if these are CrosscorrelationCylindrical objects
    first_obj = next(iter(objects_to_add), None)
    if (
        first_obj is None
        or not hasattr(first_obj, "timespan_id")
        or not hasattr(first_obj, "componentpair_cylindrical_id")
    ):
        logger.debug("DEBUG_PREDELETE_CYL: Objects are not CrosscorrelationCylindrical, skipping pre-delete")
        return

    # Extract unique keys that would conflict
    timespan_ids = set()
    componentpair_cylindrical_ids = set()
    params_ids = set()

    for obj in objects_to_add:
        if (
            hasattr(obj, "timespan_id")
            and hasattr(obj, "componentpair_cylindrical_id")
            and hasattr(obj, "crosscorrelation_cylindrical_params_id")
        ):
            timespan_ids.add(obj.timespan_id)
            componentpair_cylindrical_ids.add(obj.componentpair_cylindrical_id)
            params_ids.add(obj.crosscorrelation_cylindrical_params_id)

    if not timespan_ids or not componentpair_cylindrical_ids or not params_ids:
        return

    logger.warning(
        f"DEBUG_PREDELETE_CYL: Checking for existing cylindrical CCF records to delete: "
        f"{len(timespan_ids)} timespan(s), {len(componentpair_cylindrical_ids)} componentpair_cylindrical(s), {len(params_ids)} params"
    )

    # Delete existing records that match our batch
    try:
        delete_query = db.session.query(CrosscorrelationCylindrical).filter(
            CrosscorrelationCylindrical.timespan_id.in_(timespan_ids),
            CrosscorrelationCylindrical.componentpair_cylindrical_id.in_(componentpair_cylindrical_ids),
            CrosscorrelationCylindrical.crosscorrelation_cylindrical_params_id.in_(params_ids),
        )

        count = delete_query.count()
        if count > 0:
            logger.warning(
                f"DEBUG_PREDELETE_CYL: Found {count} existing cylindrical CCF records to delete before insert"
            )
            delete_query.delete(synchronize_session=False)
            db.session.commit()
            logger.warning(f"DEBUG_PREDELETE_CYL: Deleted {count} existing cylindrical CCF records successfully")
        else:
            logger.debug("DEBUG_PREDELETE_CYL: No existing cylindrical CCF records found, proceeding with insert")
    except Exception as e:
        logger.error(f"DEBUG_PREDELETE_CYL: Error during pre-delete: {e}")
        db.session.rollback()
        # Don't raise - let the normal upsert fallback handle it
        return


def _filter_existing_ccf_cylindrical_records(
    objects_to_add: Collection[BulkAddableObjects],
) -> Collection[BulkAddableObjects]:
    """
    Filter out CrosscorrelationCylindrical records that already exist in the database.
    This allows skipping existing records instead of overwriting them.

    Returns a filtered collection containing only objects that don't exist in DB.
    """
    from noiz.models import CrosscorrelationCylindrical

    if not objects_to_add:
        return objects_to_add

    # Check if these are CrosscorrelationCylindrical objects
    first_obj = next(iter(objects_to_add), None)
    if (
        first_obj is None
        or not hasattr(first_obj, "timespan_id")
        or not hasattr(first_obj, "componentpair_cylindrical_id")
    ):
        logger.debug("DEBUG_FILTER_CYL: Objects are not CrosscorrelationCylindrical, skipping filter")
        return objects_to_add

    # Extract unique keys to check for existence
    timespan_ids = set()
    componentpair_cylindrical_ids = set()
    params_ids = set()

    for obj in objects_to_add:
        if (
            hasattr(obj, "timespan_id")
            and hasattr(obj, "componentpair_cylindrical_id")
            and hasattr(obj, "crosscorrelation_cylindrical_params_id")
        ):
            timespan_ids.add(obj.timespan_id)
            componentpair_cylindrical_ids.add(obj.componentpair_cylindrical_id)
            params_ids.add(obj.crosscorrelation_cylindrical_params_id)

    if not timespan_ids or not componentpair_cylindrical_ids or not params_ids:
        return objects_to_add

    logger.info(
        f"DEBUG_FILTER_CYL: Checking for existing cylindrical CCF records to skip: "
        f"{len(timespan_ids)} timespan(s), {len(componentpair_cylindrical_ids)} componentpair_cylindrical(s), {len(params_ids)} params"
    )

    try:
        # Query existing records that match our batch criteria
        existing_query = db.session.query(
            CrosscorrelationCylindrical.timespan_id,
            CrosscorrelationCylindrical.componentpair_cylindrical_id,
            CrosscorrelationCylindrical.crosscorrelation_cylindrical_params_id,
        ).filter(
            CrosscorrelationCylindrical.timespan_id.in_(timespan_ids),
            CrosscorrelationCylindrical.componentpair_cylindrical_id.in_(componentpair_cylindrical_ids),
            CrosscorrelationCylindrical.crosscorrelation_cylindrical_params_id.in_(params_ids),
        )

        # Build a set of existing (timespan_id, componentpair_cylindrical_id, params_id) tuples
        existing_keys = set()
        for row in existing_query.all():
            existing_keys.add(
                (row.timespan_id, row.componentpair_cylindrical_id, row.crosscorrelation_cylindrical_params_id)
            )

        if not existing_keys:
            logger.debug("DEBUG_FILTER_CYL: No existing cylindrical CCF records found, proceeding with all objects")
            return objects_to_add

        # Filter out objects that already exist
        original_count = len(objects_to_add)
        filtered_objects = []
        for obj in objects_to_add:
            key = (obj.timespan_id, obj.componentpair_cylindrical_id, obj.crosscorrelation_cylindrical_params_id)
            if key not in existing_keys:
                filtered_objects.append(obj)

        skipped_count = original_count - len(filtered_objects)
        logger.info(
            f"DEBUG_FILTER_CYL: Skipping {skipped_count} existing cylindrical CCF records, inserting {len(filtered_objects)} new records (overwrite=False)"
        )

        return filtered_objects

    except Exception as e:
        logger.error(f"DEBUG_FILTER_CYL: Error during filter query: {e}")
        db.session.rollback()
        # Return original objects and let normal error handling take over
        return objects_to_add


def _extract_exact_beamforming_conflict_keys(
    objects_to_add: Collection[BulkAddableObjects],
) -> Tuple[Tuple[int, int], ...]:
    conflict_keys = set()
    for obj in objects_to_add:
        if all(hasattr(obj, attr) for attr in ("timespan_id", "beamforming_params_id")):
            conflict_keys.add((obj.timespan_id, obj.beamforming_params_id))

    return tuple(conflict_keys)


def _delete_conflicting_beamforming_records(objects_to_add: Collection[BulkAddableObjects]) -> None:
    from noiz.models.beamforming import (
        BeamformingResult,
        BeamformingFile,
        BeamformingPeakAverageAbspower,
        BeamformingPeakAverageRelpower,
        BeamformingPeakAllAbspower,
        BeamformingPeakAllRelpower,
        association_table_beamforming_results_datachunks,
        association_table_beamforming_result_avg_abspower,
        association_table_beamforming_result_avg_relpower,
        association_table_beamforming_result_all_abspower,
        association_table_beamforming_result_all_relpower,
    )

    if not objects_to_add:
        return

    conflict_keys = _extract_exact_beamforming_conflict_keys(objects_to_add)
    if not conflict_keys:
        return

    try:
        existing_rows = (
            db.session.query(BeamformingResult.id, BeamformingResult.beamforming_file_id)
            .filter(
                tuple_(
                    BeamformingResult.timespan_id,
                    BeamformingResult.beamforming_params_id,
                ).in_(conflict_keys)
            )
            .all()
        )

        if len(existing_rows) == 0:
            logger.debug("DEBUG_PREDELETE_BEAM: No existing beamforming records found, proceeding with insert")
            return

        result_ids = [row.id for row in existing_rows]
        file_ids = [row.beamforming_file_id for row in existing_rows if row.beamforming_file_id is not None]

        avg_abspower_peak_ids = [
            row[0]
            for row in db.session.query(
                association_table_beamforming_result_avg_abspower.c.beamforming_peak_average_abspower_id
            )
            .filter(association_table_beamforming_result_avg_abspower.c.beamforming_result_id.in_(result_ids))
            .all()
        ]
        avg_relpower_peak_ids = [
            row[0]
            for row in db.session.query(
                association_table_beamforming_result_avg_relpower.c.beamforming_peak_average_relpower_id
            )
            .filter(association_table_beamforming_result_avg_relpower.c.beamforming_result_id.in_(result_ids))
            .all()
        ]
        all_abspower_peak_ids = [
            row[0]
            for row in db.session.query(
                association_table_beamforming_result_all_abspower.c.beamforming_peak_all_abspower_id
            )
            .filter(association_table_beamforming_result_all_abspower.c.beamforming_result_id.in_(result_ids))
            .all()
        ]
        all_relpower_peak_ids = [
            row[0]
            for row in db.session.query(
                association_table_beamforming_result_all_relpower.c.beamforming_peak_all_relpower_id
            )
            .filter(association_table_beamforming_result_all_relpower.c.beamforming_result_id.in_(result_ids))
            .all()
        ]

        db.session.execute(
            association_table_beamforming_results_datachunks.delete().where(
                association_table_beamforming_results_datachunks.c.beamforming_result_id.in_(result_ids)
            )
        )
        db.session.execute(
            association_table_beamforming_result_avg_abspower.delete().where(
                association_table_beamforming_result_avg_abspower.c.beamforming_result_id.in_(result_ids)
            )
        )
        db.session.execute(
            association_table_beamforming_result_avg_relpower.delete().where(
                association_table_beamforming_result_avg_relpower.c.beamforming_result_id.in_(result_ids)
            )
        )
        db.session.execute(
            association_table_beamforming_result_all_abspower.delete().where(
                association_table_beamforming_result_all_abspower.c.beamforming_result_id.in_(result_ids)
            )
        )
        db.session.execute(
            association_table_beamforming_result_all_relpower.delete().where(
                association_table_beamforming_result_all_relpower.c.beamforming_result_id.in_(result_ids)
            )
        )

        db.session.query(BeamformingResult).filter(BeamformingResult.__table__.c.id.in_(result_ids)).delete(
            synchronize_session=False
        )

        if file_ids:
            db.session.query(BeamformingFile).filter(BeamformingFile.__table__.c.id.in_(file_ids)).delete(
                synchronize_session=False
            )
        if avg_abspower_peak_ids:
            db.session.query(BeamformingPeakAverageAbspower).filter(
                BeamformingPeakAverageAbspower.__table__.c.id.in_(avg_abspower_peak_ids)
            ).delete(synchronize_session=False)
        if avg_relpower_peak_ids:
            db.session.query(BeamformingPeakAverageRelpower).filter(
                BeamformingPeakAverageRelpower.__table__.c.id.in_(avg_relpower_peak_ids)
            ).delete(synchronize_session=False)
        if all_abspower_peak_ids:
            db.session.query(BeamformingPeakAllAbspower).filter(
                BeamformingPeakAllAbspower.__table__.c.id.in_(all_abspower_peak_ids)
            ).delete(synchronize_session=False)
        if all_relpower_peak_ids:
            db.session.query(BeamformingPeakAllRelpower).filter(
                BeamformingPeakAllRelpower.__table__.c.id.in_(all_relpower_peak_ids)
            ).delete(synchronize_session=False)

        db.session.commit()
        logger.warning(f"DEBUG_PREDELETE_BEAM: Deleted {len(result_ids)} existing beamforming record(s) before insert")
    except Exception as e:
        logger.error(f"DEBUG_PREDELETE_BEAM: Error during beamforming pre-delete: {e}")
        db.session.rollback()
        return


def _filter_existing_beamforming_records(
    objects_to_add: Collection[BulkAddableObjects],
) -> Collection[BulkAddableObjects]:
    from noiz.models.beamforming import BeamformingResult

    if not objects_to_add:
        return objects_to_add

    conflict_keys = _extract_exact_beamforming_conflict_keys(objects_to_add)
    if not conflict_keys:
        return objects_to_add

    try:
        existing_keys = set(
            db.session.query(
                BeamformingResult.timespan_id,
                BeamformingResult.beamforming_params_id,
            )
            .filter(tuple_(BeamformingResult.timespan_id, BeamformingResult.beamforming_params_id).in_(conflict_keys))
            .all()
        )

        if not existing_keys:
            logger.debug("DEBUG_FILTER_BEAM: No existing beamforming records found, proceeding with all objects")
            return objects_to_add

        filtered_objects = [
            obj for obj in objects_to_add if (obj.timespan_id, obj.beamforming_params_id) not in existing_keys
        ]
        skipped_count = len(objects_to_add) - len(filtered_objects)
        logger.info(
            f"DEBUG_FILTER_BEAM: Skipping {skipped_count} existing beamforming records, inserting {len(filtered_objects)} new records (overwrite=False)"
        )
        return filtered_objects
    except Exception as e:
        logger.error(f"DEBUG_FILTER_BEAM: Error during beamforming filter query: {e}")
        db.session.rollback()
        return objects_to_add


def bulk_merge_or_upsert_objects(
    objects_to_merge: Union[BulkAddableObjects, Collection[BulkAddableObjects]],
    upserter_callable: Callable[[BulkAddableObjects], Insert],
    bulk_insert: bool = True,
) -> None:
    """
    Merges in bulk or upserts provided Collection of objects to DB.

    :param objects_to_merge: Objects to be added to database
    :type objects_to_merge: Collection[BulkAddableObjects]
    :param upserter_callable: Callable with upsert method to be used in case of bulk add failure
    :type upserter_callable: Callable[[Collection[BulkAddableObjects]], None]
    :param bulk_insert: If bulk add should be even attempted
    :type bulk_insert: bool
    :return: None
    :rtype: NoneType
    """

    if isinstance(objects_to_merge, Collection):
        valid_objects = objects_to_merge
    else:
        valid_objects = (objects_to_merge,)

    if bulk_insert:
        logger.debug("Trying to do bulk insert")
        try:
            bulk_merge_objects(valid_objects)
        except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning("Retrying with upsert")
            _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    else:
        logger.info("Starting to perform careful upsert")
        _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    return


def bulk_add_and_check_objects(
    objects_to_add: Union[BulkAddableFileObjects, Collection[BulkAddableFileObjects]],
) -> None:
    """
    Adds in bulk or upserts provided Collection of objects to DB.

    :param objects_to_add: Objects to be added to database
    :type objects_to_add: Collection[BulkAddableFileObjects]
    :return: None
    :rtype: NoneType
    """

    if isinstance(objects_to_add, Collection):
        valid_objects = objects_to_add
    else:
        valid_objects = (objects_to_add,)

    logger.debug("Trying to do bulk insert")
    try:
        bulk_add_objects(valid_objects)
    except (IntegrityError, UnmappedInstanceError) as e:
        logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
        db.session.rollback()
        raise e
    return


def _normalize_parallel_results_for_session(
    results: Collection[BulkAddableObjects],
) -> List[BulkAddableObjects]:
    """
    Rebuild selected ORM objects returned from worker processes before inserting them
    in the main process session.

    Cross-correlation worker results are produced in separate processes and returned as
    detached ORM instances with attached file relationship objects. Rehydrating them in
    the main process avoids cross-process ORM identity/relationship state leaking into
    the insert phase.
    """
    from noiz.models import CrosscorrelationCartesian, CrosscorrelationCartesianFile, Datachunk
    from noiz.models.beamforming import (
        BeamformingResult as BeamformingResultModel,
        BeamformingFile as BeamformingFileModel,
        BeamformingPeakAverageAbspower,
        BeamformingPeakAverageRelpower,
        BeamformingPeakAllAbspower,
        BeamformingPeakAllRelpower,
    )

    normalized: List[BulkAddableObjects] = []
    rebuilt_count = 0

    beamforming_results = [res for res in results if isinstance(res, BeamformingResultModel)]
    datachunk_map = {}
    if beamforming_results:
        unique_datachunk_ids = sorted(
            {datachunk_id for res in beamforming_results for datachunk_id in getattr(res, "datachunk_ids", [])}
        )
        if unique_datachunk_ids:
            datachunk_map = {
                datachunk.id: datachunk
                for datachunk in db.session.query(Datachunk).filter(Datachunk.id.in_(unique_datachunk_ids)).all()
            }

    def _copy_beamforming_peak(source_peak, target_cls):
        return target_cls(
            slowness=source_peak.slowness,
            slowness_x=source_peak.slowness_x,
            slowness_y=source_peak.slowness_y,
            amplitude=source_peak.amplitude,
            azimuth=source_peak.azimuth,
            backazimuth=source_peak.backazimuth,
        )

    for result in results:
        if isinstance(result, CrosscorrelationCartesian):
            rebuilt_count += 1
            normalized.append(
                CrosscorrelationCartesian(
                    crosscorrelation_cartesian_params_id=result.crosscorrelation_cartesian_params_id,
                    componentpair_id=result.componentpair_id,
                    timespan_id=result.timespan_id,
                    file=(
                        CrosscorrelationCartesianFile(filepath=result.file.filepath)
                        if result.file is not None
                        else None
                    ),
                )
            )
        elif isinstance(result, BeamformingResultModel):
            rebuilt_count += 1
            rebuilt = BeamformingResultModel(
                beamforming_params_id=result.beamforming_params_id,
                timespan_id=result.timespan_id,
                used_component_count=result.used_component_count,
            )
            if result.file is not None:
                rebuilt.file = BeamformingFileModel(_filepath=result.file.filepath)
            rebuilt.datachunks = [
                datachunk_map[datachunk_id]
                for datachunk_id in getattr(result, "datachunk_ids", [])
                if datachunk_id in datachunk_map
            ]
            for peak in result.average_abspower_peaks:
                rebuilt.average_abspower_peaks.append(_copy_beamforming_peak(peak, BeamformingPeakAverageAbspower))
            for peak in result.average_relpower_peaks:
                rebuilt.average_relpower_peaks.append(_copy_beamforming_peak(peak, BeamformingPeakAverageRelpower))
            for peak in result.all_abspower_peaks:
                rebuilt.all_abspower_peaks.append(_copy_beamforming_peak(peak, BeamformingPeakAllAbspower))
            for peak in result.all_relpower_peaks:
                rebuilt.all_relpower_peaks.append(_copy_beamforming_peak(peak, BeamformingPeakAllRelpower))
            normalized.append(rebuilt)
        else:
            normalized.append(result)

    if rebuilt_count > 0:
        logger.debug(f"DEBUG_DASK: Rebuilt {rebuilt_count} parallel results in main process before DB insert")

    return normalized


def _run_upsert_commands(
    objects_to_add: Collection[BulkAddableObjects], upserter_callable: Callable[[BulkAddableObjects], Insert]
) -> None:
    logger.info(f"Starting upsert procedure. There are {len(objects_to_add)} elements to be processed.")
    insert_commands = []
    for results in objects_to_add:
        if not isinstance(results, get_args(BulkAddableObjects)):
            logger.warning(
                f"Provided object is not an instance of any of the subtypes of {BulkAddableObjects}. "
                f"Provided object was an {type(results)}. "
                f"Content of the object: {results}"
                f"Skipping."
            )
            continue

        logger.debug(f"Generating upsert command for {results}")
        insert_command = upserter_callable(results)
        insert_commands.append(insert_command)

    for insert_command in insert_commands:
        db.session.execute(insert_command)

    logger.debug("Commiting session.")
    db.session.commit()


# =============================================================================
# DEPRECATED: _run_upsert_commands_ccf (commented out for reference)
# This was a CCF-specific upsert function with extra logging.
# The unified _run_upsert_commands can be used for all cases.
# =============================================================================
# def _run_upsert_commands_ccf(
#     objects_to_add: Collection[BulkAddableObjects], upserter_callable: Callable[[BulkAddableObjects], Insert]
# ) -> None:
#     total_objects = len(objects_to_add)
#     logger.info(f"Starting upsert procedure. There are {total_objects} elements to be processed.")
#     logger.warning(f"DEBUG_UPSERT: Beginning to generate upsert commands for {total_objects} objects...")
#
#     insert_commands = []
#     for idx, results in enumerate(objects_to_add):
#         if idx % 500 == 0:
#             logger.warning(f"DEBUG_UPSERT: Generating upsert command {idx}/{total_objects}...")
#
#         if not isinstance(results, get_args(BulkAddableObjects)):
#             logger.warning(
#                 f"Provided object is not an instance of any of the subtypes of {BulkAddableObjects}. "
#                 f"Provided object was an {type(results)}. "
#                 f"Content of the object: {results}"
#                 f"Skipping."
#             )
#             continue
#
#         logger.debug(f"Generating upsert command for {results}")
#         try:
#             insert_command = upserter_callable(results)
#             insert_commands.append(insert_command)
#         except Exception as e:
#             logger.error(f"DEBUG_UPSERT: Error generating upsert command for object {idx}: {e}")
#             raise
#
#     logger.warning(f"DEBUG_UPSERT: Generated {len(insert_commands)} upsert commands. Now executing...")
#
#     for idx, insert_command in enumerate(insert_commands):
#         if idx % 500 == 0:
#             logger.warning(f"DEBUG_UPSERT: Executing upsert command {idx}/{len(insert_commands)}...")
#         try:
#             db.session.execute(insert_command)
#         except Exception as e:
#             logger.error(f"DEBUG_UPSERT: Error executing upsert command {idx}: {e}")
#             raise
#
#     logger.warning(f"DEBUG_UPSERT: All {len(insert_commands)} commands executed. Committing session...")
#     db.session.commit()
#     logger.warning("DEBUG_UPSERT: Session committed successfully.")


def _run_calculate_and_upsert_on_dask(
    inputs: Iterable[InputsForMassCalculations],
    calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
    upserter_callable: Callable[[BulkAddableObjects], Insert],
    batch_size: Optional[int] = 5000,
    total_inputs: Optional[int] = None,
    raise_errors: bool = False,
    with_file: bool = False,
    is_beamforming: bool = False,
    is_event_confirmation: bool = False,
    overwrite: bool = True,
    existing_client=None,
    n_workers: Optional[int] = None,
):
    """
    Run calculations on Dask and upsert results to database.

    This is the unified Dask runner that handles all processing types.

    :param inputs: Iterable of input dictionaries for calculations
    :param calculation_task: Callable that performs the calculation
    :param upserter_callable: Callable that prepares upsert commands
    :param batch_size: Number of inputs per Dask batch. If None, it is set to
        floor(total_inputs / n_workers), with a minimum value of 1. If omitted,
        the Python default remains 5000.
    :param total_inputs: Optional precomputed total number of input items. When
        provided together with ``batch_size=None``, this value is used to derive
        the automatic batch size without materializing the input iterable.
    :param raise_errors: Whether to raise errors or skip failed items
    :param with_file: Whether to handle file objects
    :param is_beamforming: Whether this is beamforming processing
    :param is_event_confirmation: Whether this is event confirmation processing
    :param overwrite: If True, delete existing records before insert. If False, skip existing.
    :param existing_client: Optional existing Dask client to reuse. If provided, the client
                           will NOT be closed at the end (caller is responsible for lifecycle).
                           If None, a new client will be created and closed.
    :param n_workers: Optional number of Dask workers to create when this helper owns the
                      client lifecycle. Ignored when ``existing_client`` is provided.
    """
    # Determine if we own the client (and should close it at the end)
    owns_client = existing_client is None

    if owns_client:
        client, cluster, created_worker_count = create_process_dask_client(n_workers=n_workers)
    else:
        client = existing_client
        cluster = None
        created_worker_count = None
        logger.debug(f"Reusing existing Dask client: {client.dashboard_link}")

    scheduler_info = client.scheduler_info()
    scheduler_worker_count = len(scheduler_info["workers"])
    cluster_worker_count = len(getattr(cluster, "workers", {})) if owns_client and cluster is not None else 0
    worker_count = max(1, scheduler_worker_count, cluster_worker_count, created_worker_count or 0)

    if worker_count != scheduler_worker_count:
        logger.warning(
            f"Using effective worker_count={worker_count} for batching even though scheduler_info reports "
            f"{scheduler_worker_count} worker(s)."
        )

    if batch_size is None:
        if total_inputs is not None:
            resolved_total_inputs = total_inputs
        elif isinstance(inputs, Collection):
            resolved_total_inputs = len(inputs)
        else:
            inputs = list(inputs)
            resolved_total_inputs = len(inputs)

        batch_size = max(1, resolved_total_inputs // worker_count)
        logger.info(
            f"Batch size was not provided. Using floor(total_inputs / n_workers) = "
            f"floor({resolved_total_inputs} / {worker_count}) = {batch_size}."
        )

    logger.info(f"Processing will be executed in batches. The chunks size is {batch_size}")

    # Log worker info
    logger.debug(
        f"Client worker counts: effective={worker_count}, scheduler={scheduler_worker_count}, "
        f"cluster={cluster_worker_count}, requested={created_worker_count}"
    )
    for worker_id, worker_info in scheduler_info["workers"].items():
        logger.debug(
            f"Worker {worker_id}: nthreads={worker_info.get('nthreads')}, memory_limit={worker_info.get('memory_limit', 0) / 1024**3:.2f}GB"
        )

    # Keep track of the cluster for recreation
    cluster_ref = cluster if owns_client else None

    for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
        # Materialize batch to allow counting
        batch_list = list(input_batch)
        logger.debug(f"Batch {i} has {len(batch_list)} input items")
        input_batch = batch_list

        if i != 0 and owns_client:
            # Close old client and cluster, create new ones to clear unmanaged memory
            # NOTE: client.restart() is unreliable in Docker containers with old dask versions
            # It can leave workers in an inconsistent state. Creating a new client is more robust.
            logger.info("Creating new Dask client to clear unmanaged memory (avoiding client.restart()).")
            client, cluster_ref, _ = recreate_process_dask_client(
                client=client,
                cluster=cluster_ref,
                n_workers=created_worker_count,
            )
            logger.info(f"New Dask client created with {created_worker_count} workers: {client.dashboard_link}")
        logger.info(f"Starting processing of chunk no.{i}")
        _submit_task_to_client_and_add_results_to_db(
            client=client,
            inputs_to_process=input_batch,
            calculation_task=calculation_task,
            upserter_callable=upserter_callable,
            raise_errors=raise_errors,
            with_file=with_file,
            is_beamforming=is_beamforming,
            is_event_confirmation=is_event_confirmation,
            overwrite=overwrite,
        )

    # Only close client if we created it
    if owns_client:
        logger.info("Shutting down Dask cluster (this may take a few seconds)...")
        try:
            # Give workers time to finish any pending operations
            client.close(timeout=30)
        except Exception as e:
            logger.debug(f"Client close warning: {e}")
        if cluster_ref is not None:
            try:
                cluster_ref.close(timeout=30)
            except Exception as e:
                logger.debug(f"Cluster close warning: {e}")
        logger.info("Dask cluster shut down.")
    return


def _submit_task_to_client_and_add_results_to_db(
    client,
    inputs_to_process: Iterable[InputsForMassCalculations],
    calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
    upserter_callable: Callable[[BulkAddableObjects], Insert],
    raise_errors: bool = False,
    with_file: bool = False,
    is_beamforming: bool = False,
    is_event_confirmation: bool = False,
    overwrite: bool = True,
):
    """
    Submit tasks to Dask client and add results to database.

    This unified function handles:
    - File handling for various processing types
    - Beamforming peak handling
    - Event confirmation merge handling
    - Memory cleanup after processing
    """
    from dask.distributed import as_completed
    import gc

    logger.info("Submitting tasks to Dask client")

    # Convert to list to allow multiple passes
    inputs_list = list(inputs_to_process)

    futures = []
    for _, input_dict in enumerate(inputs_list):
        try:
            futures.append(client.submit(calculation_task, input_dict))
        except CorruptedDataException as e:
            if raise_errors:
                logger.error(f"Cought error {e}. Finishing execution.")
                raise CorruptedDataException(e) from e
            else:
                logger.error(f"Cought error {e}. Skipping to next timespan.")
                continue
        except InconsistentDataException as e:
            if raise_errors:
                logger.error(f"Cought error {e}. Finishing execution.")
                raise InconsistentDataException(e) from e
            else:
                logger.error(f"Cought error {e}. Skipping to next timespan.")
                continue
        except ValueError as e:
            logger.error(e)
            raise e
    logger.info(f"There are {len(futures)} tasks to be executed")

    logger.info("Starting execution. Results will be saved to database on the fly. ")

    total_results_inserted = 0
    for batch_idx, future_batch in enumerate(as_completed(futures, with_results=True, raise_errors=False).batches()):
        finished_futures = []
        non_finished_futures = []

        for future, result in future_batch:
            if future.status == "finished":
                finished_futures.append((future, result))
                continue

            exc = None
            try:
                exc = future.exception()
            except Exception as future_exc:
                exc = future_exc
            non_finished_futures.append((future.status, exc))

        cancelled_count = sum(1 for status, _ in non_finished_futures if status == "cancelled")
        error_count = sum(1 for status, _ in non_finished_futures if status == "error")
        logger.debug(
            f"Completed future_batch {batch_idx}: {len(finished_futures)} finished, "
            f"{cancelled_count} cancelled, {error_count} failed"
        )

        for status, exc in non_finished_futures:
            logger.error(f"Task {status}: {exc}")

        if non_finished_futures:
            if raise_errors:
                raise RuntimeError(
                    "Dask returned cancelled or failed tasks. Aborting to avoid partial database writes. "
                    f"Statuses: {[status for status, _ in non_finished_futures]}"
                )

            recoverable_exceptions = (CorruptedDataException, InconsistentDataException, ObspyError)
            unrecoverable_futures = [
                (status, exc)
                for status, exc in non_finished_futures
                if status == "cancelled" or not isinstance(exc, recoverable_exceptions)
            ]
            if unrecoverable_futures:
                raise RuntimeError(
                    "Dask returned cancelled or unrecoverable failed tasks. Aborting to avoid partial database writes. "
                    f"Statuses: {[status for status, _ in unrecoverable_futures]}"
                )

            logger.warning(
                f"Skipping {len(non_finished_futures)} recoverable failed Dask task(s) because raise_errors is disabled."
            )

        results_nested: List[Tuple[BulkAddableObjects, ...]] = [result for _, result in finished_futures]
        results: List[BulkAddableObjects] = [res for res in more_itertools.flatten(results_nested) if res is not None]
        results = _normalize_parallel_results_for_session(results)
        logger.info(f"Running bulk_add_or_upsert for {len(results)} results")

        if with_file:
            files_to_add = [x.file for x in results if x.file is not None]
            if len(files_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=files_to_add,
                )
                # After files are committed, update the file IDs on the result objects
                # This is necessary because the relationship doesn't auto-update the FK column
                for res in results:
                    if res.file is not None and res.file.id is not None:
                        if hasattr(res, "beamforming_file_id"):
                            res.beamforming_file_id = res.file.id
                        elif hasattr(res, "crosscorrelation_cylindrical_file_id"):
                            res.crosscorrelation_cylindrical_file_id = res.file.id
                        elif hasattr(res, "crosscorrelation_cartesian_file_id"):
                            res.crosscorrelation_cartesian_file_id = res.file.id
                        elif hasattr(res, "datachunk_file_id"):
                            res.datachunk_file_id = res.file.id
                        elif hasattr(res, "processed_datachunk_file_id"):
                            res.processed_datachunk_file_id = res.file.id
                        elif hasattr(res, "ppsd_file_id"):
                            res.ppsd_file_id = res.file.id
        if is_beamforming:
            peaks_to_add = []
            for res in results:
                peaks_to_add.extend(res.average_abspower_peaks)
                peaks_to_add.extend(res.average_relpower_peaks)
                peaks_to_add.extend(res.all_abspower_peaks)
                peaks_to_add.extend(res.all_relpower_peaks)
            if len(peaks_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=peaks_to_add,
                )

        if is_event_confirmation:
            bulk_merge_or_upsert_objects(
                objects_to_merge=results, upserter_callable=upserter_callable, bulk_insert=True
            )
        else:
            # Use unified bulk_add_or_upsert_objects with overwrite parameter
            bulk_add_or_upsert_objects(
                objects_to_add=results, upserter_callable=upserter_callable, bulk_insert=True, overwrite=overwrite
            )

        total_results_inserted += len(results)

        # Clear batch-level references to prevent accumulation
        del results_nested
        del results
        del future_batch

    # Ensure all DB operations are fully committed
    db.session.commit()
    logger.info(
        f"All {len(futures)} tasks completed and committed to DB. Total results inserted: {total_results_inserted}"
    )

    # Clear references to large objects
    del inputs_list
    del futures

    # Clear SQLAlchemy session to prevent identity map accumulation
    db.session.expire_all()

    gc.collect()
    logger.debug("Memory cleanup completed for _submit_task_to_client_and_add_results_to_db")

    return


# =============================================================================
# DEPRECATED: _run_calculate_and_upsert_on_dask_ccf (commented out for reference)
# This functionality is now integrated into the unified _run_calculate_and_upsert_on_dask above.
# The unified function includes existing_client support and all CCF-specific optimizations.
# =============================================================================
# def _run_calculate_and_upsert_on_dask_ccf(
#     inputs: Iterable[InputsForMassCalculations],
#     calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
#     upserter_callable: Callable[[BulkAddableObjects], Insert],
#     batch_size: int = 5000,
#     raise_errors: bool = False,
#     with_file: bool = False,
#     is_beamforming: bool = False,
#     is_event_confirmation: bool = False,
#     overwrite: bool = True,
#     existing_client=None,
# ):
#     """
#     Run calculations on Dask and upsert results to database.
#
#     :param existing_client: Optional existing Dask client to reuse. If provided, the client
#                            will NOT be closed at the end (caller is responsible for lifecycle).
#                            If None, a new client will be created and closed.
#     """
#     from dask.distributed import Client, LocalCluster
#     import os  # DEBUG_DASK
#     import multiprocessing  # DEBUG_DASK
#
#     # Determine if we own the client (and should close it at the end)
#     owns_client = existing_client is None
#
#     if owns_client:
#         # DEBUG_DASK: Diagnose CPU detection
#         logger.warning(f"DEBUG_DASK: os.cpu_count() = {os.cpu_count()}")
#         logger.warning(f"DEBUG_DASK: multiprocessing.cpu_count() = {multiprocessing.cpu_count()}")
#         try:
#             import psutil  # DEBUG_DASK
#             logger.warning(f"DEBUG_DASK: psutil.cpu_count(logical=True) = {psutil.cpu_count(logical=True)}")
#             logger.warning(f"DEBUG_DASK: psutil.cpu_count(logical=False) = {psutil.cpu_count(logical=False)}")
#         except ImportError:
#             pass
#
#         client = Client()  # Current default behavior
#         logger.info(f"Dask client started successfully. You can monitor execution on {client.dashboard_link}")
#     else:
#         client = existing_client
#         logger.debug(f"Reusing existing Dask client: {client.dashboard_link}")
#
#     logger.info(f"Processing will be executed in batches. The chunks size is {batch_size}")
#
#     # DEBUG_DASK: Log worker info
#     logger.warning(f"DEBUG_DASK: Client has {len(client.scheduler_info()['workers'])} workers")
#     for worker_id, worker_info in client.scheduler_info()['workers'].items():
#         logger.warning(f"DEBUG_DASK: Worker {worker_id}: nthreads={worker_info.get('nthreads')}, memory_limit={worker_info.get('memory_limit', 0) / 1024**3:.2f}GB")
#
#     for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
#         batch_list = list(input_batch)
#         logger.warning(f"DEBUG_DASK: Batch {i} has {len(batch_list)} input items")
#         input_batch = batch_list
#
#         if i != 0 and owns_client:
#             logger.info("Restarting client to clear unmanaged memory.")
#             sleep(2)
#             client.restart()
#             logger.warning(f"DEBUG_DASK: Client restarted for batch {i}")
#         logger.info(f"Starting processing of chunk no.{i}")
#         _submit_task_to_client_and_add_results_to_db_ccf(
#             client=client,
#             inputs_to_process=input_batch,
#             calculation_task=calculation_task,
#             upserter_callable=upserter_callable,
#             raise_errors=raise_errors,
#             with_file=with_file,
#             is_beamforming=is_beamforming,
#             is_event_confirmation=is_event_confirmation,
#             overwrite=overwrite,
#         )
#
#     if owns_client:
#         client.close()
#     return


# =============================================================================
# DEPRECATED: _submit_task_to_client_and_add_results_to_db_ccf (commented out for reference)
# This functionality is now integrated into the unified _submit_task_to_client_and_add_results_to_db above.
# The unified function includes data scattering and memory cleanup optimizations.
# =============================================================================
# def _submit_task_to_client_and_add_results_to_db_ccf(
#     client,
#     inputs_to_process: Iterable[InputsForMassCalculations],
#     calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
#     upserter_callable: Callable[[BulkAddableObjects], Insert],
#     raise_errors: bool = False,
#     with_file: bool = False,
#     is_beamforming: bool = False,
#     is_event_confirmation: bool = False,
#     overwrite: bool = True,
# ):
#     # ... CCF-specific implementation now in unified function ...
#     pass


def _run_calculate_and_upsert_sequentially(
    inputs: Iterable[InputsForMassCalculations],
    calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
    upserter_callable: Callable[[BulkAddableObjects], Insert],
    batch_size: int = 1000,
    raise_errors: bool = False,
    with_file: bool = False,
    is_beamforming: bool = False,
    is_event_confirmation: bool = False,
    overwrite: bool = True,
):
    for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
        logger.info(f"Starting processing of chunk no.{i}")
        results_nested = []
        for input_dict in input_batch:
            try:
                results_nested.append(calculation_task(input_dict))
            except CorruptedDataException as e:
                if raise_errors:
                    logger.error(f"Cought error {e}. Finishing execution.")
                    raise CorruptedDataException(e) from e
                else:
                    logger.error(f"Cought error {e}. Skipping to next timespan.")
                    continue
            except InconsistentDataException as e:
                if raise_errors:
                    logger.error(f"Cought error {e}. Finishing execution.")
                    raise InconsistentDataException(e) from e
                else:
                    logger.error(f"Cought error {e}. Skipping to next timespan.")
                    continue
            except ObspyError as e:
                if raise_errors:
                    logger.error(f"Cought error {e}. Finishing execution.")
                    raise ObspyError(e) from e
                else:
                    logger.error(f"Cought error {e}. Skipping to next timespan.")
                    continue
        logger.info("Calculations finished for a batch. Starting upsert operation.")

        results: List[BulkAddableObjects] = list(more_itertools.flatten(results_nested))
        if with_file:
            files_to_add = [x.file for x in results if x is not None and x.file is not None]
            if len(files_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=files_to_add,
                )
                # After files are committed, update the file IDs on the result objects
                # This is necessary because the relationship doesn't auto-update the FK column
                # Dynamically detect the correct file ID attribute based on object type
                for res in results:
                    if res is not None and res.file is not None and res.file.id is not None:
                        if hasattr(res, "beamforming_file_id"):
                            res.beamforming_file_id = res.file.id
                        elif hasattr(res, "crosscorrelation_cylindrical_file_id"):
                            res.crosscorrelation_cylindrical_file_id = res.file.id
                        elif hasattr(res, "crosscorrelation_cartesian_file_id"):
                            res.crosscorrelation_cartesian_file_id = res.file.id
                        elif hasattr(res, "datachunk_file_id"):
                            res.datachunk_file_id = res.file.id
                        elif hasattr(res, "processed_datachunk_file_id"):
                            res.processed_datachunk_file_id = res.file.id
                        elif hasattr(res, "ppsd_file_id"):
                            res.ppsd_file_id = res.file.id

        if is_beamforming:
            peaks_to_add = []
            for res in results:
                peaks_to_add.extend(res.average_abspower_peaks)
                peaks_to_add.extend(res.average_relpower_peaks)
                peaks_to_add.extend(res.all_abspower_peaks)
                peaks_to_add.extend(res.all_relpower_peaks)
            if len(peaks_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=peaks_to_add,
                )

        if is_event_confirmation:
            bulk_merge_or_upsert_objects(
                objects_to_merge=results, upserter_callable=upserter_callable, bulk_insert=True
            )
        else:
            bulk_add_or_upsert_objects(
                objects_to_add=results, upserter_callable=upserter_callable, bulk_insert=True, overwrite=overwrite
            )

    logger.info("All processing is done.")
    return


def _parse_query_as_dataframe(query: Query) -> pd.DataFrame:
    """
    Takes a standard sqlalchemy :py:class:`~sqlalchemy.orm.query.Query`, executes it and parses results as
    a :py:class:`pandas.DataFrame`.

    :param query: QUery to be processed
    :type query: Query
    :return: Results of the query as a DataFrame
    :rtype: pd.DataFrame
    """
    with query.session.bind.connect() as connection:
        result = connection.execute(query.statement)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
