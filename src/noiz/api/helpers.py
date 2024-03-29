# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
import more_itertools
import pandas as pd
from time import sleep
from sqlalchemy.orm import Query
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.sql import Insert
from typing import Iterable, Union, List, Tuple, Any, Collection, Callable, get_args, Dict, TypeVar

from noiz.database import db
from noiz.exceptions import CorruptedDataException, InconsistentDataException, ObspyError
from noiz.models.beamforming import BeamformingResultDatchunksAssociation
from noiz.models.type_aliases import BulkAddableObjects, InputsForMassCalculations, BulkAddableFileObjects


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
    if not isinstance(instances, Iterable):
        instances = list(instances)
    ids = [x.id for x in instances]
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
    if not isinstance(instances, Iterable):
        instances = list(instances)
    return {x.id: x for x in instances}


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
) -> None:
    """
    Adds in bulk or upserts provided Collection of objects to DB.

    :param objects_to_add: Objects to be added to database
    :type objects_to_add: Collection[BulkAddableObjects]
    :param upserter_callable: Callable with upsert method to be used in case of bulk add failure
    :type upserter_callable: Callable[[Collection[BulkAddableObjects]], None]
    :param bulk_insert: If bulk add should be even attempted
    :type bulk_insert: bool
    :return: None
    :rtype: NoneType
    """

    if isinstance(objects_to_add, Collection):
        valid_objects = objects_to_add
    else:
        valid_objects = (objects_to_add,)

    if bulk_insert:
        logger.debug("Trying to do bulk insert")
        try:
            bulk_add_objects(valid_objects)
        except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning("Retrying with upsert")
            _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    else:
        logger.info("Starting to perform careful upsert")
        _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    return


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


def _run_upsert_commands(
        objects_to_add: Collection[BulkAddableObjects],
        upserter_callable: Callable[[BulkAddableObjects], Insert]
) -> None:

    logger.info(f"Starting upsert procedure. There are {len(objects_to_add)} elements to be processed.")
    insert_commands = []
    for results in objects_to_add:

        if not isinstance(results, get_args(BulkAddableObjects)):
            logger.warning(f'Provided object is not an instance of any of the subtypes of {BulkAddableObjects}. '
                           f'Provided object was an {type(results)}. '
                           f'Content of the object: {results}'
                           f'Skipping.')
            continue

        logger.debug(f"Generating upsert command for {results}")
        insert_command = upserter_callable(results)
        insert_commands.append(insert_command)

    for insert_command in insert_commands:
        db.session.execute(insert_command)

    logger.debug('Commiting session.')
    db.session.commit()


def _run_calculate_and_upsert_on_dask(
        inputs: Iterable[InputsForMassCalculations],
        calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
        upserter_callable: Callable[[BulkAddableObjects], Insert],
        batch_size: int = 5000,
        raise_errors: bool = False,
        with_file: bool = False,
        is_beamforming: bool = False,
        is_event_confirmation: bool = False,
):
    from dask.distributed import Client
    client = Client()
    logger.info(f'Dask client started successfully. '
                f'You can monitor execution on {client.dashboard_link}')
    logger.info(f"Processing will be executed in batches. The chunks size is {batch_size}")
    for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
        if i != 0:
            logger.info("Restarting client to clear unmanaged memory.")
            # Prevents client.restart() crash if exception occured in the previous tasks.
            sleep(2)
            client.restart()
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
        )
    client.close()
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
):
    from dask.distributed import as_completed

    logger.info("Submitting tasks to Dask client")
    futures = []
    for input_dict in inputs_to_process:
        try:
            futures.append(client.submit(calculation_task, input_dict))
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
        except ValueError as e:
            logger.error(e)
            raise e
    logger.info(f"There are {len(futures)} tasks to be executed")

    logger.info("Starting execution. Results will be saved to database on the fly. ")

    for future_batch in as_completed(futures, with_results=True, raise_errors=False).batches():
        results_nested: List[Tuple[BulkAddableObjects, ...]] = [x[1] for x in future_batch if x[0].status != "error"]
        results: List[BulkAddableObjects] = list(more_itertools.flatten(results_nested))
        logger.info(f"Running bulk_add_or_upsert for {len(results)} results")
        datachunk_assocs = []

        if with_file:
            files_to_add = [x.file for x in results if x.file is not None]
            if len(files_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=files_to_add,
                )
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

            for res in results:
                for _ in range(len(res.datachunks)):
                    chunk = res.datachunks.pop()

                    datachunk_assocs.append(
                        BeamformingResultDatchunksAssociation(
                            beamfroming_result=res,
                            datachunk=chunk,
                        )
                    )

        if is_event_confirmation:
            bulk_merge_or_upsert_objects(
                objects_to_merge=results,
                upserter_callable=upserter_callable,
                bulk_insert=True
            )
        else:
            bulk_add_or_upsert_objects(
                objects_to_add=results,
                upserter_callable=upserter_callable,
                bulk_insert=True
            )

        if len(datachunk_assocs) > 0:
            bulk_add_and_check_objects(
                objects_to_add=datachunk_assocs,
            )

    return


def _run_calculate_and_upsert_sequentially(
        inputs: Iterable[InputsForMassCalculations],
        calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
        upserter_callable: Callable[[BulkAddableObjects], Insert],
        batch_size: int = 1000,
        raise_errors: bool = False,
        with_file: bool = False,
        is_beamforming: bool = False,
        is_event_confirmation: bool = False,
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
            except ObspyError as e:
                if raise_errors:
                    logger.error(f"Cought error {e}. Finishing execution.")
                    raise ObspyError(e)
                else:
                    logger.error(f"Cought error {e}. Skipping to next timespan.")
                    continue
        logger.info("Calculations finished for a batch. Starting upsert operation.")

        results: List[BulkAddableObjects] = list(more_itertools.flatten(results_nested))
        if with_file:
            files_to_add = [x.file for x in results if x.file is not None]
            if len(files_to_add) > 0:
                bulk_add_and_check_objects(
                    objects_to_add=files_to_add,
                )

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
                objects_to_merge=results,
                upserter_callable=upserter_callable,
                bulk_insert=True
            )
        else:
            bulk_add_or_upsert_objects(
                objects_to_add=results,
                upserter_callable=upserter_callable,
                bulk_insert=True
            )

    logger.info("All processing is done.")
    return


def _parse_query_as_dataframe(query: Query) -> pd.DataFrame:
    """
    Takes a standard sqlalchemy :py:class:`~sqlalchemy.orm.query.Query`, executes it and parses results as
    a :py:class:`pandas.DataFrame`.

    :param query: Query to be processed
    :type query: Query
    :return: Results of the query as a DataFrame
    :rtype: pd.DataFrame
    """
    connectable = query.session.get_bind()
    c = query.statement.compile(connectable, compile_kwargs={"render_postcompile": True})
    df = pd.read_sql(c.string, connectable, params=c.params)
    return df
