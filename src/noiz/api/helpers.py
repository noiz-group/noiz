import more_itertools
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.sql import Insert
from typing import Iterable, Union, List, Tuple, Type, Any, Optional, Collection, Callable, get_args

from noiz.api.type_aliases import BulkAddableObjects, InputsForMassCalculations, BulkAddOrUpsertObjectsInputs
from noiz.database import db


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


def validate_to_tuple(
        val: Union[Tuple[Any], Any],
        accepted_type: Type,
) -> Tuple:
    """
    Method that checks if provided argument is a str, int or float or a tuple and returns
    the same thing but converted to a single element tuple

    :param val: Value to be validated
    :type val: Union[Tuple, str, int, float]
    :param accepted_type: Type to validate val against
    :type accepted_type: Type
    :return: Input tuple or a single element tuple with input val
    :rtype: Tuple
    """
    if isinstance(val, accepted_type):
        return (val,)
    elif isinstance(val, tuple):
        validate_uniformity_of_tuple(val=val, accepted_type=accepted_type)
        return val
    else:
        raise ValueError(
            f"Expecting a tuple or a single value of type {accepted_type}. Provided value was {type(val)}"
        )


def validate_uniformity_of_tuple(
        val: Tuple[Any, ...],
        accepted_type: Type,
        raise_errors: bool = True,
) -> bool:
    """
    Checks if all elements of provided tuple are of the same type.
    It can raise error or return False in case of negative validation.
    Returns true if tuple is uniform.

    :param val: Tuple to be checked for uniformity
    :type val: Tuple[Any, ...]
    :param accepted_type: Accepted type
    :type accepted_type: Type
    :param raise_errors: If errors should be raised
    :type raise_errors: bool
    :return: If provided tuple is uniform.
    :rtype: bool
    :raises: ValueError
    """

    types: List[Type] = []

    for item in val:
        types.append(type(item))
        if not isinstance(item, accepted_type):
            if raise_errors:
                raise ValueError(f'Values inside of provided tuple should be of type: {accepted_type}. '
                                 f'Value {item} is of type {type(item)}. ')
            else:
                return False

    if not len(list(set(types))) == 1:
        if raise_errors:
            raise ValueError(f"Type of values inside of tuple should be uniform. "
                             f"Inside of tuple {val} there were types: {set(types)}")
        else:
            return False

    return True


def validate_exactly_one_argument_provided(
        first: Optional[Any],
        second: Optional[Any],
) -> bool:
    """
    Method that checks if exactly one of provided arguments is not None.

    :param first: First value to check
    :type first: Optional[Any]
    :param second: Second value to check
    :type second: Optional[Any]
    :return: True if only one value is not None
    :rtype: bool
    :raises: ValueError
    """

    if (second is None and first is None) or (second is not None and first is not None):
        raise ValueError('There has to be either main_filepath or filepaths provided.')
    else:
        return True


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
        except (IntegrityError, UnmappedInstanceError) as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning("Retrying with upsert")
            _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
    else:
        logger.info("Starting to perform careful upsert")
        _run_upsert_commands(objects_to_add=valid_objects, upserter_callable=upserter_callable)
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
):
    from dask.distributed import Client
    client = Client()
    logger.info(f'Dask client started successfully. '
                f'You can monitor execution on {client.dashboard_link}')
    logger.info(f"Processing will be executed in batches. The chunks size is {batch_size}")
    for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
        logger.info(f"Starting processing of chunk no.{i}")

        _submit_task_to_client_and_add_results_to_db(
            client=client,
            inputs_to_process=input_batch,
            calculation_task=calculation_task,
            upserter_callable=upserter_callable,
        )
    client.close()


def _submit_task_to_client_and_add_results_to_db(
        client,
        inputs_to_process: Iterable[InputsForMassCalculations],
        calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
        upserter_callable: Callable[[BulkAddableObjects], Insert],
):
    from dask.distributed import as_completed

    logger.info("Submitting tasks to Dask client")
    futures = []
    try:
        for input_dict in inputs_to_process:
            future = client.submit(calculation_task, input_dict)
            futures.append(future)
    except ValueError as e:
        logger.error(e)
        raise e
    logger.info(f"There are {len(futures)} tasks to be executed")

    logger.info("Starting execution. Results will be saved to database on the fly. ")
    for future_batch in as_completed(futures, with_results=True, raise_errors=False).batches():
        results_nested: List[Tuple[BulkAddableObjects, ...]] = [x[1] for x in future_batch]
        results: List[BulkAddableObjects] = list(more_itertools.flatten(results_nested))
        logger.info(f"Running bulk_add_or_upsert for {len(results)} results")

        kwargs = BulkAddOrUpsertObjectsInputs(
            objects_to_add=results,
            upserter_callable=upserter_callable,
            bulk_insert=True,
        )
        bulk_add_or_upsert_objects(**kwargs)


def _run_calculate_and_upsert_sequentially(
        inputs: Iterable[InputsForMassCalculations],
        calculation_task: Callable[[InputsForMassCalculations], Tuple[BulkAddableObjects, ...]],
        upserter_callable: Callable[[BulkAddableObjects], Insert],
        batch_size: int = 1000,
):

    for i, input_batch in enumerate(more_itertools.chunked(iterable=inputs, n=batch_size)):
        logger.info(f"Starting processing of chunk no.{i}")
        results_nested = []
        for input_dict in input_batch:
            results_nested.append(calculation_task(input_dict))
        logger.info("Calculations finished for a batch. Starting upsert operation.")

        results: List[BulkAddableObjects] = list(more_itertools.flatten(results_nested))
        bulk_add_or_upsert_objects(
            objects_to_add=results,
            upserter_callable=upserter_callable,
            bulk_insert=True
        )
    logger.info("All processing is done.")
