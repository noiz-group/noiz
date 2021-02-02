from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.sql import Insert
from typing import Iterable, Union, List, Tuple, Type, Any, Optional, Collection, Callable, get_args, TypedDict

from noiz.database import db
from noiz.models import Crosscorrelation, CCFStack, DatachunkStats, ProcessedDatachunk, QCOneResults, QCTwoResults


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


BulkAddableObjects = Union[
        Crosscorrelation,
        CCFStack,
        DatachunkStats,
        ProcessedDatachunk,
        QCOneResults,
        QCTwoResults,
    ]


def bulk_add_objects(objects_to_add: Collection[BulkAddableObjects]) -> None:
    """
    Tries to perform bulk insert of objects to database.

    :param objects_to_add: Objects to be inserted to the db
    :type objects_to_add: BulkAddableObjects
    :return: None
    :rtype: None
    """
    logger.info("Performing bulk add_all operation")
    db.session.add_all(objects_to_add)
    logger.info("Committing")
    db.session.commit()
    return


class BulkAddOrUpsertObjectsInputs(TypedDict):
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]]
    upserter_callable: Callable[[BulkAddableObjects], Insert]
    bulk_insert: bool


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
        logger.info("Trying to do bulk insert")
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
