from loguru import logger
from sqlalchemy.dialects.postgresql import insert
from typing import Collection

from noiz.database import db
from noiz.models.stacking import StackingTimespan
from noiz.api.processing_config import fetch_stacking_schema_by_id
from noiz.processing.stacking import generate_stacking_timespans


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
    stacking_timespans = list(generate_stacking_timespans(stacking_schema=stacking_schema))
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
            update_dict = dict(
                starttime=ts.starttime,
                midtime=ts.midtime,
                endtime=ts.endtime,
                stacking_schema_id=ts.stacking_schema_id,
            )
            insert_command = (
                insert(StackingTimespan)
                .values(
                    starttime=ts.starttime,
                    midtime=ts.midtime,
                    endtime=ts.endtime,
                    stacking_schema_id=ts.stacking_schema_id,
                )
                .on_conflict_do_update(
                    constraint="unique_stack_starttime", set_=update_dict
                )
                .on_conflict_do_update(
                    constraint="unique_stack_midtime", set_=update_dict
                )
                .on_conflict_do_update(
                    constraint="unique_stack_endtime", set_=update_dict
                )
                .on_conflict_do_update(
                    constraint="unique_stack_times", set_=update_dict
                )
            )
            con.execute(insert_command)
