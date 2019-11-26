from typing import Iterable

from sqlalchemy.dialects.postgresql import insert

from noiz.database import db
from noiz.models import StackingTimespan


def insert_stacking_timespans_into_db(
    timespans: Iterable[StackingTimespan], bulk_insert: bool
) -> None:
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
