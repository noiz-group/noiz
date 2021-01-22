import datetime
import itertools
from loguru import logger
from sqlalchemy.exc import IntegrityError

from noiz.api.component_pair import fetch_componentpairs
from sqlalchemy.dialects.postgresql import insert
from typing import Collection, Union, List, Optional

from noiz.api.qc import fetch_qctwo_config_single
from obspy import UTCDateTime

from noiz.database import db
from noiz.models import StackingTimespan, Crosscorrelation, Timespan, CCFStack, \
    QCTwoResults
from noiz.api.processing_config import fetch_stacking_schema_by_id
from noiz.processing.stacking import _generate_stacking_timespans, do_linear_stack_of_crosscorrelations


def fetch_stacking_timespans(
        stacking_schema_id: int,
        starttime: Optional[Union[datetime.date, datetime.datetime, UTCDateTime]] = None,
        endtime: Optional[Union[datetime.date, datetime.datetime, UTCDateTime]] = None,
) -> List[StackingTimespan]:

    if starttime is not None:
        if isinstance(starttime, UTCDateTime):
            starttime = starttime.datetime
        elif not isinstance(starttime, (datetime.date, datetime.datetime)):
            raise ValueError(f"And starttime was expecting either "
                             f"datetime.date, datetime.datetime or UTCDateTime objects."
                             f"Got instance of {type(starttime)}")

    if endtime is not None:
        if isinstance(endtime, UTCDateTime):
            endtime = endtime.datetime
        elif not isinstance(endtime, (datetime.date, datetime.datetime)):
            raise ValueError(f"And endtime was expecting either "
                             f"datetime.date, datetime.datetime or UTCDateTime objects."
                             f"Got instance of {type(endtime)}")

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


def stack_crosscorrelation(
        stacking_schema_id=1,
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

):
    stacking_schema = fetch_stacking_schema_by_id(id=stacking_schema_id)

    stacking_timespans = fetch_stacking_timespans(
        stacking_schema_id=stacking_schema.id,
        starttime=starttime,
        endtime=endtime,
    )
    no_timespans = len(stacking_timespans)
    logger.info(f"There are {no_timespans} to stack for")

    qctwo_config = fetch_qctwo_config_single(id=stacking_schema.qctwo_config_id)

    componentpairs = fetch_componentpairs(
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
    logger.info(f"There are {len(componentpairs)} ComponentPairs to stack for")

    for stacking_timespan, pair in itertools.product(stacking_timespans, componentpairs):

        fetched_qc_ccfs = (
            db.session.query(QCTwoResults, Crosscorrelation)
            .filter(QCTwoResults.qctwo_config_id == qctwo_config.id)
            .join(Crosscorrelation, QCTwoResults.crosscorrelation_id == Crosscorrelation.id)
            .join(Timespan, Crosscorrelation.timespan_id == Timespan.id)
            .filter(
                Crosscorrelation.componentpair_id == pair.id,
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
            .all()
        )

        if len(fetched_qc_ccfs) == 0:
            continue

        valid_ccfs = []
        for qcres, ccf in fetched_qc_ccfs:
            if not qcres.is_passing():
                continue
            valid_ccfs.append(ccf)

        no_ccfs = len(valid_ccfs)
        logger.debug(f"There are {no_ccfs} valid ccfs for that stack")

        if no_ccfs < stacking_schema.minimum_ccf_count:
            logger.debug(
                f"There only {no_ccfs} ccfs to be stacked. "
                f"The minimum number of ccfs for stack to be valid is {stacking_schema.minimum_ccf_count}."
                f" Skipping."
            )
            continue

        logger.info("Calculating linear stack")
        mean_ccf = do_linear_stack_of_crosscorrelations(ccfs=valid_ccfs)

        stack = CCFStack(
            stacking_timespan_id=stacking_timespan.id,
            stack=mean_ccf,
            componentpair_id=pair.id,
            no_ccfs=no_ccfs,
            ccfs=valid_ccfs,
        )

        logger.info("Inserting into db")
        try:
            db.session.add(stack)
            db.session.commit()
        except IntegrityError:
            logger.error(
                "There was integrity error. Trying to update existing stack."
            )
            db.session.rollback()

            db.session.query(CCFStack).filter(
                CCFStack.stacking_timespan_id == stack.stacking_timespan_id,
                CCFStack.componentpair_id == stack.componentpair_id,
            ).update(dict(stack=stack.stack, no_ccfs=stack.no_ccfs))
            db.session.commit()
        logger.info("Commit successful. Next")
        logger.info("That was everything. Finishing")
    return
