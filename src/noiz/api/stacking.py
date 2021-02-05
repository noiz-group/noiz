import datetime
import itertools
from loguru import logger
from sqlalchemy.sql import Insert

from noiz.api.helpers import bulk_add_objects, _run_calculate_and_upsert_on_dask, _run_calculate_and_upsert_sequentially
from noiz.api.type_aliases import StackingInputs
from noiz.exceptions import MissingProcessingStepError
from obspy import UTCDateTime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert
from typing import Collection, Union, List, Optional, Tuple, Generator

from noiz.api.component_pair import fetch_componentpairs
from noiz.api.qc import fetch_qctwo_config_single, count_qctwo_results
from noiz.database import db
from noiz.models import Crosscorrelation, StackingTimespan, Timespan, CCFStack, \
    QCTwoResults, ComponentPair, StackingSchema
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
        batch_size: int = 5000,
        parallel: bool = True,
) -> None:
    calculation_inputs = _prepare_inputs_for_stacking_ccfs(
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
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_validate_and_stack_ccfs_wrapper,  # type: ignore
            upserter_callable=_generate_ccfstack_upsert_command,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_validate_and_stack_ccfs_wrapper,  # type: ignore
            upserter_callable=_generate_ccfstack_upsert_command,
            raise_errors=raise_errors,
        )

    return


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

    if count_qctwo_results(qctwo_config=qctwo_config) == 0:
        raise MissingProcessingStepError("There are no QCTwo results for that QCTwoConfig. Are you sure you ran QCTwo "
                                         "before?")

    for stacking_timespan, componentpair in itertools.product(stacking_timespans, componentpairs):
        fetched_qc_ccfs = (
            db.session.query(QCTwoResults, Crosscorrelation)
            .filter(QCTwoResults.qctwo_config_id == qctwo_config.id)
            .join(Crosscorrelation, QCTwoResults.crosscorrelation_id == Crosscorrelation.id)
            .join(Timespan, Crosscorrelation.timespan_id == Timespan.id)
            .filter(
                Crosscorrelation.componentpair_id == componentpair.id,
                Timespan.starttime >= stacking_timespan.starttime,
                Timespan.endtime <= stacking_timespan.endtime,
            )
            .all()
        )

        if len(fetched_qc_ccfs) == 0:
            logger.debug("There were no ccfs for that stack")
            continue

        yield StackingInputs(
            qctwo_ccfs_container=fetched_qc_ccfs,
            componentpair=componentpair,
            stacking_schema=stacking_schema,
            stacking_timespan=stacking_timespan
        )


def _validate_and_stack_ccfs_wrapper(
        inputs: StackingInputs,
) -> Tuple[Optional[CCFStack], ...]:
    return (
        _validate_and_stack_ccfs(
            qctwo_ccfs_container=inputs["qctwo_ccfs_container"],
            componentpair=inputs["componentpair"],
            stacking_schema=inputs["stacking_schema"],
            stacking_timespan=inputs["stacking_timespan"],
        ),
    )


def _validate_and_stack_ccfs(
        qctwo_ccfs_container: List[Tuple[QCTwoResults, Crosscorrelation]],
        componentpair: ComponentPair,
        stacking_schema: StackingSchema,
        stacking_timespan: StackingTimespan,
) -> Optional[CCFStack]:
    """
    Takes container of tuples with QCTwoResults and Crosscorrelation (the same crosscorrelation_id),
    verifies if Crosscorrelation is passing the QCTwo and if yes, it stacks it.

    Before stacking it verifies if there is enough Crosscorrelations to be stacked, it can be adjusted by setting
    a value of :paramref:`noiz.models.stacking.StackingSchema.minimum_ccf_count`.

    It returns an instance of :py:class:`~noiz.models.stacking.CCFStack` that is ready to be inserted to the database.

    :param qctwo_ccfs_container: Crosscorrelations to be stacked together with associated QCTwoResult instances
    :type qctwo_ccfs_container: List[Tuple[QCTwoResults, Crosscorrelation]]
    :param componentpair: ComponentPair for which the stack is done
    :type componentpair: ComponentPair
    :param stacking_schema: StackingSchema defining that stack
    :type stacking_schema: StackingSchema
    :param stacking_timespan: StackingTimespan that is defining that stack
    :type stacking_timespan: StackingTimespan
    :return: Returns None if Crosscorrelations cannot be stacked or CCFStack if they can
    :rtype: Optional[CCFStack]
    """

    valid_ccfs = _validate_crosscorrelations_with_qctwo(qctwo_ccfs_container)

    no_ccfs = len(valid_ccfs)
    logger.debug(f"There are {no_ccfs} valid ccfs for that stack")
    if no_ccfs < stacking_schema.minimum_ccf_count:
        logger.debug(
            f"There only {no_ccfs} ccfs to be stacked. "
            f"The minimum number of ccfs for stack to be valid is {stacking_schema.minimum_ccf_count}."
            f" Skipping."
        )
        return None

    logger.debug(f"Calculating linear stack for {componentpair} {stacking_schema} {stacking_timespan}")
    mean_ccf = do_linear_stack_of_crosscorrelations(ccfs=valid_ccfs)

    stack = CCFStack(
        stacking_timespan_id=stacking_timespan.id,
        stacking_schema_id=stacking_schema.id,
        stack=mean_ccf,
        componentpair_id=componentpair.id,
        no_ccfs=no_ccfs,
        ccfs=list(valid_ccfs),
    )
    return stack


def _validate_crosscorrelations_with_qctwo(
        qctwo_ccfs_container: Collection[Tuple[QCTwoResults, Crosscorrelation]]
) -> Tuple[Crosscorrelation, ...]:
    """
    Checks if which Crosscorrelations are passing QCTwo.
    It accepts as input a Collection of Tuples with QCTwoResults and Crosscorrelation.
    It outputs a tuple containing only those Crosscorrelation objects that are passing QCTwo.

    :param qctwo_ccfs_container: Container of tuples with QCTwoResults and Crosscorrelations to be verified
    :type qctwo_ccfs_container: Collection[Tuple[QCTwoResults, Crosscorrelation]]
    :return: Valid Crosscorrelation objects
    :rtype: Tuple[Crosscorrelation, ...]
    """

    valid_ccfs = []
    for qcres, ccf in qctwo_ccfs_container:
        if not qcres.is_passing():
            continue
        valid_ccfs.append(ccf)

    return tuple(valid_ccfs)


def _generate_ccfstack_upsert_command(
        stack: CCFStack
) -> Insert:
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
            set_=dict(
                stack=stack.stack,
                no_ccfs=stack.no_ccfs,
            ),
        )
    )
    return insert_command
