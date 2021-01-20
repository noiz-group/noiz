from typing import Iterable, Generator

import pandas as pd
from loguru import logger
import numpy as np
import sqlalchemy

from noiz.database import db
from noiz.models.component_pair import ComponentPair
from noiz.models.crosscorrelation import Crosscorrelation
from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan
from noiz.models.stacking import StackingSchema, StackingTimespan, CCFStack
from noiz.processing.time_utils import get_year_doy
from noiz.processing.timespan import generate_starttimes_endtimes


def stack_crosscorrelation(
    execution_date, pairs_to_correlate, processing_params_id=1, stacking_schema_id=1
):
    logger.info(f"Fetching processing params no {processing_params_id}")
    processing_params = (
        db.session.query(DatachunkParams)
        .filter(DatachunkParams.id == processing_params_id)
        .first()
    )

    logger.info(f"Starting to stack data for day {execution_date}")
    year, doy = get_year_doy(execution_date)
    stacking_timespans = (
        db.session.query(StackingTimespan).filter(
            StackingTimespan.endtime_year == year,
            StackingTimespan.endtime_doy == doy,
            StackingTimespan.stacking_schema_id == stacking_schema_id,
        )
    ).all()

    no_timespans = len(stacking_timespans)

    logger.info(f"There are {no_timespans} finishing that day")

    for j, stacking_timespan in enumerate(stacking_timespans):
        logger.info(
            f"Starting stacking for {stacking_timespan}. {j + 1}/{no_timespans}"
        )

        logger.info(f"Fetching pairids of componentpairs {pairs_to_correlate}")
        componentpair_ids = (
            db.session.query(Crosscorrelation.componentpair_id)
            .join(Timespan)
            .join(ComponentPair)
            .filter(
                db.and_(
                    Timespan.starttime >= stacking_timespan.starttime,
                    Timespan.endtime <= stacking_timespan.endtime,
                    ComponentPair.component_names.in_(pairs_to_correlate),
                )
            )
            .distinct()
            .all()
        )
        componentpair_ids = [x[0] for x in componentpair_ids]
        no_pairs = len(componentpair_ids)

        logger.info(f"There are {no_pairs} pairs to process")

        for i, pair_id in enumerate(componentpair_ids):
            logger.info(f"Fetching ccfs from pair {i + 1}/{no_pairs}")

            ccfs = (
                db.session.query(Crosscorrelation)
                .join(Timespan)
                .join(ComponentPair)
                .filter(
                    db.and_(
                        Crosscorrelation.datachunk_processing_config_id == processing_params.id,
                        Timespan.starttime >= stacking_timespan.starttime,
                        Timespan.endtime <= stacking_timespan.endtime,
                        Crosscorrelation.componentpair_id == pair_id,
                    )
                )
                .all()
            )

            no_ccfs = len(ccfs)
            logger.info(f"There were {no_ccfs} fetched from db for that stack and pair")

            stacking_threshold = 648
            logger.warning("USING HARDCODED STACKING LIMIT THRESHOLD!")
            # TODO MAKE IT PARAMETRIZED THOURGH PROCESSING PARAMS!

            if no_ccfs < stacking_threshold:
                logger.info(
                    f"There only {no_ccfs} ccfs in stack. The minimum number of ccfs for stack is {stacking_threshold}."
                    f" Skipping."
                )
                continue

            logger.info("Calculating linear stack")
            mean_ccf = np.array([x.ccf for x in ccfs]).mean(axis=0)

            stack = CCFStack(
                stacking_timespan_id=stacking_timespan.id,
                stack=mean_ccf,
                componentpair_id=pair_id,
                no_ccfs=no_ccfs,
                ccfs=ccfs,
            )

            logger.info("Inserting into db")
            try:
                db.session.add(stack)
                db.session.commit()
            except sqlalchemy.exc.IntegrityError:
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


def generate_stacking_timespans(stacking_schema: StackingSchema) -> Generator[StackingTimespan, None, None]:

    timespans = generate_starttimes_endtimes(
        startdate=stacking_schema.starttime,
        enddate=stacking_schema.endtime,
        window_length=pd.Timedelta(stacking_schema.stacking_length),
        window_overlap=pd.Timedelta(stacking_schema.stacking_overlap),
        generate_midtimes=True,
    )

    for starttime, midtime, endtime in zip(*timespans):
        yield StackingTimespan(
            starttime=starttime,
            midtime=midtime,
            endtime=endtime,
            stacking_schema_id=stacking_schema.id,
        )
