from typing import Generator

import pandas as pd

from noiz.models.stacking import StackingSchema, StackingTimespan
from noiz.processing.timespan import generate_starttimes_endtimes


def _generate_stacking_timespans(stacking_schema: StackingSchema) -> Generator[StackingTimespan, None, None]:
    """
    Generates StackingTimespan objects based on provided StackingSchema

    :param stacking_schema: StackingSchema on which generation should be based
    :type stacking_schema: StackingSchema
    :return: Generator with all possible StackingTimespans for that schema
    :rtype: Generator[StackingTimespan, None, None]
    """

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
