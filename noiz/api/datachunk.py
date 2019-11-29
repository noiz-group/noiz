from typing import List
from noiz.models import Datachunk, Timespan


def fetch_datachunks_for_timespan(timespan: Timespan) -> List[Datachunk]:
    ret = Datachunk.query.filter(Datachunk.timespan_id == timespan.id).all()
    return ret
