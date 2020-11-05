import datetime

from typing import List, Collection, Union, Optional

from noiz.api import fetch_components
from noiz.api.helpers import validate_to_tuple, extract_object_ids
from noiz.database import db
from noiz.models import QCOne, QCOneRejectedTime


def fetch_qc_one(ids: Union[int, Collection[int]]) -> List[QCOne]:

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOne).filter(
        QCOne.id.in_(ids),
    ).all()

    return fetched


def create_qcone_rejected_time(
        starttime: datetime.datetime,
        endtime: datetime.datetime,
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
):
    fetched_components = fetch_components(networks=networks, stations=stations, components=components)

    return [QCOneRejectedTime(component=cmp, starttime=starttime, endtime=endtime) for cmp in fetched_components]
