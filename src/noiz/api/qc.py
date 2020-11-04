from typing import List, Collection, Union

from noiz.api.helpers import validate_to_tuple
from noiz.database import db
from noiz.models import QCOne


def fetch_qc_one(ids: Union[int, Collection[int]]) -> List[QCOne]:

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOne).filter(
        QCOne.id.in_(ids),
    ).all()

    return fetched
