import datetime

from typing import List, Collection, Union, Optional, Dict

from noiz.api import fetch_components
from noiz.api.helpers import validate_to_tuple, extract_object_ids
from noiz.database import db
from noiz.models import QCOne, QCOneRejectedTime
from noiz.processing.qc import QCOneRejectedTimeHolder, QCOneHolder, validate_dict_as_qcone_holder


def fetch_qc_one(ids: Union[int, Collection[int]]) -> List[QCOne]:

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOne).filter(
        QCOne.id.in_(ids),
    ).all()

    return fetched


def create_qcone_rejected_time(
        holder: QCOneRejectedTimeHolder,
) -> List[QCOneRejectedTime]:

    fetched_components = fetch_components(
        networks=holder.network,
        stations=holder.station,
        components=holder.component,
    )

    res = [
        QCOneRejectedTime(component=cmp, starttime=holder.starttime, endtime=holder.endtime)
        for cmp in fetched_components
    ]

    return res


def create_qcone(
        qcone_holder: Optional[QCOneHolder] = None,
        **kwargs,
) -> QCOne:
    """
    This method takes a :class:`~noiz.processing.qc.QCOneHolder` instance and based on it creates an instance
    of database model :class:`~noiz.models.QCOne`.

    Optionally, it can create the instance of :class:`~noiz.processing.qc.QCOneHolder` from provided kwargs, but
    why dont you do it on your own to ensure that it will get everything it needs?

    :param qcone_holder: Object containing all required elements to create a QCOne instance
    :type qcone_holder: QCOneHolder
    :param kwargs: Optional kwargs to create QCOneHolder
    :return: Working QCOne model that needs to be inserted into db
    :rtype: QCOne
    """

    if qcone_holder is None:
        qcone_holder = validate_dict_as_qcone_holder(kwargs)

    qc_one_rejected_times = []
    for rej_time in qcone_holder.forbidden_channels:
        qc_one_rejected_times.append(create_qcone_rejected_time(holder=rej_time))

    qcone = QCOne(
        starttime=qcone_holder.starttime,
        endtime=qcone_holder.endtime,
        avg_gps_time_error_min=qcone_holder.avg_gps_time_error_min,
        avg_gps_time_error_max=qcone_holder.avg_gps_time_error_max,
        avg_gps_time_uncertainty_min=qcone_holder.avg_gps_time_uncertainty_min,
        avg_gps_time_uncertainty_max=qcone_holder.avg_gps_time_uncertainty_max,
    )
    return qcone


def insert_qc_one_into_db(qcone: QCOne):
    """
    This is method simply adding an instance of :class:`~noiz.models.QCOne` to DB and committing changes.

    :param qcone: Instance of QCOne to be added to db
    :type qcone: QCOne
    :return: None
    :rtype: NoneType
    """
    db.session.add(qcone)
    db.session.commit()
    return
