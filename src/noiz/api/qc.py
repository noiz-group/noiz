from pathlib import Path

from typing import List, Collection, Union, Optional, Tuple

from noiz.api.component import fetch_components
from noiz.api.helpers import validate_to_tuple
from noiz.database import db
from noiz.models.qc import QCOneConfig, QCOneRejectedTime, QCOneRejectedTimeHolder, QCOneHolder
from noiz.processing.configs import validate_dict_as_qcone_holder, load_qc_one_config_toml


def fetch_qc_one(ids: Union[int, Collection[int]]) -> List[QCOneConfig]:
    """
    Fetches the QCOne from db based on id.

    :param ids: IDs to be fetched
    :type ids: Union[int, Collection[int]]
    :return: Fetched QCones
    :rtype: List[QCOneConfig]
    """

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOneConfig).filter(
        QCOneConfig.id.in_(ids),
    ).all()

    return fetched


def create_qcone_rejected_time(
        holder: QCOneRejectedTimeHolder,
) -> List[QCOneRejectedTime]:
    """
    Based on provided :class:`~noiz.processing.qc.QCOneRejectedTimeHolder` creates instances of the
    database models :class:`~noiz.models.QCOneRejectedTime`.

    Since the holder itself is focused on the single component inputs, it should never return more than a
    single element list but for safety, it will return a list instead of single object.

    :param holder: Holder to be processed
    :type holder: QCOneRejectedTimeHolder
    :return: Instance of a model, ready to be added to to the database
    :rtype: QCOneRejectedTime
    """

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


def create_qcone_config(
        qcone_holder: Optional[QCOneHolder] = None,
        **kwargs,
) -> QCOneConfig:
    """
    This method takes a :class:`~noiz.processing.qc.QCOneHolder` instance and based on it creates an instance
    of database model :class:`~noiz.models.QCOneConfig`.

    Optionally, it can create the instance of :class:`~noiz.processing.qc.QCOneHolder` from provided kwargs, but
    why dont you do it on your own to ensure that it will get everything it needs?

    Has to be executed within `app_context`

    :param qcone_holder: Object containing all required elements to create a QCOne instance
    :type qcone_holder: QCOneHolder
    :param kwargs: Optional kwargs to create QCOneHolder
    :return: Working QCOne model that needs to be inserted into db
    :rtype: QCOneConfig
    """

    if qcone_holder is None:
        qcone_holder = validate_dict_as_qcone_holder(kwargs)

    qc_one_rejected_times = []
    for rej_time in qcone_holder.rejected_times:
        qc_one_rejected_times.extend(create_qcone_rejected_time(holder=rej_time))

    qcone = QCOneConfig(
        starttime=qcone_holder.starttime,
        endtime=qcone_holder.endtime,
        avg_gps_time_error_min=qcone_holder.avg_gps_time_error_min,
        avg_gps_time_error_max=qcone_holder.avg_gps_time_error_max,
        avg_gps_time_uncertainty_min=qcone_holder.avg_gps_time_uncertainty_min,
        avg_gps_time_uncertainty_max=qcone_holder.avg_gps_time_uncertainty_max,
    )
    return qcone


def insert_qc_one_config_into_db(qcone_config: QCOneConfig):
    """
    This is method simply adding an instance of :class:`~noiz.models.QCOneConfig` to DB and committing changes.

    Has to be executed within `app_context`

    :param qcone_config: Instance of QCOne to be added to db
    :type qcone_config: QCOneConfig
    :return: None
    :rtype: NoneType
    """
    db.session.add(qcone_config)
    db.session.commit()
    return


def create_and_add_qc_one_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[QCOneHolder, QCOneConfig]]:
    """
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.processing.qc.QCOneHolder` and subsequently :class:`~noiz.models.QCOneConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.processing.qc.QCOneHolder`
    and :class:`~noiz.models.QCOneConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCOneHolder object for manual validation
    :rtype: Optional[QCOneHolder]
    """

    qcone_holder = load_qc_one_config_toml(filepath=filepath)
    qcone_config = create_qcone_config(qcone_holder=qcone_holder)

    if add_to_db:
        insert_qc_one_config_into_db(qcone_config=qcone_config)
    else:
        return (qcone_holder, qcone_config)
    return None
