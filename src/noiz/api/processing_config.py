from loguru import logger
from pathlib import Path
from typing import Optional, Tuple, Union

from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models.processing_params import DatachunkParams, DatachunkParamsHolder, ProcessedDatachunkParams, \
    ProcessedDatachunkParamsHolder
from noiz.processing.configs import parse_single_config_toml, DefinedConfigs, \
    create_datachunkparams, create_processed_datachunk_params


def fetch_datachunkparams_by_id(id: int) -> DatachunkParams:
    """
    Fetches a DatachunkParams objects by its ID.
    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched DatachunkParams object
    :rtype: Optional[DatachunkParams]
    """
    fetched_params = DatachunkParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise ValueError(f"DatachunkParams object of id {id} does not exist.")

    return fetched_params


def fetch_processed_datachunk_params_by_id(id: int) -> ProcessedDatachunkParams:
    """
    Fetches a ProcessedDatachunkParams objects by its ID.
    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched ProcessedDatachunkParams object
    :rtype: ProcessedDatachunkParams
    :raises ValueError
    """
    fetched_params = ProcessedDatachunkParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"ProcessedDatachunkParams object of id {id} does not exist.")
    return fetched_params


def insert_params_into_db(params: Union[DatachunkParams, ProcessedDatachunkParams]):
    """
    This is method simply adding an instance of :py:class:`~noiz.models.DatachunkParams`
    or :py:class:`~noiz.models.ProcessedDatachunkParams` to DB and committing changes.

    Has to be executed within `app_context`

    :param params: Instance of supported params object to be added to db
    :type params: Union[DatachunkParams, ProcessedDatachunkParams]
    :return: None
    :rtype: NoneType
    """
    # TODO make this return id of inserted object and cli to be printing it out
    db.session.add(params)
    db.session.commit()
    logger.info(f"Inserted {type(params)} to db with id {params.id}")
    return


def create_and_add_datachunk_params_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[DatachunkParamsHolder, DatachunkParams]]:
    """
    filldocs
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
    :rtype: Optional[Tuple[DatachunkParamsHolder, DatachunkParams]]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.DATACHUNKPARAMS)
    datachunk_params = create_datachunkparams(params_holder=params_holder)

    if add_to_db:
        insert_params_into_db(params=datachunk_params)
    else:
        return (params_holder, datachunk_params)
    return None


def create_and_add_processed_datachunk_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[ProcessedDatachunkParamsHolder, ProcessedDatachunkParams]]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.PROCESSEDDATACHUNKPARAMS)
    datachunk_params = create_processed_datachunk_params(params_holder=params_holder)

    if add_to_db:
        insert_params_into_db(params=datachunk_params)
    else:
        return (params_holder, datachunk_params)
    return None
