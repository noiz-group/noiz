from loguru import logger

from noiz.models.qc import QCOneConfig, QCOneHolder
from pathlib import Path
from typing import Optional, Tuple, Union

from noiz.database import db
from noiz.models.processing_params import DatachunkParams, DatachunkParamsHolder, ProcessedDatachunkParams
from noiz.processing.configs import validate_config_dict_as_datachunkparams, parse_single_config_toml, DefinedConfigs


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


def create_datachunkparams(
        params_holder: Optional[DatachunkParamsHolder] = None,
        **kwargs,
) -> DatachunkParams:
    """
    This method takes a :class:`~noiz.models.processing_params.DatachunkParamsHolder` instance and based on it creates
    an instance of database model :class:`~noiz.models.processing_params.DatachunkParams`.

    Optionally, it can create the instance of :class:`~noiz.models.processing_params.DatachunkParamsHolder` from
    provided kwargs, but why dont you do it on your own to ensure that it will get everything it needs?

    Has to be executed within `app_context`

    :param params_holder: Object containing all required elements to create a DatachunkParams instance
    :type params_holder: DatachunkParamsHolder
    :param kwargs: Optional kwargs to create DatachunkParamsHolder
    :return: Working DatachunkParams model that needs to be inserted into db
    :rtype: DatachunkParams
    """

    if params_holder is None:
        params_holder = validate_config_dict_as_datachunkparams(kwargs)

    params = DatachunkParams(
        sampling_rate=params_holder.sampling_rate,
        prefiltering_low=params_holder.prefiltering_low,
        prefiltering_high=params_holder.prefiltering_high,
        prefiltering_order=params_holder.prefiltering_order,
        preprocessing_taper_type=params_holder.preprocessing_taper_type,
        preprocessing_taper_side=params_holder.preprocessing_taper_side,
        preprocessing_taper_max_length=params_holder.preprocessing_taper_max_length,
        preprocessing_taper_max_percentage=params_holder.preprocessing_taper_max_percentage,
        remove_response=params_holder.remove_response,
        datachunk_sample_tolerance=params_holder.datachunk_sample_tolerance,
        zero_padding_method=params_holder.zero_padding_method,
        padding_taper_type=params_holder.padding_taper_type,
        padding_taper_max_length=params_holder.padding_taper_max_length,
        padding_taper_max_percentage=params_holder.padding_taper_max_percentage,
    )
    return params


def insert_params_into_db(params: Union[DatachunkParams, ProcessedDatachunkParams]):
    """
    This is method simply adding an instance of :class:`~noiz.models.DatachunkParams` to DB and committing changes.

    Has to be executed within `app_context`

    :param params: Instance of DatachunkParams to be added to db
    :type params: DatachunkParams
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

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.DATACHUNKPARAMS)
    datachunk_params = create_datachunkparams(params_holder=params_holder)

    if add_to_db:
        insert_params_into_db(params=datachunk_params)
    else:
        return (params_holder, datachunk_params)
    return None
