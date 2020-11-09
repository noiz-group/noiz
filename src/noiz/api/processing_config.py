from typing import Optional

from noiz.database import db
from noiz.models.processing_params import DatachunkPreprocessingConfig


def upsert_default_params() -> None:
    default_config = DatachunkPreprocessingConfig(id=1)
    current_config = fetch_processing_config_by_id(1)

    if current_config is not None:
        db.session.merge(default_config)
    else:
        db.session.add(default_config)
    db.session.commit()

    return


def fetch_processing_config_by_id(id: int) -> DatachunkPreprocessingConfig:
    """
    Fetches a DatachunkPreprocessingConfig objects by its ID.
    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched DatachunkPreprocessingConfig object
    :rtype: Optional[DatachunkPreprocessingConfig]
    """
    fetched_params = DatachunkPreprocessingConfig.query.filter_by(id=id).first()
    if fetched_params is None:
        raise ValueError(f"DatachunkPreprocessingConfig object of id {id} does not exist.")

    return fetched_params
