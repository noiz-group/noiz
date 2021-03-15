from noiz.exceptions import EmptyResultException
from noiz.models.processing_params import PPSDParams


def fetch_ppsd_params_by_id(id: int) -> PPSDParams:
    """
    Fetches a single PPSDParams objects by its ID.

    :param id: ID of PPSDParams to be fetched
    :type id: int
    :return: fetched PPSDParams object
    :rtype: PPSDParams
    """
    fetched_params = PPSDParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"PPSDParams object of id {id} does not exist.")
    return fetched_params
