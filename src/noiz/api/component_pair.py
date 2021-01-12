from loguru import logger
from noiz.api.helpers import extract_object_ids, validate_to_tuple
from typing import Iterable, Optional, List, Union, Collection
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, subqueryload

from noiz.api.component import fetch_components
from noiz.database import db
from noiz.models.component import Component
from noiz.models.component_pair import ComponentPair
from noiz.processing.component_pair import (
    prepare_componentpairs,
)


def upsert_componentpairs(component_pairs: List[ComponentPair]) -> None:
    """
    Takes iterable of ComponentPairs and inserts it into database.
    In case of conflict on `single_component_pair` constraint, updates the entry.

    Warning: Used UPSERT operation is PostgreSQL specific due to used SQLAlchemy command.
    Warning: Has to be run within application context.

    :param component_pairs: List of ComponentPairs to be upserted into db
    :type component_pairs: List[ComponentPair]
    :return: None
    :rtype: None
    """
    no = len(component_pairs)
    logger.info(f"There are {no} component pairs to process")
    for i, component_pair in enumerate(component_pairs):
        insert_command = (
            insert(ComponentPair)
            .values(
                component_a_id=component_pair.component_a_id,
                component_b_id=component_pair.component_b_id,
                component_code_pair=component_pair.component_code_pair,
                autocorrelation=component_pair.autocorrelation,
                intracorrelation=component_pair.intracorrelation,
                azimuth=component_pair.azimuth,
                backazimuth=component_pair.backazimuth,
                distance=component_pair.distance,
                arcdistance=component_pair.arcdistance,
            )
            .on_conflict_do_update(
                constraint="single_component_pair",
                set_=dict(
                    component_code_pair=component_pair.component_code_pair,
                    autocorrelation=component_pair.autocorrelation,
                    intracorrelation=component_pair.intracorrelation,
                    azimuth=component_pair.azimuth,
                    backazimuth=component_pair.backazimuth,
                    distance=component_pair.distance,
                    arcdistance=component_pair.arcdistance,
                ),
            )
        )
        db.session.execute(insert_command)
        logger.info(f"Inserted {i}/{no - 1} component_pairs")
    logger.info("Commiting changes")
    db.session.commit()
    logger.info("Commit successfull")
    return


def create_all_componentpairs() -> None:
    """
    Fetches all components from the database, creates all component pairs possible and upserts them into db.

    Has to be run within app_context.

    :return: None
    :rtype: None
    """
    components: List[Component] = Component.query.all()

    component_pairs = prepare_componentpairs(components)

    upsert_componentpairs(component_pairs)
    return


def fetch_componentpairs(
        network_codes_a: Optional[Union[Collection[str], str]] = None,
        station_codes_a: Optional[Union[Collection[str], str]] = None,
        component_codes_a: Optional[Union[Collection[str], str]] = None,
        network_codes_b: Optional[Union[Collection[str], str]] = None,
        station_codes_b: Optional[Union[Collection[str], str]] = None,
        component_codes_b: Optional[Union[Collection[str], str]] = None,
        accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
        autocorrelation: Optional[bool] = False,
        intracorrelation: Optional[bool] = False,
) -> List[ComponentPair]:
    """
    Fetched requested component pairs.
    You can pass either selection for both station A and station B or just for A.
    By default, if none of selectors for station A will be provided, all ComponentPairs will be retrieved.
    If you won't pass any values for any of the station B selectors, selectors for A will be used.
    You can choose to fetch intracorrelation or autocorrelations.

    :param network_codes_a: Selector for network code of A station in the pair
    :type network_codes_a: Optional[Union[Collection[str], str]]
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]]
    :param component_codes_a: Selector for component code of A station in the pair
    :type component_codes_a: Optional[Union[Collection[str], str]]
    :param network_codes_b: Selector for network code of B station in the pair
    :type network_codes_b: Optional[Union[Collection[str], str]]
    :param station_codes_b: Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]]
    :param component_codes_b: Selector for component code of B station in the pair
    :type component_codes_b: Optional[Union[Collection[str], str]]
    :param autocorrelation: If autocorrelation pairs should be also included
    :type autocorrelation: Optional[bool]
    :param intracorrelation: If intracorrelation pairs should be also included
    :type intracorrelation: Optional[bool]
    :return: Selected ComponentPair objects
    :rtype: List[ComponentPair]
    """
    filters = []

    cmp_a = aliased(Component)
    cmp_b = aliased(Component)

    components_a = fetch_components(
        networks=network_codes_a,
        stations=station_codes_a,
        components=component_codes_a,
    )
    filters.append(cmp_a.id.in_(extract_object_ids(components_a)))

    if network_codes_b is None and station_codes_b is None and component_codes_b is None:
        filters.append(cmp_b.id.in_(extract_object_ids(components_a)))
    else:
        components_b = fetch_components(
            networks=network_codes_b,
            stations=station_codes_b,
            components=component_codes_b,
        )
        filters.append(cmp_b.id.in_(extract_object_ids(components_b)))

    if accepted_component_code_pairs is not None:
        accepted_component_code_pairs = validate_to_tuple(accepted_component_code_pairs, str)
        filters.append(ComponentPair.component_code_pair.in_(accepted_component_code_pairs))

    filters.append(ComponentPair.autocorrelation == autocorrelation)
    filters.append(ComponentPair.intracorrelation == intracorrelation)

    component_pairs = (
        db.session.query(ComponentPair)
        .join(cmp_a, ComponentPair.component_a)
        .join(cmp_b, ComponentPair.component_b)
        .options(
            subqueryload(ComponentPair.component_a),
            subqueryload(ComponentPair.component_b),
        )
        .filter(*filters)
        .all()
    )
    return component_pairs
