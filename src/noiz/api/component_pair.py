from loguru import logger
from typing import Iterable, Optional, List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, subqueryload

from noiz.database import db
from noiz.models.component import Component
from noiz.models.component_pair import ComponentPair
from noiz.processing.component_pair import (
    prepare_componentpairs,
)


def upsert_component_pairs(component_pairs: List[ComponentPair]) -> None:
    """
    Takes iterable of ComponentPairs and inserts it into database.
    In case of conflict on `single_component_pair` constraint, updates the entry.

    Warning: Used UPSERT operation is PostgreSQL specific due to used SQLAlchemy command.
    Warning: Has to be run within application context.

    :param component_pairs: Iterable with ComponentPairs to be upserted into db
    :type component_pairs:
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
    logger.info("Commit successfull. Returning")
    return


def create_all_channelpairs() -> None:
    """
    Fetches all components from the database,
    creates all component pairs possible
    and upserts them into db.

    Warning: Has to be run within application context.

    :return: None
    :rtype: None
    """
    components: List[Component] = Component.query.all()

    component_pairs = prepare_componentpairs(components)

    upsert_component_pairs(component_pairs)
    return


def fetch_component_pairs(
    station_a: Iterable[str],
    component_a: Iterable[str],
    station_b: Optional[Iterable[str]],
    component_b: Optional[Iterable[str]],
) -> Iterable[ComponentPair]:
    """
    Fetches from db requested channelpairs
    :param station_a:
    :type station_a:
    :param component_a:
    :type component_a:
    :param station_b:
    :type station_b:
    :param component_b:
    :type component_b:
    :return:
    :rtype:
    """

    if station_b is None:
        station_b = station_a
    if component_b is None:
        component_b = component_a

    cmp_a = aliased(Component)
    cmp_b = aliased(Component)
    component_pairs = (
        db.session.query(ComponentPair)
        .join(cmp_a, ComponentPair.component_a)
        .join(cmp_b, ComponentPair.component_b)
        .options(
            subqueryload(ComponentPair.component_a),
            subqueryload(ComponentPair.component_b),
        )
        .filter(
            cmp_a.station.in_(station_a),
            cmp_b.station.in_(station_b),
            cmp_a.component.in_(component_a),
            cmp_b.component.in_(component_b),
        )
        .all()
    )
    return component_pairs
