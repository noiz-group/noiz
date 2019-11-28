import itertools
import logging
from typing import Iterable, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, subqueryload

from noiz.database import db
from noiz.models import Component, ComponentPair
from noiz.processing.component_pair import (
    is_autocorrelation,
    is_intrastation_correlation,
    is_east_to_west,
)
from processing.component_pair import calculate_distance_azimuths


def prepare_componentpairs(components: Iterable[Component]) -> Iterable[ComponentPair]:
    """
    Takes iterable of Components and creates all possible ComponentPairs including autocorrelations
    and intrastation correlations.

    :param components: Iterable of Component objects
    :type components: Iterable[Component]
    :return: Iterable with ComponentPairs
    :rtype: Iterable[ComponentPair]
    """
    component_pairs = []
    potential_pairs = list(itertools.product(components, repeat=2))
    no = len(potential_pairs)
    logging.info(f"There are {no} potential pairs to be checked.")
    for i, (cmp_a, cmp_b) in enumerate(potential_pairs):
        logging.info(f"Starting with potential pair {i}/{no - 1}")

        component_pair = ComponentPair(
            component_a_id=cmp_a.id,
            component_b_id=cmp_b.id,
            component_names="".join([cmp_a.component, cmp_b.component]),
        )

        if is_autocorrelation(cmp_a, cmp_b):
            logging.info(f"Pair {component_pair} is autocorrelation")
            component_pair.set_autocorrelation()
            component_pairs.append(component_pair)
            continue

        if is_intrastation_correlation(cmp_a, cmp_b):
            logging.info(f"Pair {component_pair} is intracorrelation")
            component_pair.set_intracorrelation()
            component_pairs.append(component_pair)
            continue

        if not is_east_to_west(cmp_a, cmp_b):
            logging.info(f"Pair {component_pair} is not east to west, skipping")
            continue

        logging.info(
            f"Pair {component_pair} is east to west, calculating distance and backazimuths"
        )
        distaz = calculate_distance_azimuths(cmp_a, cmp_b)
        component_pair.set_params_from_distaz(distaz)
        component_pairs.append(component_pair)

    return component_pairs


def upsert_component_pairs(component_pairs: Iterable[ComponentPair]) -> None:
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
    logging.info(f"There are {no} component pairs to process")
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
        logging.info(f"Inserted {i}/{no - 1} component_pairs")
    logging.info(f"Commiting changes")
    db.session.commit()
    logging.info("Commit successfull. Returning")
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
    components = Component.query.all()

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
