import itertools
import logging
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert

from models import Component
from noiz.database import db
from noiz.models import Component, ComponentPair
from noiz.processing.component_pair import (
    is_autocorrelation,
    is_intrastation_correlation,
    is_east_to_west,
)
from processing.component_pair import _calculate_distance_backazimuth


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


def calculate_distance_azimuths(
    cmp_a: Component, cmp_b: Component, iris: bool = False
) -> dict:
    """
    Calculates a distance (arc), distancemeters, backazimuth, azimuth either with use of inhouse method or iris.
    Method developed my Max, refactored by Damian.

    :param cmp_a: First Component object
    :type cmp_a: Component
    :param cmp_b: Second Component object
    :type cmp_b: Component
    :param iris: Should it use iris client or not? Warning! Client uses online resolver!
    It does not catch potential http errors!
    :type iris: bool
    :return: Dict with params.
    :rtype: dict
    """

    if iris:
        logging.info("Calculating distance and azimuths with iris")
        from obspy.clients.iris import Client

        distaz = Client().distaz(cmp_a.lat, cmp_a.lon, cmp_b.lat, cmp_b.lon)
        logging.info("Calculation successful!")
    else:
        logging.info("Calculating distance and azimuths with local method")
        distaz = _calculate_distance_backazimuth(
            cmp_a.lat, cmp_a.lon, cmp_b.lat, cmp_b.lon
        )
        logging.info("Calculation successful!")
    return distaz
