import itertools
import logging
import numpy as np

from sqlalchemy.dialects.postgresql import insert
from typing import Iterable

from noiz.database import db
from noiz.models import Component, ComponentPair


def is_autocorrelation(cmp_a: Component, cmp_b: Component) -> bool:
    """
    Checks if two Component objects belong to the same station and have the same component direction.

    :param cmp_a: First Component object
    :type cmp_a: Component
    :param cmp_b: Second Component object
    :type cmp_b: Component
    :return: If Component belong to the same station and component
    :rtype: bool
    """
    if (
        cmp_a.station == cmp_b.station
        and cmp_a.network == cmp_b.network
        and cmp_a.component == cmp_b.component
    ):
        return True
    else:
        return False


def is_intrastation_correlation(cmp_a: Component, cmp_b: Component) -> bool:
    """
    Checks if two Components objects belong to the same station but have different component direction.

    :param cmp_a: First Component object
    :type cmp_a: Component
    :param cmp_b: Second Component object
    :type cmp_b: Component
    :return: If Components belong to the same station ut have different component letter
    :rtype: bool
    """
    if (
        cmp_a.station == cmp_b.station
        and cmp_a.network == cmp_b.network
        and cmp_a.component != cmp_b.component
    ):
        return True
    else:
        return False


def calculate_distance_backazimuth(lat_a, lon_a, lat_b, lon_b):
    """
    Offline calculator of backazimuth, distance, azimuth along great circle.
    Consistent with IRIS
    see obspy.clients.iris.Client.distaz

    :param lon_a: longitude of the station, in decimal degrees
    :type lon_a: float
    :param lat_a: latitude of the station, in decimal degrees
    :type lat_a: float
    :param lon_b: longitude of the event in decimal degrees
    :type lon_b: float
    :param lat_b: latitude of the event in decimal degrees
    :type lat_b: float
    :return Returns a dict with params: distancemeters, distancekilometers, distance (arcdistance), backazimuth, azimuth
    :rtype Dict[float]
    """

    if lon_a == lon_b and lat_a == lat_b:
        return 0.0, 0.0, np.nan, np.nan

    lon_a = np.deg2rad(lon_a)
    lat_a = np.deg2rad(lat_a)
    lon_b = np.deg2rad(lon_b)
    lat_b = np.deg2rad(lat_b)

    arc_distance = np.arccos(
        np.sin(lat_b) * np.sin(lat_a)
        + np.cos(lat_b) * np.cos(lat_a) * np.cos(lon_b - lon_a)
    )
    distance_km = arc_distance * 6371.0

    if np.isnan(distance_km):
        results = {
            "distancemeters": 0,
            "distancekilometers": 0,
            "distance": 0,
            "backazimuth": np.nan,
            "azimuth": np.nan,
        }
        return results

    cosa = (np.sin(lat_b) - np.sin(lat_a) * np.cos(arc_distance)) / (
        np.cos(lat_a) * np.sin(arc_distance)
    )
    sina = np.cos(lat_b) * np.sin(lon_b - lon_a) / np.sin(arc_distance)
    sina = np.clip(sina, -1.0, 1.0)

    backazimuth = np.arcsin(sina)
    if cosa < 0.0:
        backazimuth = np.pi - backazimuth

    cosb = (np.sin(lat_a) - np.sin(lat_b) * np.cos(arc_distance)) / (
        np.cos(lat_b) * np.sin(arc_distance)
    )
    sinb = -np.cos(lat_a) * sina / np.cos(lat_b)
    sinb = np.clip(sinb, -1.0, 1.0)

    azimuth = np.arcsin(sinb)
    if cosb < 0.0:
        azimuth = np.pi - azimuth

    azimuth = azimuth % (2.0 * np.pi)
    backazimuth = backazimuth % (2.0 * np.pi)

    # back to degrees
    arc_distance = np.rad2deg(arc_distance)
    azimuth = np.rad2deg(azimuth)
    backazimuth = np.rad2deg(backazimuth)
    distance_m = distance_km * 1000

    results = {
        "distancemeters": distance_m,
        "distancekilometers": distance_km,
        "distance": arc_distance,
        "backazimuth": backazimuth,
        "azimuth": azimuth,
    }

    return results


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
        distaz = calculate_distance_backazimuth(
            cmp_a.lat, cmp_a.lon, cmp_b.lat, cmp_b.lon
        )
        logging.info("Calculation successful!")
    return distaz


def is_east_to_west(cmp_a: Component, cmp_b: Component) -> bool:
    """
    Checks if orientation of two componenents is east to west.
    If longitude of both is the same then checks if first component is to the north of the second.

    :param cmp_a: First Component object
    :type cmp_a: Component
    :param cmp_b: Second Component object
    :type cmp_b: Component
    :return: If cmp_a is to the east of cmp_b
    :rtype: bool
    """
    east_west = cmp_a.lon > cmp_b.lon
    if cmp_a.lon == cmp_b.lon:
        east_west = cmp_a.lat > cmp_b.lat
    return east_west


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
