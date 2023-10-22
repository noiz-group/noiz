# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import itertools
from loguru import logger
import numpy as np
import pandas as pd
from typing import List, Tuple, Optional
from datetime import datetime, timedelta

from noiz.models.component import Component
from noiz.models.component_pair import ComponentPairCartesian, ComponentPairCylindrical


def prepare_componentpairs_cartesian(components: List[Component]) -> List[ComponentPairCartesian]:
    """
    Takes iterable of Components and creates all possible componentpairs_cartesian including autocorrelations
    and intrastation correlations.

    :param components: Iterable of Component objects
    :type components: Iterable[Component]
    :return: Iterable with componentpairs_cartesian
    :rtype: Iterable[ComponentPairCartesian]
    """
    component_pairs_cartesian: List[ComponentPairCartesian] = []
    potential_pairs = list(itertools.product(components, repeat=2))
    no = len(potential_pairs)
    logger.info(f"There are {no} potential pairs to be checked.")
    for i, (cmp_a, cmp_b) in enumerate(potential_pairs):
        if i % int(no/10) == 0:
            logger.info(f"Processed already {i}/{no} pairs")

        cmpa_start_date = datetime(cmp_a.start_date.year, cmp_a.start_date.month, cmp_a.start_date.day, cmp_a.start_date.hour)
        cmpb_start_date = datetime(cmp_b.start_date.year, cmp_b.start_date.month, cmp_b.start_date.day, cmp_b.start_date.hour)
        cmpa_end_date = datetime(cmp_a.end_date.year, cmp_a.end_date.month, cmp_a.end_date.day, cmp_a.end_date.hour)
        cmpb_end_date = datetime(cmp_b.end_date.year, cmp_b.end_date.month, cmp_b.end_date.day, cmp_b.end_date.hour)

        working_time_a = np.arange(cmpa_start_date, cmpa_end_date, timedelta(minutes=1)).astype(datetime)
        working_time_b = np.arange(cmpb_start_date, cmpb_end_date, timedelta(minutes=1)).astype(datetime)
        a_set = set(working_time_a)
        b_set = set(working_time_b)
        if (a_set & b_set):
            logger.debug(f"Starting with potential pair {i}/{no - 1}")

            component_pair_cartesian = ComponentPairCartesian(
                component_a_id=cmp_a.id,
                component_b_id=cmp_b.id,
                component_code_pair=f"{cmp_a.component}{cmp_b.component}"
            )

            if is_autocorrelation(cmp_a, cmp_b):
                logger.debug(f"Pair {component_pair_cartesian} is autocorrelation")
                component_pair_cartesian.set_autocorrelation()
                component_pairs_cartesian.append(component_pair_cartesian)
                continue

            if is_intrastation_correlation(cmp_a, cmp_b):
                logger.debug(f"Pair {component_pair_cartesian} is intracorrelation")
                component_pair_cartesian.set_intracorrelation()
                component_pairs_cartesian.append(component_pair_cartesian)
                continue

            if not is_east_to_west(cmp_a, cmp_b):
                logger.debug(f"Pair {component_pair_cartesian} is not east to west, skipping")
                continue

            logger.debug(
                f"Pair {component_pair_cartesian} is east to west, calculating distance and backazimuths"
            )
            distaz = calculate_distance_azimuths(cmp_a, cmp_b)
            component_pair_cartesian.set_params_from_distaz(distaz)
            component_pairs_cartesian.append(component_pair_cartesian)
    logger.info(f"There were {len(component_pairs_cartesian)} component pairs created.")
    return component_pairs_cartesian


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


def _calculate_distance_backazimuth(lat_a, lon_a, lat_b, lon_b):
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
        logger.debug("Calculating distance and azimuths with iris client from Obspy")
        from obspy.clients.iris import Client

        distaz = Client().distaz(cmp_a.lat, cmp_a.lon, cmp_b.lat, cmp_b.lon)
        logger.debug("Calculation successful!")
    else:
        logger.debug("Calculating distance and azimuths with local method")
        distaz = _calculate_distance_backazimuth(
            cmp_a.lat, cmp_a.lon, cmp_b.lat, cmp_b.lon
        )
        logger.debug("Calculation successful!")
    return distaz


def prepare_componentpairs_cylindrical(componentpair_cartesian, station_unique, sta, check_sta) -> List[ComponentPairCylindrical]:
    """
    Takes cartesian component pairs and creates all possible cylindrical component pairs excluding autocorrelations
    and intrastation correlations.

    :param componentpair_cartesian: cartesian component pairs created in prepare_componentpairs_cartesian
    :type componentpair_cartesian: List[ComponentPairCartesian]
    :param station_unique: list of working stations
    :type station_unique: list of string
    :param sta: station for which one the cylindrical component pairs have been computed
    :type sta: string
    :param check_sta: vector containing stations whose all possible cylindrical pair combinaisons have been deternined
    :type check_sta: vector of string - Iterable.
    :return: Iterable with cylindrical pari
    :rtype: Iterable[ComponentPairCylindrical]
    """

    component_pairs_cylindrical: List[ComponentPairCylindrical] = []
    comp_cart_list = ["E", "N", "Z"]
    component_cyndrical_pair = ["RR", "TT", "RT", "TR", "ZR", "RZ", "ZT", "TZ"]

    cpair_cartesienne = [
        [cpair.component_a,
         cpair.component_a_id,
         cpair.component_b,
         cpair.component_b_id,
         cpair.distance,
         cpair.azimuth,
         cpair.backazimuth,
         cpair.arcdistance,
         cpair.autocorrelation,
         cpair.intracorrelation,
         ]
        for cpair in componentpair_cartesian
    ]
    df_cpair = pd.DataFrame(cpair_cartesienne, columns=['comp_a', 'comp_a_id', 'comp_b', 'comp_b_id', 'distance', 'azimuth', 'backazimuth', 'arcdistance', 'autocorrelation', 'intracorrelation'])
    cpair_cartesienne_station_info = [
        [compa_sta.station,
         compa_comp.component,
         compb_sta.station,
         compb_comp.component
         ]
        for compa_sta, compa_comp, compb_sta, compb_comp in zip(df_cpair["comp_a"], df_cpair["comp_a"], df_cpair["comp_b"], df_cpair["comp_b"])
    ]
    df_cpair = df_cpair.merge(pd.DataFrame(cpair_cartesienne_station_info, columns=['stationa', 'comp_a_code', 'stationb', 'comp_b_code']), left_index=True, right_index=True)

    for stab in station_unique:
        if stab in check_sta:  # if stab not in check_sta:
            continue
        pair_station_info = df_cpair[((df_cpair["stationa"] == sta) & (df_cpair["stationb"] == stab)) | ((df_cpair["stationa"] == stab) & (df_cpair["stationb"] == sta))]
        if len(pair_station_info) != 0:
            comp_a_to_ckeck = np.unique(pair_station_info["comp_a_code"])
            comp_b_to_ckeck = np.unique(pair_station_info["comp_b_code"])
            if not ((comp_cart_list == comp_a_to_ckeck).all()) and not ((comp_cart_list == comp_b_to_ckeck).all()):
                logger.info(f"not all components were load {sta}-{stab}")
                continue
            azimuth, backazimuth, distance, arcdistance, autocorrelation, intracorrelation = _read_station_pair_information(pair_station_info)
            for cp_cylindrical in component_cyndrical_pair:
                component_aE, component_bE, component_aN, component_bN, component_aZ, component_bZ = _component_cylindrical(pair_station_info, cp_cylindrical)
                component_pair_cylindrical = ComponentPairCylindrical(
                                            component_cylindrical_code_pair=cp_cylindrical,
                                            component_aE_id=component_aE,
                                            component_bE_id=component_bE,
                                            component_aN_id=component_aN,
                                            component_bN_id=component_bN,
                                            component_aZ_id=component_aZ,
                                            component_bZ_id=component_bZ,
                                            distance=distance,
                                            azimuth=azimuth,
                                            backazimuth=backazimuth,
                                            arcdistance=arcdistance,
                                            autocorrelation=autocorrelation,
                                            intracorrelation=intracorrelation,
                                            )
                component_pairs_cylindrical.append(component_pair_cylindrical)
            logger.info(f"Cylindrical component pair found for {sta}-{stab}")

    return component_pairs_cylindrical


def _read_station_pair_information(pair_station_info):
    """
    Read the station pair information (azimuth,backazimuth, distance, arcdistance, autocorrelation, intracorrelation) for a station pair.

    :param pair_station_info: dataframe containing all the station pair information
    :type pair_station_info: Dataframe
    :return: vector containing azimuth, backazimuth, distance, arcdistance, autocorrelation, intracorrelation for a station couple
    :rtype: list of float
    """
    azimuth = pair_station_info["azimuth"].values[0]
    backazimuth = pair_station_info["backazimuth"].values[0]
    distance = pair_station_info["distance"].values[0]
    arcdistance = pair_station_info["arcdistance"].values[0]
    autocorrelation = pair_station_info["autocorrelation"].values[0]
    intracorrelation = pair_station_info["intracorrelation"].values[0]

    return azimuth, backazimuth, distance, arcdistance, autocorrelation, intracorrelation


def _component_cylindrical(pair_station_info, comp):
    """
    Definition of the cartesian component pairs for a cylindrical component pair.

    :param pair_station_info: Dataframe containing the pair station and component information
    :type pair_station_info: Dataframe
    :param comp: possible cylindrical component pairs (RR, TT, ...)
    :type comp: string
    :return: cartesian component pair relating to the considered cylindrical component pair.
    :rtype: vector if integer
    """
    if comp in ["RR", "TT", "RT", "TR"]:
        component_aE = int(pair_station_info["comp_a_id"][pair_station_info["comp_a_code"] == "E"].unique()[0])
        component_bE = int(pair_station_info["comp_b_id"][pair_station_info["comp_b_code"] == "E"].unique()[0])
        component_aN = int(pair_station_info["comp_a_id"][pair_station_info["comp_a_code"] == "N"].unique()[0])
        component_bN = int(pair_station_info["comp_b_id"][pair_station_info["comp_b_code"] == "N"].unique()[0])
        component_aZ = None  # type: ignore
        component_bZ = None  # type: ignore
    elif (comp in ["ZR", "ZT"]):
        component_aE = None  # type: ignore
        component_bE = int(pair_station_info["comp_b_id"][pair_station_info["comp_b_code"] == "E"].unique()[0])
        component_aN = None  # type: ignore
        component_bN = int(pair_station_info["comp_b_id"][pair_station_info["comp_b_code"] == "N"].unique()[0])
        component_aZ = int(pair_station_info["comp_a_id"][pair_station_info["comp_a_code"] == "Z"].unique()[0])
        component_bZ = None  # type: ignore
    elif (comp in ["RZ", "TZ"]):
        component_aE = int(pair_station_info["comp_a_id"][pair_station_info["comp_a_code"] == "E"].unique()[0])
        component_bE = None  # type: ignore
        component_aN = int(pair_station_info["comp_a_id"][pair_station_info["comp_a_code"] == "N"].unique()[0])
        component_bN = None  # type: ignore
        component_aZ = None  # type: ignore
        component_bZ = int(pair_station_info["comp_b_id"][pair_station_info["comp_b_code"] == "Z"].unique()[0])

    return component_aE, component_bE, component_aN, component_bN, component_aZ, component_bZ
