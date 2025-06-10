# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
from typing import Optional, List, Union, Collection
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, subqueryload

import datetime
import pandas as pd
import numpy as np
from obspy import UTCDateTime

from noiz.api.component import fetch_components
from noiz.api.helpers import extract_object_ids
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Component, ComponentPairCartesian, ComponentPairCylindrical
from noiz.processing.component_pair import prepare_componentpairs_cartesian, prepare_componentpairs_cylindrical
from noiz.validation_helpers import validate_to_tuple


def upsert_componentpairs_cartesian(component_pairs_cartesian: List[ComponentPairCartesian]) -> None:
    """
    Takes iterable of componentpairs_cartesian and inserts it into database.
    In case of conflict on `single_component_pair` constraint, updates the entry.

    Warning: Used UPSERT operation is PostgreSQL specific due to used SQLAlchemy command.
    Warning: Has to be run within application context.

    :param component_pairs_cartesian: List of componentpairs_cartesian to be upserted into db
    :type component_pairs_cartesian: List[ComponentPairCartesian]
    :return: None
    :rtype: None
    """
    no = len(component_pairs_cartesian)
    logger.info(f"There are {no} component pairs to process")
    for i, component_pair_cartesian in enumerate(component_pairs_cartesian):
        insert_command = (
            insert(ComponentPairCartesian)
            .values(
                component_a_id=component_pair_cartesian.component_a_id,
                component_b_id=component_pair_cartesian.component_b_id,
                component_code_pair=component_pair_cartesian.component_code_pair,
                autocorrelation=component_pair_cartesian.autocorrelation,
                intracorrelation=component_pair_cartesian.intracorrelation,
                azimuth=component_pair_cartesian.azimuth,
                backazimuth=component_pair_cartesian.backazimuth,
                distance=component_pair_cartesian.distance,
                arcdistance=component_pair_cartesian.arcdistance,
            )
            .on_conflict_do_update(
                constraint="single_component_pair",
                set_={
                    "component_code_pair": component_pair_cartesian.component_code_pair,
                    "autocorrelation": component_pair_cartesian.autocorrelation,
                    "intracorrelation": component_pair_cartesian.intracorrelation,
                    "azimuth": component_pair_cartesian.azimuth,
                    "backazimuth": component_pair_cartesian.backazimuth,
                    "distance": component_pair_cartesian.distance,
                    "arcdistance": component_pair_cartesian.arcdistance,
                },
            )
        )
        db.session.execute(insert_command)
        if i % int(no / 10) == 0:
            logger.info(f"Inserted {i}/{no - 1} component_pairs_cartesian")

    logger.info("Commiting changes")
    db.session.commit()
    logger.info("Commit successfull")
    return


def create_all_componentpairs_cartesian(cp_optimization) -> None:
    """
    Fetches all components from the database, creates all component pairs possible and upserts them into db.

    Has to be run within app_context.

    :return: None
    :rtype: None
    """
    components = fetch_components()

    component_pairs_cartesian = prepare_componentpairs_cartesian(components, cp_optimization)

    upsert_componentpairs_cartesian(component_pairs_cartesian)
    return


def fetch_componentpairs_cartesian_by_id(
    component_pair_cartesian_id: Union[Collection[int], int],
) -> List[ComponentPairCartesian]:
    """
    Fetches :py:class:`~noiz.models.component_pair_cartesian.ComponentPairCartesian` from the database by id.
    By default it also loads both Components that belong to the pair.

    :param component_pair_cartesian_id: Accepts either single id or multiple ids
    :type component_pair_cartesian_id: Union[Collection[int], int]
    :return: Fetched Pairs
    :rtype: List[ComponentPairCartesian]
    """
    pair_ids = validate_to_tuple(component_pair_cartesian_id, int)

    component_pairs_cartesian = (
        db.session.query(ComponentPairCartesian)
        .filter(ComponentPairCartesian.id.in_(pair_ids))
        .options(
            subqueryload(ComponentPairCartesian.component_a),
            subqueryload(ComponentPairCartesian.component_b),
        )
        .all()
    )

    if len(component_pairs_cartesian) == 0:
        EmptyResultException("There were no pairs in database with IDs you asked for.")

    return component_pairs_cartesian


def fetch_componentpairs_cartesian(
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    component_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    component_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
    include_autocorrelation: Optional[bool] = False,
    include_intracorrelation: Optional[bool] = False,
    only_autocorrelation: Optional[bool] = False,
    only_intracorrelation: Optional[bool] = False,
) -> List[ComponentPairCartesian]:
    """
    Fetched requested component pairs.
    You can pass either selection for both station A and station B or just for A.
    By default, if none of selectors for station A will be provided, all componentpairs_cartesian will be retrieved.
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
    :param include_autocorrelation: If autocorrelation pairs should be also included
    :type include_autocorrelation: Optional[bool]
    :param include_intracorrelation: If intracorrelation pairs should be also included
    :type include_intracorrelation: Optional[bool]
    :param only_autocorrelation: If only autocorrelation pairs should be selected
    :type only_autocorrelation: Optional[bool]
    :param only_intracorrelation: If only intracorrelation pairs should be selected
    :type only_intracorrelation: Optional[bool]
    :return: Selected ComponentPairCartesian objects
    :rtype: List[ComponentPairCartesian]
    """
    if only_autocorrelation and only_intracorrelation:
        raise ValueError("You cannot use only_autocorrelation and only_intracorrelation at the same time")

    if any((only_autocorrelation, only_intracorrelation)) and any((include_autocorrelation, include_intracorrelation)):
        raise ValueError("You cannot use only_* and include_* arguments at the same time.")

    filters = []

    cmp_a = aliased(Component)
    cmp_b = aliased(Component)

    components_a = fetch_components(
        networks=network_codes_a,
        stations=station_codes_a,
        components=component_codes_a,
    )

    components_b = fetch_components(
        networks=network_codes_b,
        stations=station_codes_b,
        components=component_codes_b,
    )
    filters.append(cmp_a.id.in_(extract_object_ids(components_a)))
    filters.append(cmp_b.id.in_(extract_object_ids(components_b)))

    if accepted_component_code_pairs is not None:
        accepted_component_code_pairs = validate_to_tuple(accepted_component_code_pairs, str)
        filters.append(ComponentPairCartesian.component_code_pair.in_(accepted_component_code_pairs))

    if include_autocorrelation:
        autocorr_filter = ComponentPairCartesian.autocorrelation.in_((True, False))
    elif only_autocorrelation:
        autocorr_filter = ComponentPairCartesian.autocorrelation.in_((True,))
    else:
        autocorr_filter = ComponentPairCartesian.autocorrelation.in_((False,))
    filters.append(autocorr_filter)

    if include_intracorrelation:
        intracorr_filter = ComponentPairCartesian.intracorrelation.in_((True, False))
    elif only_intracorrelation:
        intracorr_filter = ComponentPairCartesian.intracorrelation.in_((True,))
    else:
        intracorr_filter = ComponentPairCartesian.intracorrelation.in_((False,))
    filters.append(intracorr_filter)

    component_pairs_cartesian = (
        db.session.query(ComponentPairCartesian)
        .join(cmp_a, ComponentPairCartesian.component_a)
        .join(cmp_b, ComponentPairCartesian.component_b)
        .options(
            subqueryload(ComponentPairCartesian.component_a),
            subqueryload(ComponentPairCartesian.component_b),
        )
        .filter(*filters)
        .all()
    )

    if len(component_pairs_cartesian) == 0:
        EmptyResultException("There were no pairs in database that fit this query.")

    return component_pairs_cartesian


def fetch_componentpairs_cylindrical_by_id(
    component_pair_cylindrical_id: Union[Collection[int], int],
) -> List[ComponentPairCylindrical]:
    """
    Fetches :py:class:`~noiz.models.component_pair_cylindrical.ComponentPairCylindrical` from the database by id.
    By default it also loads both Components that belong to the pair.

    :param component_pair_cylindrical_id:Accepts either single id or multiple ids
    :type component_pair_cylindrical_id: Union[Collection[int], int]
    :return: Fetched Pairs
    :rtype: List[ComponentPairCylindrical]
    """

    pair_ids = validate_to_tuple(component_pair_cylindrical_id, int)

    component_pairs_cylindrical = (
        db.session.query(ComponentPairCylindrical).filter(ComponentPairCylindrical.id.in_(pair_ids)).all()
    )

    if len(component_pairs_cylindrical) == 0:
        EmptyResultException("There were no pairs in database with IDs you asked for.")

    return component_pairs_cylindrical


def fetch_componentpairs_cylindrical(
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    component_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    component_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
) -> List[ComponentPairCylindrical]:
    """
    Fetched requested cylindrical component pairs.
    You can pass either selection for both station A and station B or just for A.
    By default, if none of selectors for station A will be provided, all componentpairs_cylindrical will be retrieved.
    If you won't pass any values for any of the station B selectors, selectors for A will be used.

    :param network_codes_a: Selector for network code of A station in the pair, defaults to None
    :type network_codes_a: Optional[Union[Collection[str], str]], optional
    :param station_codes_a: Selector for station code of A station in the pair, defaults to None
    :type station_codes_a: Optional[Union[Collection[str], str]], optional
    :param component_codes_a: Selector for component code of A station in the pair, defaults to None
    :type component_codes_a: Optional[Union[Collection[str], str]], optional
    :param network_codes_b: Selector for network code of B station in the pair, defaults to None
    :type network_codes_b: Optional[Union[Collection[str], str]], optional
    :param station_codes_b: Selector for station code of B station in the pair, defaults to None
    :type station_codes_b: Optional[Union[Collection[str], str]], optional
    :param component_codes_b: Selector for component code of B station in the pair, defaults to None
    :type component_codes_b: Optional[Union[Collection[str], str]], optional
    :param accepted_component_code_pairs_cylindrical: Selector for componentpair code, defaults to None
    :type accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]], optional
    :return: : Selected ComponentPairCylindrical objects
    :rtype: List[ComponentPairCylindrical]
    """

    filters = []

    if network_codes_a is not None or station_codes_a is not None:
        components_a = fetch_components(
            networks=network_codes_a,
            stations=station_codes_a,
            starttime=starttime,
            endtime=endtime,
        )

        if network_codes_b is None and station_codes_b is None:
            components_b = components_a.copy()
        else:
            components_b = fetch_components(
                networks=network_codes_b,
                stations=station_codes_b,
                starttime=starttime,
                endtime=endtime,
            )

    else:
        components_a = fetch_components(
            starttime=starttime,
            endtime=endtime,
        )
        components_b = fetch_components(
            starttime=starttime,
            endtime=endtime,
        )

    if (accepted_component_code_pairs_cylindrical is None) and (
        component_codes_a is not None and component_codes_b is not None
    ):
        accepted_component_code_pairs_cylindrical = tuple(
            [ca + cb for (ca, cb) in zip(component_codes_a, component_codes_b)]
        )

    if accepted_component_code_pairs_cylindrical is not None:
        accepted_component_code_pairs_cylindrical = validate_to_tuple(accepted_component_code_pairs_cylindrical, str)
        filters.append(
            ComponentPairCylindrical.component_cylindrical_code_pair.in_(accepted_component_code_pairs_cylindrical)
        )

    cmp_a_ids = extract_object_ids(components_a)
    cmp_b_ids = extract_object_ids(components_b)

    filters.append(
        ComponentPairCylindrical.component_aE_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_aE_id.in_(cmp_b_ids)
        | ComponentPairCylindrical.component_aN_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_aN_id.in_(cmp_b_ids)
        | ComponentPairCylindrical.component_aZ_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_aZ_id.in_(cmp_b_ids)
    )
    filters.append(
        ComponentPairCylindrical.component_bE_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_bE_id.in_(cmp_b_ids)
        | ComponentPairCylindrical.component_bN_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_bN_id.in_(cmp_b_ids)
        | ComponentPairCylindrical.component_bZ_id.in_(cmp_a_ids)
        | ComponentPairCylindrical.component_bZ_id.in_(cmp_b_ids)
    )

    component_pairs_cylindrical = db.session.query(ComponentPairCylindrical).filter(*filters).all()

    if len(component_pairs_cylindrical) == 0:
        raise EmptyResultException("There were no pairs in database that fit this query.")

    return component_pairs_cylindrical


def create_all_componentpairs_cylindrical() -> None:
    """
    Fetches all components from the database, creates all component pairs possible and upserts them into db.

    Has to be run within app_context.

    :return: None
    :rtype: None
    """
    components = fetch_components()
    station_unique = list({cp.station for cp in components})
    station_unique.sort()
    logger.info("Station fetch done")
    check_sta = []  # type: list
    for sta in station_unique:
        componentpair = fetch_componentpairs_cartesian(station_codes_a=sta, station_codes_b=tuple(station_unique))
        componentpair += fetch_componentpairs_cartesian(station_codes_a=tuple(station_unique), station_codes_b=sta)
        if sta == station_unique[0]:
            component_pairs_cylindrical = prepare_componentpairs_cylindrical(
                componentpair, station_unique, sta, check_sta
            )
        else:
            component_pairs_cylindrical += prepare_componentpairs_cylindrical(
                componentpair, station_unique, sta, check_sta
            )
        check_sta.append(sta)
    upsert_componentpairs_cylindrical(component_pairs_cylindrical)
    return


def upsert_componentpairs_cylindrical(component_pairs_cylindrical: List[ComponentPairCylindrical]) -> None:
    """
    Takes iterable of ComponentPairCylindrical and inserts it into database.

    Warning: Used UPSERT operation is PostgreSQL specific due to used SQLAlchemy command.
    Warning: Has to be run within application context.

    :param component_pairs_cylindrical: List of component_pairs_cylindrical to be upserted into db
    :type ComponentPairCylindrical: List[ComponentPairCylindrical]
    :return: None
    :rtype: None
    """
    no = len(component_pairs_cylindrical)
    logger.info(f"There are {no} component pairs to process")

    for i, component_pair_cylindrical in enumerate(component_pairs_cylindrical):
        insert_command = insert(ComponentPairCylindrical).values(
            component_aE_id=component_pair_cylindrical.component_aE_id,
            component_bE_id=component_pair_cylindrical.component_bE_id,
            component_aN_id=component_pair_cylindrical.component_aN_id,
            component_bN_id=component_pair_cylindrical.component_bN_id,
            component_aZ_id=component_pair_cylindrical.component_aZ_id,
            component_bZ_id=component_pair_cylindrical.component_bZ_id,
            component_cylindrical_code_pair=component_pair_cylindrical.component_cylindrical_code_pair,
            autocorrelation=component_pair_cylindrical.autocorrelation,
            intracorrelation=component_pair_cylindrical.intracorrelation,
            azimuth=component_pair_cylindrical.azimuth,
            backazimuth=component_pair_cylindrical.backazimuth,
            distance=component_pair_cylindrical.distance,
            arcdistance=component_pair_cylindrical.arcdistance,
        )
        db.session.execute(insert_command)
        logger.info(f"Inserted {i}/{no - 1} component_pairs_cylindrical")
    logger.info("Commiting changes")
    db.session.commit()
    logger.info("Commit successfull")
    return
