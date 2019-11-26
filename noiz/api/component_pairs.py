from typing import Iterable, Optional

from sqlalchemy.orm import aliased, subqueryload

from noiz.database import db
from noiz.models import ComponentPair, Component


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
