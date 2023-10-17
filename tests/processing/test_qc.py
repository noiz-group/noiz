import pytest
from datetime import datetime
from noiz.models import Datachunk, QCOneConfig, Timespan, QCOneRejectedTime

from noiz.processing.qc import _determine_if_datachunk_is_in_qcone_accepted_time


def test__determine_if_datachunk_is_in_qcone_accepted_time_raise_on_different_timespan_ids():
    datachunk = Datachunk(component_id=1, timespan_id=10)
    timespan = Timespan(
        id=42,
        starttime=datetime(2023, 2, 1),
        midtime=datetime(2023, 2, 14, 12),
        endtime=datetime(2023, 2, 28)
    )
    config = QCOneConfig()

    with pytest.raises(ValueError):
        _determine_if_datachunk_is_in_qcone_accepted_time(
            datachunk=datachunk,
            timespan=timespan,
            config=config,
        )


def test__determine_if_datachunk_is_in_qcone_accepted_time_no_rejected_periods():
    datachunk = Datachunk(component_id=1)
    timespan = Timespan(
        starttime=datetime(2023, 2, 1),
        midtime=datetime(2023, 2, 14, 12),
        endtime=datetime(2023, 2, 28)
    )
    config = QCOneConfig(time_periods_rejected=[])

    accepted_time = _determine_if_datachunk_is_in_qcone_accepted_time(
        datachunk=datachunk,
        timespan=timespan,
        config=config,
    )
    assert datachunk.component_id == 1
    assert len(config.time_periods_rejected) == 0
    assert config.component_ids_rejected_times == tuple()

    assert isinstance(accepted_time, bool)
    assert accepted_time is True


@pytest.mark.parametrize(
    "name, rejected_starttime, rejected_endtime, is_passing",
    (
            (
                "no_overlap",
                datetime(2023, 1, 1),
                datetime(2023, 1, 10),
                True
            ),
            (
                "partial_overlap",
                datetime(2023, 1, 1),
                datetime(2023, 2, 10),
                False
            ),
            (
                "startday_overlap",
                datetime(2023, 1, 1),
                datetime(2023, 2, 12),
                False
            ),
            (
                "endday_overlap",
                datetime(2023, 2, 1),
                datetime(2023, 2, 12),
                False
            ),
            (
                "full_overlap",
                datetime(2023, 1, 1),
                datetime(2023, 3, 10),
                False
            ),
    )
)
def test__determine_if_datachunk_is_in_qcone_accepted_time_different_overlaps(name, rejected_starttime, rejected_endtime, is_passing):
    datachunk = Datachunk(component_id=1)
    timespan = Timespan(
        starttime=datetime(2023, 2, 1),
        midtime=datetime(2023, 2, 14, 12),
        endtime=datetime(2023, 2, 28)
    )
    config = QCOneConfig(
        time_periods_rejected=[
            QCOneRejectedTime(
                qcone_config_id=1,
                component_id=1,
                starttime=rejected_starttime,
                endtime=rejected_endtime,
            )
        ],
    )

    accepted_time = _determine_if_datachunk_is_in_qcone_accepted_time(
        datachunk=datachunk,
        timespan=timespan,
        config=config,
    )
    assert datachunk.component_id == 1
    assert len(config.time_periods_rejected) == 1
    assert config.component_ids_rejected_times == (1, )
    assert isinstance(accepted_time, bool)
    assert accepted_time is is_passing


def test__determine_if_datachunk_is_in_qcone_accepted_time_overlap_but_different_component_id():
    datachunk = Datachunk(component_id=1)
    timespan = Timespan(
        starttime=datetime(2023, 2, 1),
        midtime=datetime(2023, 2, 14, 12),
        endtime=datetime(2023, 2, 28)
    )
    config = QCOneConfig(
        time_periods_rejected=[
            QCOneRejectedTime(
                qcone_config_id=1,
                component_id=42,
                starttime=datetime(2023, 1, 1),
                endtime=datetime(2023, 2, 10),
            )
        ],
    )

    accepted_time = _determine_if_datachunk_is_in_qcone_accepted_time(
        datachunk=datachunk,
        timespan=timespan,
        config=config,
    )

    assert isinstance(accepted_time, bool)
    assert accepted_time is True
