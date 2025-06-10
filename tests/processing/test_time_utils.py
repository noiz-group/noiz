# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from noiz.processing.time_utils import get_year_doy, check_if_two_timeperiods_have_any_overlap
from noiz.validation_helpers import validate_timestamp_as_pdtimestamp


@pytest.mark.parametrize(
    "test_date, year, doy",
    (
        (datetime(2019, 1, 1), 2019, 1),
        (datetime(2018, 3, 16), 2018, 75),
        (datetime(2020, 2, 29), 2020, 60),
    ),
)
def test_get_year_doy(test_date, year, doy):
    res_year, res_doy = get_year_doy(test_date)
    assert res_year == year
    assert res_doy == doy


@pytest.mark.parametrize(
    "test_date",
    (datetime(2019, 1, 1), pd.Timestamp(2017, 10, 30), np.datetime64("2019-05-07")),
)
def test_validate_timestamp(test_date):
    assert isinstance(validate_timestamp_as_pdtimestamp(test_date), pd.Timestamp)


@pytest.mark.parametrize(
    "name, first_period, second_period, is_overlapping",
    (
        (
            "Fully separate",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2022, 1, 1), datetime(2022, 12, 31)),
            False,
        ),
        (
            "Having significant overlap",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2022, 1, 1), datetime(2023, 1, 11)),
            True,
        ),
        (
            "Endtime of one equal to starttime of second",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2022, 1, 1), datetime(2023, 1, 1)),
            True,
        ),
        (
            "One contained fully in the second",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2023, 3, 1), datetime(2023, 4, 1)),
            True,
        ),
        (
            "One contained fully in the second & common end date",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2023, 3, 1), datetime(2023, 12, 31)),
            True,
        ),
        (
            "Equal time periods",
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            (datetime(2023, 1, 1), datetime(2023, 12, 31)),
            True,
        ),
    ),
)
def test_check_if_two_timeperiods_have_any_overlap(name, first_period, second_period, is_overlapping):
    assert is_overlapping == check_if_two_timeperiods_have_any_overlap(
        first_starttime=first_period[0],
        first_endtime=first_period[1],
        second_starttime=second_period[0],
        second_endtime=second_period[1],
    )
    assert is_overlapping == check_if_two_timeperiods_have_any_overlap(
        first_starttime=second_period[0],
        first_endtime=second_period[1],
        second_starttime=first_period[0],
        second_endtime=first_period[1],
    )
