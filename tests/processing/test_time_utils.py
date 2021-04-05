import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from noiz.processing.time_utils import get_year_doy
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
