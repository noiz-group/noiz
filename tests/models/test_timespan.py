import datetime
import numpy as np
import obspy
import pandas as pd
import pytest

from noiz.models.timespan import Timespan


class TestTimespan:
    params_to_be_instantiated = (
        "starttime, midtime, endtime",
        (
            (datetime.datetime(2019, 1, 1), datetime.datetime(2019, 1, 2), datetime.datetime(2019, 1, 3)),
            (
                pd.Timestamp(2019, 1, 1),
                pd.Timestamp(2019, 1, 2),
                pd.Timestamp(2019, 1, 3),
            ),
            (
                np.datetime64("2019-01-01"),
                np.datetime64("2019-01-02"),
                np.datetime64("2019-01-03"),
            ),
        ),
    )

    params_to_be_assembled = (
        "syear, sdoy, myear, mdoy, eyear, edoy",
        ((2019, 300, 2019, 315, 2019, 330), (2019, 360, 2020, 10, 2020, 320)),
    )

    @pytest.mark.parametrize(*params_to_be_instantiated)
    def test_init(self, starttime, midtime, endtime):
        timespan = Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
        assert isinstance(timespan, Timespan)
        assert isinstance(timespan.starttime, datetime.datetime)
        assert isinstance(timespan.midtime, datetime.datetime)
        assert isinstance(timespan.endtime, datetime.datetime)

    @pytest.mark.parametrize(*params_to_be_assembled)
    def test_hybrid_properties_year_doy(self, syear, sdoy, myear, mdoy, eyear, edoy):
        timespan = Timespan(
            starttime=datetime.datetime.strptime(f"{syear} {sdoy}", "%Y %j"),
            midtime=datetime.datetime.strptime(f"{myear} {mdoy}", "%Y %j"),
            endtime=datetime.datetime.strptime(f"{eyear} {edoy}", "%Y %j"),
        )
        assert timespan.starttime_year == syear
        assert timespan.starttime_doy == sdoy
        assert timespan.midtime_year == myear
        assert timespan.midtime_doy == mdoy
        assert timespan.endtime_year == eyear
        assert timespan.endtime_doy == edoy

    @pytest.mark.parametrize(*params_to_be_assembled)
    def test_casting_UTCDateTime(self, syear, sdoy, myear, mdoy, eyear, edoy):
        timespan = Timespan(
            starttime=datetime.datetime.strptime(f"{syear} {sdoy}", "%Y %j"),
            midtime=datetime.datetime.strptime(f"{myear} {mdoy}", "%Y %j"),
            endtime=datetime.datetime.strptime(f"{eyear} {edoy}", "%Y %j"),
        )
        assert isinstance(timespan.starttime_obspy(), obspy.UTCDateTime)
        assert isinstance(timespan.midtime_obspy(), obspy.UTCDateTime)
        assert isinstance(timespan.endtime_obspy(), obspy.UTCDateTime)

    @pytest.mark.parametrize(*params_to_be_instantiated)
    def test_remove_last_nanosecond(self, starttime, midtime, endtime):
        timespan = Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
        timespan_removed = timespan.remove_last_microsecond()
        assert isinstance(timespan_removed, obspy.UTCDateTime)
        assert (timespan.endtime - pd.Timedelta(microseconds=1)) == pd.Timestamp(
            timespan_removed.datetime
        )

    @pytest.mark.parametrize(
        "timespan_a, same_day",
        (
            (
                Timespan(
                    starttime=datetime.datetime(2019, 1, 1),
                    midtime=datetime.datetime(2019, 1, 15),
                    endtime=datetime.datetime(2019, 1, 30),
                ),
                False,
            ),
            (
                Timespan(
                    starttime=datetime.datetime(2019, 1, 1),
                    midtime=datetime.datetime(2019, 1, 1),
                    endtime=datetime.datetime(2019, 1, 2),
                ),
                True,
            ),
            (
                Timespan(
                    starttime=datetime.datetime(2019, 1, 1),
                    midtime=datetime.datetime(2019, 1, 1),
                    endtime=datetime.datetime(2019, 1, 1, 23, 59),
                ),
                True,
            ),
        ),
    )
    def test_same_day(self, timespan_a, same_day):
        assert timespan_a.same_day() is same_day
