import numpy as np
from typing import Dict, Tuple, Type
from enum import Enum
from collections import defaultdict

from dataclasses import dataclass

taurus_instrument_header_names = (
    "timestamp",
    "UTCDateTime",
    "Supply Voltage(mV)",
    "Temperature(C)",
    "NMX Bus Current(mA)",
    "Sensor Current(mA)",
    "Serial Port Current(mA)",
    "Controller Current(mA)",
    "Digitizer Current(mA)",
)

taurus_instrument_used_names = (
    "UTCDateTime",
    "Supply Voltage(mV)",
    "Temperature(C)",
    "NMX Bus Current(mA)",
    "Sensor Current(mA)",
    "Serial Port Current(mA)",
    "Controller Current(mA)",
    "Digitizer Current(mA)",
)

taurus_instrument_dtypes = {
    "Supply Voltage(mV)": np.float64,
    "Temperature(C)": np.float64,
    "NMX Bus Current(mA)": np.float64,
    "Sensor Current(mA)": np.float64,
    "Serial Port Current(mA)": np.float64,
    "Controller Current(mA)": np.float64,
    "Digitizer Current(mA)": np.float64,
}

taurus_environment_header_names = (
    "timestamp",
    "UTCDateTime",
    "External SOH Voltage 1(V)",
    "External SOH Voltage 2(V)",
    "External SOH Voltage 3(V)",
    "External SOH Voltage 4(V)",
    "Sensor SOH Voltage 1(V)",
    "Sensor SOH Voltage 2(V)",
    "Sensor SOH Voltage 3(V)",
)

taurus_environment_used_names = (
    "UTCDateTime",
    "External SOH Voltage 1(V)",
    "External SOH Voltage 2(V)",
    "External SOH Voltage 3(V)",
    "External SOH Voltage 4(V)",
    "Sensor SOH Voltage 1(V)",
    "Sensor SOH Voltage 2(V)",
    "Sensor SOH Voltage 3(V)",
)

taurus_environment_dtypes = {
    "External SOH Voltage 1(V)": np.float64,
    "External SOH Voltage 2(V)": np.float64,
    "External SOH Voltage 3(V)": np.float64,
    "External SOH Voltage 4(V)": np.float64,
    "Sensor SOH Voltage 1(V)": np.float64,
    "Sensor SOH Voltage 2(V)": np.float64,
    "Sensor SOH Voltage 3(V)": np.float64,
}

taurus_gpstime_header_names = (
    "timestamp",
    "UTCDateTime",
    "Earth Location",
    "GPS receiver status",
    "GPS satellites used",
    "GPS PDOP",
    "GPS TDOP",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time error(ns)",
    "GPS Last Update Time",
)

taurus_gpstime_used_names = (
    "UTCDateTime",
    "GPS receiver status",
    "GPS satellites used",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time error(ns)",
)

taurus_gpstime_dtypes = {
    "GPS receiver status": str,
    "GPS satellites used": np.int8,
    "Timing status": str,
    "Phase lock loop status": str,
    "Time uncertainty(ns)": np.int32,
    "Timing DAC count": np.int32,
    "Time error(ns)": np.int32,
}

centaur_instrument_header_columns = (
    "timestamp",
    "UTCDateTime",
    "Supply voltage(V)",
    "Total current(A)",
    "Temperature(C)",
)

centaur_instrument_used_columns = (
    "UTCDateTime",
    "Supply voltage(V)",
    "Total current(A)",
    "Temperature(C)",
)

centaur_instrument_dtypes = {
    "Supply voltage(V)": np.float64,
    "Total current(A)": np.float64,
    "Temperature(C)": np.float64,
}


centaur_environment_header_columns = (
    "timestamp",
    "UTCDateTime",
    "Maximum mass position 1(mV)",
    "Sensor SOH Voltage 1(mV)",
    "Sensor SOH Voltage 2(mV)",
    "Sensor SOH Voltage 3(mV)",
    "External SOH Voltage 1(V)",
    "External SOH Voltage 2(V)",
    "External SOH Voltage 3(V)",
)

centaur_environment_used_columns = (
    "UTCDateTime",
    "Maximum mass position 1(mV)",
    "Sensor SOH Voltage 1(mV)",
    "Sensor SOH Voltage 2(mV)",
    "Sensor SOH Voltage 3(mV)",
    "External SOH Voltage 1(V)",
    "External SOH Voltage 2(V)",
    "External SOH Voltage 3(V)",
)


centaur_environment_dtypes = {
    "Maximum mass position 1(mV)": np.float64,
    "Sensor SOH Voltage 1(mV)": np.float64,
    "Sensor SOH Voltage 2(mV)": np.float64,
    "Sensor SOH Voltage 3(mV)": np.float64,
    "External SOH Voltage 1(V)": np.float64,
    "External SOH Voltage 2(V)": np.float64,
    "External SOH Voltage 3(V)": np.float64,
}


centaur_gpstime_header_columns = (
    "timestamp",
    "UTCDateTime",
    "Earth Location",
    "GPS receiver status",
    "GPS satellites used",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time quality(%)",
    "Time error(ns)",
)

centaur_gpstime_used_columns = (
    "UTCDateTime",
    "GPS receiver status",
    "GPS satellites used",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time quality(%)",
    "Time error(ns)",
)

centaur_gpstime_dtypes = {
    "GPS receiver status": str,
    "GPS satellites used": np.int8,
    "Timing status": str,
    "Phase lock loop status": str,
    "Time uncertainty(ns)": np.int32,
    "Timing DAC count": np.int32,
    "Time quality(%)": np.int8,
    "Time error(ns)": np.int32,
}

centaur_gnsstime_header_columns = (
    "timestamp",
    "UTCDateTime",
    "Earth Location",
    "GPS receiver status",
    "GPS satellites used",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time quality(%)",
    "Time error(ns)",
)

centaur_gnsstime_used_columns = (
    "UTCDateTime",
    "GPS receiver status",
    "GPS satellites used",
    "Timing status",
    "Phase lock loop status",
    "Time uncertainty(ns)",
    "Timing DAC count",
    "Time quality(%)",
    "Time error(ns)",
)

centaur_gnsstime_dtypes = {
    "GPS receiver status": str,
    "GPS satellites used": np.int8,
    "Timing status": str,
    "Phase lock loop status": str,
    "Time uncertainty(ns)": np.int32,
    "Timing DAC count": np.int32,
    "Time quality(%)": np.int8,
    "Time error(ns)": np.int32,
}


class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))  # type: ignore


class SohInstrumentNames(ExtendedEnum):
    TAURUS = "taurus"
    CENTAUR = "centaur"


class SohType(ExtendedEnum):
    INSTRUMENT = "instrument"
    GPSTIME = "gpstime"
    GNSSTIME = "gnsstime"
    ENVIRONMENT = "environment"


@dataclass
class SohCSVParsingParams:
    """
    This is just here implemented for the future.
    The SOHProcessingParams dict should be refactored to use that class
    """
    instrument_name: SohInstrumentNames
    soh_type: SohType
    header_names: Tuple[str, ...]
    used_names: Tuple[str, ...]
    header_dtypes: Dict[str, type]
    search_regex: str


__parsing_params_list = (
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.INSTRUMENT,
        header_names=centaur_instrument_header_columns,
        used_names=centaur_instrument_used_columns,
        header_dtypes=centaur_instrument_dtypes,
        search_regex="*Instrument*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.GPSTIME,
        header_names=centaur_gpstime_header_columns,
        used_names=centaur_gpstime_used_columns,
        header_dtypes=centaur_gpstime_dtypes,
        search_regex="*GPSTime*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.GNSSTIME,
        header_names=centaur_gnsstime_header_columns,
        used_names=centaur_gnsstime_used_columns,
        header_dtypes=centaur_gnsstime_dtypes,
        search_regex="*GNSSTime*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.ENVIRONMENT,
        header_names=centaur_environment_header_columns,
        used_names=centaur_environment_used_columns,
        header_dtypes=centaur_environment_dtypes,
        search_regex="*EnvironmentSOH*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.INSTRUMENT,
        header_names=taurus_instrument_header_names,
        used_names=taurus_instrument_used_names,
        header_dtypes=taurus_instrument_dtypes,
        search_regex="*Instrument*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.GPSTIME,
        header_names=taurus_gpstime_header_names,
        used_names=taurus_gpstime_used_names,
        header_dtypes=taurus_gpstime_dtypes,
        search_regex="*GPSTime*.csv",
    ),
    SohCSVParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.ENVIRONMENT,
        header_names=taurus_environment_header_names,
        used_names=taurus_environment_used_names,
        header_dtypes=taurus_environment_dtypes,
        search_regex='*EnvironmentSOH*.csv',
    ),
)

# SOH_PARSING_PARAMETERS = {
#     "centaur": {
#         "instrument": {
#             "header_columns": centaur_instrument_header_columns,
#             "used_columns": centaur_instrument_used_columns,
#             "dtypes": centaur_instrument_dtypes,
#             "search_regex": "*Instrument*.csv",
#         },
#         "gpstime": {
#             "header_columns": centaur_gpstime_header_columns,
#             "used_columns": centaur_gpstime_used_columns,
#             "dtypes": centaur_gpstime_dtypes,
#             "search_regex": "*GPSTime*.csv",
#         },
#         "gnsstime": {
#             "header_columns": centaur_gnsstime_header_columns,
#             "used_columns": centaur_gnsstime_used_columns,
#             "dtypes": centaur_gnsstime_dtypes,
#             "search_regex": "*GNSSTime*.csv",
#         },
#         "environment": {
#             "header_columns": centaur_environment_header_columns,
#             "used_columns": centaur_environment_used_columns,
#             "dtypes": centaur_environment_dtypes,
#             'search_regex': '*EnvironmentSOH*.csv',
#         },
#     },
#     "taurus": {
#         "instrument": {
#             "header_columns": taurus_instrument_header_names,
#             "used_columns": taurus_instrument_used_names,
#             "dtypes": taurus_instrument_dtypes,
#             "search_regex": "*Instrument*.csv",
#         },
#         "gpstime": {
#             "header_columns": taurus_gpstime_header_names,
#             "used_columns": taurus_gpstime_used_names,
#             "dtypes": taurus_gpstime_dtypes,
#             "search_regex": "*GPSTime*.csv",
#         },
#         "environment": {
#             "header_columns": taurus_environment_header_names,
#             "used_columns": taurus_environment_used_names,
#             "dtypes": taurus_environment_dtypes,
#             'search_regex': '*EnvironmentSOH*.csv',
#         },
#     },
# }

__soh_parsing_params = defaultdict(dict)  # type: ignore

for item in __parsing_params_list:
    __soh_parsing_params[item.instrument_name][item.soh_type] = item

SOH_PARSING_PARAMETERS = dict(__soh_parsing_params)


def load_parsing_parameters(soh_type: str, station_type: str) -> SohCSVParsingParams:
    """
    Checks if provided soh_type and station_type are valid names and then checks if a given combination
    of station_type and soh_type have SohCSVParsingParams associated with them.

    :param soh_type: Type of soh to be queried
    :type soh_type: str
    :param station_type: Type of station to be queried
    :type station_type: str
    :return: Valid SohCSVParsingParams
    :rtype: SohCSVParsingParams
    raises: ValueError
    """

    _station_type = validate_soh_instrument_name(station_type)

    _soh_type = validate_soh_type(soh_type)

    if _soh_type not in SOH_PARSING_PARAMETERS[_station_type].keys():
        raise ValueError(f"Not supported soh type for this station type. "
                         f"For this station type the supported soh types are: "
                         f"{SOH_PARSING_PARAMETERS[_station_type].keys()}, "
                         f"You provided {_soh_type}")

    parsing_parameters = SOH_PARSING_PARAMETERS[_station_type][_soh_type]

    return parsing_parameters


def validate_soh_type(soh_type: str) -> SohType:
    """
    Validates if provided soh_name is a valid SohType

    :param soh_type: Name to be validated
    :type soh_type: str
    :return: Valid SohType
    :rtype: SohType
    :raises: ValueError
    """
    try:
        _soh_type = SohType(soh_type)
    except ValueError:
        raise ValueError(f"Not supported soh type. Supported types are: {list(SohType)}, "
                         f"You provided {soh_type}")
    return _soh_type


def validate_soh_instrument_name(station_type: str) -> SohInstrumentNames:
    """
    Validates if provided station_type is a valid SohInstrumentNames

    :param station_type: Name to be validated
    :type station_type:
    :return: Valid SohInstrumentNames
    :rtype: SohInstrumentNames
    :raises: ValueError
    """
    try:
        _station_type = SohInstrumentNames(station_type)
    except ValueError:
        raise ValueError(f"Not supported station type. Supported types are: {list(SohInstrumentNames)}, "
                         f"You provided {station_type}")
    return _station_type