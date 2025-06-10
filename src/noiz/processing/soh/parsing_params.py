# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from __future__ import annotations

import numpy as np
import obspy
import pandas as pd
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional, Mapping
from typing_extensions import Protocol

from noiz.exceptions import UnparsableDateTimeException
from noiz.globals import ExtendedEnum

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


centaur_miniseed_header_columns = (
    "GLA",  # GPS latitude [microdegrees]
    "GLO",  # GPS longitude [microdegrees]
    "GEL",  # GPS elevation [micrometers]
    "VCO",  # VCO control voltage (for timing oscillator) [raw DAC counts]
    "LCQ",  # Clock quality [percent]
    "LCE",  # Absolute clock phase error [microseconds]
    "GNS",  # GPS number of satellites used
    "GAN",  # GPS antenna status
    "GST",  # GPS status
    "GPL",  # GPS PLL status
    "VEC",  # Digitizer system current [miliamps]
    "VEI",  # Input system voltage [milivolts]
    "VDT",  # Digitizer system temperature [10e-3 C]
    "VM1",  # Sensor SOH channel 1
    "EX1",  # External SOH channel 1
    "EX2",  # External SOH channel 2
    "EX3",  # External SOH channel 3
    "VPB",  # Digitizer buffer percent used [%]
)

centaur_miniseed_gpstime_used_columns = ("GST", "GNS", "LCQ", "GPL", "VCO", "LCE", "TIME_UNCERTAINTY")

centaur_miniseed_gpstime_name_mappings = {
    "GST": "GPS receiver status",
    "GNS": "GPS satellites used",
    "LCQ": "Timing status",
    "GPL": "Phase lock loop status",
    "VCO": "Timing DAC count",
    "LCE": "Time error(ms)",
    "TIME_UNCERTAINTY": "Time uncertainty(ms)",
}

centaur_miniseed_gpstime_dtypes = {
    "GPS receiver status": np.float64,
    "GPS satellites used": np.float64,
    "Timing status": np.float64,
    "Phase lock loop status": np.float64,
    "Timing DAC count": np.float64,
    "Time error(ms)": np.float64,
}
centaur_miniseed_instrument_used_columns = (
    "VEI",
    "VEC",
    "VDT",
)

centaur_miniseed_instrument_name_mappings = {
    "VEI": "Supply voltage(V)",
    "VEC": "Total current(A)",
    "VDT": "Temperature(C)",
}

centaur_miniseed_instrument_dtypes = {
    "Supply voltage(V)": np.float64,
    "Total current(A)": np.float64,
    "Temperature(C)": np.float64,
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


class SohInstrumentNames(ExtendedEnum):
    TAURUS = "taurus"
    CENTAUR = "centaur"


class SohType(ExtendedEnum):
    INSTRUMENT = "instrument"
    GPSTIME = "gpstime"
    GNSSTIME = "gnsstime"
    ENVIRONMENT = "environment"
    MINISEED_GPSTIME = "miniseed_gpstime"
    MINISEED_INSTRUMENT = "miniseed_instrument"


def _empty_postprocessor(df: pd.DataFrame) -> pd.DataFrame:
    return df


class Postprocessor(Protocol):
    """
    This is just a callback protocol which defines type for
    :param:`noiz.processing.soh.soh_column_names.SohParsingParams.postprocessor`.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame: ...


class Parser(Protocol):
    """
    This is just a callback protocol which defines type for
    :param:`noiz.processing.soh.soh_column_names.SohParsingParams.parser`.
    """

    def __call__(self, filepath: Path, parsing_params: SohParsingParams) -> pd.DataFrame: ...


@dataclass
class SohParsingParams:
    """
    This is just here implemented for the future.
    The SOHProcessingParams dict should be refactored to use that class
    """

    instrument_name: SohInstrumentNames
    soh_type: SohType
    header_names: Tuple[str, ...]
    used_names: Tuple[str, ...]
    header_dtypes: Mapping[str, type]
    name_mappings: Mapping[str, str]
    search_regex: str
    postprocessor: Postprocessor
    parser: Parser


def _read_single_soh_csv(
    filepath: Path,
    parsing_params: SohParsingParams,
) -> Optional[pd.DataFrame]:
    """
    Takes a filepath to a single CSV file and parses it according to parameters passed.

    :param filepath: File to be parsed
    :type filepath: Path
    :param parsing_params: Parameters to parse with
    :type parsing_params: SohParsingParams
    :return: Returns dataframe if there was anything to parse
    :rtype: Optional[pd.DataFrame]
    """
    try:
        single_df = pd.read_csv(
            filepath,
            index_col=False,
            names=parsing_params.header_names,
            usecols=parsing_params.used_names,
            parse_dates=["UTCDateTime"],
            skiprows=1,
        ).set_index("UTCDateTime")
    except ValueError as e:
        raise UnparsableDateTimeException(f"There is a problem with parsing file.\n {filepath}") from e

    if len(single_df) == 0:
        return None

    if single_df.index.dtype == "O":
        single_df = single_df[~single_df.index.str.contains("Time")]
        try:
            single_df.index = single_df.index.astype("datetime64[ns]")
        except ValueError as e:
            raise UnparsableDateTimeException(
                f"There was a problem with parsing the SOH file.\n"
                f" One of elements of UTCDateTime column could not be parsed to datetime format.\n"
                f" Check the file, it might contain single unparsable line.\n"
                f" {filepath} "
            ) from e

    single_df = single_df.astype(parsing_params.header_dtypes)
    single_df.index = single_df.index.tz_localize("UTC")

    return single_df


def _read_single_soh_miniseed_centaur(
    filepath: Path,
    parsing_params: SohParsingParams,
) -> pd.DataFrame:
    """
    This method reads a miniseed file and looks for channels defined in the
    :class:`~noiz.processing.soh.soh_column_names.SohParsingParams` that is provided as param `parsing_params.
    It also renames all the channels to propoper names.
    It doesn't postprocess data.

    :param filepath: Filepath of miniseed soh
    :type filepath: Path
    :param parsing_params: Parameters object for parsing
    :type parsing_params: SohParsingParams
    :return: Resulting DataFrame with soh data
    :rtype: pd.DataFrame
    """

    st = obspy.read(str(filepath))
    data_read = []
    for channel in parsing_params.used_names:
        st_selected = st.select(channel=channel)
        if len(st_selected) == 0:
            data_read.append(pd.Series(name=parsing_params.name_mappings[channel], dtype=np.float64))
            continue
        for tr in st_selected:
            data_read.append(
                pd.Series(
                    index=[pd.Timestamp.utcfromtimestamp(t) for t in tr.times("timestamp")],
                    data=tr.data,
                    name=parsing_params.name_mappings[channel],
                )
            )

    df = pd.concat(data_read, axis=1)
    df.index = pd.DatetimeIndex(df.index)
    df = df.astype(parsing_params.header_dtypes)
    df.index = df.index.tz_localize("UTC")

    return df


def _postprocess_soh_miniseed_instrument_centaur(df: pd.DataFrame) -> pd.DataFrame:
    """
    This is internal postprocessor routine for unifying values read from files with what is expected in the Noiz db.
    This method is fully internal and used only for specific type of Soh.

    :param df: Dataframe to be processed
    :type df: pd.DataFrame
    :return: Postprocessed dataframe
    :rtype: pd.DataFrame
    """
    df.loc[:, "Supply voltage(V)"] = df.loc[:, "Supply voltage(V)"] / 1000
    df.loc[:, "Total current(A)"] = df.loc[:, "Total current(A)"] / 1000
    df.loc[:, "Temperature(C)"] = df.loc[:, "Temperature(C)"] / 1000
    return df


def _postprocess_soh_miniseed_gpstime_centaur(df: pd.DataFrame) -> pd.DataFrame:
    """
    This is internal postprocessor routine for unifying values read from files with what is expected in the Noiz db.
    This method is fully internal and used only for specific type of Soh.

    It converts ns to ms.

    :param df: Dataframe to be processed
    :type df: pd.DataFrame
    :return: Postprocessed dataframe
    :rtype: pd.DataFrame
    """
    df.loc[:, "Time error(ms)"] = df.loc[:, "Time error(ms)"] / 1000
    return df


def __postprocess_soh_gpstime_gnsstime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocessing of the dataframes coming from Nanometrics devices.
    It recalculates the time GPS time errors from ns to ms.

    :param df: Dataframe to be postprocessed
    :type df: pd.DataFrame
    :return: Postprocessed dataframe
    :rtype: pd.DataFrame
    """

    df["Time uncertainty(ns)"] = df["Time uncertainty(ns)"] / 1000
    df["Time error(ns)"] = df["Time error(ns)"] / 1000

    df = df.rename(
        columns={
            "Time uncertainty(ns)": "Time uncertainty(ms)",
            "Time error(ns)": "Time error(ms)",
        }
    )
    return df


def __postprocess_soh_taurus_instrument(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocessing of the dataframes coming from Nanometrics devices.
    Also, it sums up all the current values of submodules of the Taurus in order to have one value
    that can be compared to the Centaur.

    :param df: Dataframe to be postprocessed
    :type df: pd.DataFrame
    :return: Postprocessed dataframe
    :rtype: pd.DataFrame
    """

    df["Supply voltage(V)"] = df["Supply Voltage(mV)"] / 1000
    df["Total current(A)"] = (
        df.loc[
            :,
            [
                "NMX Bus Current(mA)",
                "Sensor Current(mA)",
                "Serial Port Current(mA)",
                "Controller Current(mA)",
                "Digitizer Current(mA)",
            ],
        ].sum(axis="columns")
        / 1000
    )

    df = df.drop(
        columns=[
            "Supply Voltage(mV)",
            "NMX Bus Current(mA)",
            "Sensor Current(mA)",
            "Serial Port Current(mA)",
            "Controller Current(mA)",
            "Digitizer Current(mA)",
        ]
    )

    return df


__parsing_params_list = (
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.MINISEED_GPSTIME,
        header_names=centaur_miniseed_header_columns,
        used_names=centaur_miniseed_gpstime_used_columns,
        header_dtypes=centaur_miniseed_gpstime_dtypes,
        search_regex="*SOH_*.miniseed",
        postprocessor=_postprocess_soh_miniseed_gpstime_centaur,
        name_mappings=centaur_miniseed_gpstime_name_mappings,
        parser=_read_single_soh_miniseed_centaur,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.MINISEED_INSTRUMENT,
        header_names=centaur_miniseed_header_columns,
        used_names=centaur_miniseed_instrument_used_columns,
        header_dtypes=centaur_miniseed_instrument_dtypes,
        search_regex="*SOH_*.miniseed",
        postprocessor=_postprocess_soh_miniseed_instrument_centaur,
        name_mappings=centaur_miniseed_instrument_name_mappings,
        parser=_read_single_soh_miniseed_centaur,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.INSTRUMENT,
        header_names=centaur_instrument_header_columns,
        used_names=centaur_instrument_used_columns,
        header_dtypes=centaur_instrument_dtypes,
        search_regex="*Instrument*.csv",
        postprocessor=_empty_postprocessor,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.GPSTIME,
        header_names=centaur_gpstime_header_columns,
        used_names=centaur_gpstime_used_columns,
        header_dtypes=centaur_gpstime_dtypes,
        search_regex="*GPSTime*.csv",
        postprocessor=__postprocess_soh_gpstime_gnsstime,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.GNSSTIME,
        header_names=centaur_gnsstime_header_columns,
        used_names=centaur_gnsstime_used_columns,
        header_dtypes=centaur_gnsstime_dtypes,
        search_regex="*GNSSTime*.csv",
        postprocessor=__postprocess_soh_gpstime_gnsstime,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.CENTAUR,
        soh_type=SohType.ENVIRONMENT,
        header_names=centaur_environment_header_columns,
        used_names=centaur_environment_used_columns,
        header_dtypes=centaur_environment_dtypes,
        search_regex="*EnvironmentSOH*.csv",
        postprocessor=_empty_postprocessor,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.INSTRUMENT,
        header_names=taurus_instrument_header_names,
        used_names=taurus_instrument_used_names,
        header_dtypes=taurus_instrument_dtypes,
        search_regex="*Instrument*.csv",
        postprocessor=__postprocess_soh_taurus_instrument,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.GPSTIME,
        header_names=taurus_gpstime_header_names,
        used_names=taurus_gpstime_used_names,
        header_dtypes=taurus_gpstime_dtypes,
        search_regex="*GPSTime*.csv",
        postprocessor=__postprocess_soh_gpstime_gnsstime,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
    SohParsingParams(
        instrument_name=SohInstrumentNames.TAURUS,
        soh_type=SohType.ENVIRONMENT,
        header_names=taurus_environment_header_names,
        used_names=taurus_environment_used_names,
        header_dtypes=taurus_environment_dtypes,
        search_regex="*EnvironmentSOH*.csv",
        postprocessor=_empty_postprocessor,
        name_mappings={},
        parser=_read_single_soh_csv,
    ),
)

__soh_parsing_params = defaultdict(dict)  # type: ignore

for item in __parsing_params_list:
    __soh_parsing_params[item.instrument_name][item.soh_type] = item

SOH_PARSING_PARAMETERS = dict(__soh_parsing_params)


def load_parsing_parameters(soh_type: str, station_type: str) -> SohParsingParams:
    """
    Checks if provided soh_type and station_type are valid names and then checks if a given combination
    of station_type and soh_type have SohParsingParams associated with them.

    :param soh_type: Type of soh to be queried
    :type soh_type: str
    :param station_type: Type of station to be queried
    :type station_type: str
    :return: Valid SohParsingParams
    :rtype: SohParsingParams
    raises: ValueError
    """

    _station_type = validate_soh_instrument_name(station_type)

    _soh_type = validate_soh_type(soh_type)

    if _soh_type not in SOH_PARSING_PARAMETERS[_station_type].keys():
        raise ValueError(
            f"Not supported soh type for this station type. "
            f"For this station type the supported soh types are: "
            f"{SOH_PARSING_PARAMETERS[_station_type].keys()}, "
            f"You provided {_soh_type}"
        )

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
    except ValueError as e:
        raise ValueError(
            f"Not supported soh type. Supported types are: {list(SohType)}, You provided {soh_type}"
        ) from e
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
    except ValueError as e:
        raise ValueError(
            f"Not supported station type. Supported types are: {list(SohInstrumentNames)}, You provided {station_type}"
        ) from e
    return _station_type
