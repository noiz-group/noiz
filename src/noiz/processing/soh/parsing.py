import pandas as pd
from pathlib import Path

from typing import Tuple, Optional, Dict, Type, Collection, Generator, Union

from noiz.exceptions import UnparsableDateTimeException, NoSOHPresentException, SohParsingException


def read_single_soh_csv(
        filepath: Path,
        header_columns: Tuple[str],
        used_columns: Tuple[str],
        dtypes: Dict[str, Type],
) -> Optional[pd.DataFrame]:
    """
    Takes a filepath to a single CSV file and parses it according to parameters passed.


    :param filepath: File to be parsed
    :type filepath: Path
    :param header_columns: Header column names inside of the file
    :type header_columns: Tuple[str]
    :param used_columns: Header column names to be extracted
    :type used_columns: Tuple[str]
    :param dtypes:
    :type dtypes:
    :return:
    :rtype:
    """
    try:
        single_df = pd.read_csv(
            filepath,
            index_col=False,
            names=header_columns,
            usecols=used_columns,
            parse_dates=["UTCDateTime"],
            skiprows=1,
        ).set_index("UTCDateTime")
    except ValueError:
        raise UnparsableDateTimeException(
            f"There is a problem with parsing file.\n {filepath}"
        )

    if len(single_df) == 0:
        return None

    if single_df.index.dtype == "O":
        single_df = single_df[~single_df.index.str.contains("Time")]
        try:
            single_df.index = single_df.index.astype("datetime64[ns]")
        except ValueError:
            raise UnparsableDateTimeException(
                f"There was a problem with parsing the SOH file.\n"
                f" One of elements of UTCDateTime column could not be parsed to datetime format.\n"
                f" Check the file, it might contain single unparsable line.\n"
                f" {filepath} "
            )

    single_df = single_df.astype(dtypes)
    single_df.index = single_df.index.tz_localize("UTC")

    return single_df


def read_multiple_soh(
        filepaths: Union[Collection[Path], Generator[Path, None, None]],
        parsing_params: Dict
) -> pd.DataFrame:
    """
    Method that takes a collection of Paths and iterates over them trying to parse each of them according
    to the provided parsing parameters.
    In the end it concatenates them and returns a single dataframe.

    :param filepaths: Filepaths to csv files that are supposed to be parsed
    :type filepaths: Collection[Path]
    :param parsing_params: Parsing parameters dictionary
    :type parsing_params: Dict
    :return: Dataframe containing all parsed values
    :rtype: pd.DataFrame
    """
    all_dfs = []
    for filepath in filepaths:
        try:
            single_df = read_single_soh_csv(
                filepath=filepath,
                header_columns=parsing_params["header_columns"],
                used_columns=parsing_params["used_columns"],
                dtypes=parsing_params["dtypes"],
            )
        except UnparsableDateTimeException as e:
            raise UnparsableDateTimeException(f"{filepath} has raised exception {e}")

        if single_df is None:
            continue

        all_dfs.append(single_df)

    if len(all_dfs) == 0:
        raise SohParsingException('There were no parsable SOH among provided filepaths.')

    try:
        df = pd.concat(all_dfs)
    except ValueError as e:
        raise SohParsingException(f"There was an exception raised by pd.concat. The exception was: {e}")
    df = df.sort_index()

    return df


def postprocess_soh_dataframe(df, station_type, soh_type):
    """
    TODO docstring
    TODO typing
    """

    if soh_type.lower() in ("gpstime", "gnsstime"):
        df["Time uncertainty(ns)"] = df["Time uncertainty(ns)"] / 1000
        df["Time error(ns)"] = df["Time error(ns)"] / 1000

        df = df.rename(
            columns={
                "Time uncertainty(ns)": "Time uncertainty(ms)",
                "Time error(ns)": "Time error(ms)",
            }
        )

    if (station_type == "taurus") and (soh_type.lower() == "instrument"):
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
