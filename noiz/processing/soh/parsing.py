import pandas as pd
from pathlib import Path

from typing import Tuple, Optional

from noiz.processing.soh.exceptions import (
    UnparsableDateTimeException,
    NoSOHPresentException,
)


def read_single_soh_csv(
    filepath: Path, header_columns: Tuple[str], used_columns: Tuple[str], dtypes: dict
) -> Optional[pd.DataFrame]:
    """
    TODO docstring
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
                + f" One of elements of UTCDateTime column could not be parsed to datetime format.\n"
                + f" Check the file, it might contain single unparsable line.\n"
                + f" {filepath} "
            )

    single_df = single_df.astype(dtypes)
    single_df.index = single_df.index.tz_localize("UTC")

    return single_df


def read_multiple_soh(filepaths, parsing_params):
    """
    TODO docstring
    TODO typing
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
            raise Exception(filepath)
            continue

        if single_df is None:
            continue

        all_dfs.append(single_df)

    try:
        df = pd.concat(all_dfs)
    except ValueError as e:
        raise NoSOHPresentException(str(e))
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
