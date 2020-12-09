import numpy as np
import obspy
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict, Collection, Generator, Union

from noiz.exceptions import UnparsableDateTimeException, SohParsingException
from noiz.processing.soh.soh_column_names import SohCSVParsingParams


def _read_single_soh_miniseed_centaur(
        filepath: Path,
        parsing_params: SohCSVParsingParams,
) -> pd.DataFrame:
    """
    This method reads a miniseed file and looks for channels defined in the
    :class:`~noiz.processing.soh.soh_column_names.SohCSVParsingParams` that is provided as param `parsing_params.
    It also renames all the channels to propoper names.
    It doesn't postprocess data.

    :param filepath: Filepath of miniseed soh
    :type filepath: Path
    :param parsing_params: Parameters object for parsing
    :type parsing_params: SohCSVParsingParams
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
                    index=[pd.Timestamp.fromtimestamp(t) for t in tr.times('timestamp')],
                    data=tr.data,
                    name=parsing_params.name_mappings[channel]
                )
            )

    df = pd.concat(data_read, axis=1)
    df.index = pd.DatetimeIndex(df.index)
    df = df.astype(parsing_params.header_dtypes)
    df.index = df.index.tz_localize("UTC")

    return df


def _postprocess_soh_miniseed_instrument_centaur(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Supply voltage(V)'] = df.loc[:, 'Supply voltage(V)']/1000
    df.loc[:, 'Total current(A)'] = df.loc[:, 'Total current(A)']/1000
    df.loc[:, 'Temperature(C)'] = df.loc[:, 'Temperature(C)']/1000
    return df


def read_single_soh_csv(
        filepath: Path,
        parsing_params: SohCSVParsingParams,
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
            names=parsing_params.header_names,
            usecols=parsing_params.used_names,
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

    single_df = single_df.astype(parsing_params.header_dtypes)
    single_df.index = single_df.index.tz_localize("UTC")

    return single_df


def read_multiple_soh(
        filepaths: Union[Collection[Path], Generator[Path, None, None]],
        parsing_params: SohCSVParsingParams,
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
            single_df = parsing_params.parser(
                filepath=filepath,
                parsing_params=parsing_params,
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

    df = parsing_params.postprocessor(df=df)

    return df


def __postprocess_soh_dataframe(df: pd.DataFrame, parsing_params: SohCSVParsingParams) -> pd.DataFrame:
    """
    Postprocessing of the dataframes coming from Nanometrics devices.
    It recalculates the time GPS time errors from ns to ms.
    Also, it sums up all the current values of submodules of the Taurus in order to have one value
    that can be compared to the Centaur.

    :param df: Dataframe to be postprocessed
    :type df: pd.DataFrame
    :param parsing_params: Soh processing params
    :type parsing_params: SohCSVParsingParams
    :return: Postprocessed dataframe
    :rtype: pd.DataFrame
    """

    from noiz.processing.soh.soh_column_names import SohType, SohInstrumentNames
    if parsing_params.soh_type in (SohType.GPSTIME, SohType.GNSSTIME):
        df["Time uncertainty(ns)"] = df["Time uncertainty(ns)"] / 1000
        df["Time error(ns)"] = df["Time error(ns)"] / 1000

        df = df.rename(
            columns={
                "Time uncertainty(ns)": "Time uncertainty(ms)",
                "Time error(ns)": "Time error(ms)",
            }
        )

    if (parsing_params.instrument_name == SohInstrumentNames.TAURUS)\
            and (parsing_params.soh_type == SohType.INSTRUMENT):
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


def glob_soh_directory(
        parsing_parameters: SohCSVParsingParams,
        main_filepath: Path
) -> Generator[Path, None, None]:
    """
    Method that uses Path.rglob to find all files in main_filepath that fit a globbing string defined in
    parsing_parameters.search_regex

    :param parsing_parameters: Parsing parameters to be used
    :type parsing_parameters: SohCSVParsingParams
    :param main_filepath: Directory to be rglobbed
    :type main_filepath: Path
    :return: Paths to files fitting the search_regex
    :rtype: Generator[Path, None, None]
    """

    if not isinstance(main_filepath, Path):
        if not isinstance(main_filepath, str):
            raise ValueError(f"Expected a filepath to the directory. Got {main_filepath}")
        else:
            main_filepath = Path(main_filepath)

    if not main_filepath.exists():
        raise FileNotFoundError(f"Provided path does not exist. {main_filepath}")

    if not main_filepath.is_dir():
        raise NotADirectoryError(f"It is not a directory! {main_filepath}")

    filepaths_to_parse = main_filepath.rglob(parsing_parameters.search_regex)

    return filepaths_to_parse
