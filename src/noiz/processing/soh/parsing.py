# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pandas as pd
from pathlib import Path
from typing import Dict, Collection, Generator, Union

from noiz.exceptions import UnparsableDateTimeException, SohParsingException
from noiz.processing.soh.parsing_params import SohParsingParams


def read_multiple_soh(
        filepaths: Union[Collection[Path], Generator[Path, None, None]],
        parsing_params: SohParsingParams,
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
    from tqdm import tqdm
    all_dfs = []
    for filepath in tqdm(filepaths):
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


def _glob_soh_directory(
        parsing_parameters: SohParsingParams,
        main_filepath: Path
) -> Generator[Path, None, None]:
    """
    Method that uses Path.rglob to find all files in main_filepath that fit a globbing string defined in
    parsing_parameters.search_regex

    :param parsing_parameters: Parsing parameters to be used
    :type parsing_parameters: SohParsingParams
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
