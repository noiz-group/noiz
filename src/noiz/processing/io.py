# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Collection, Tuple, Any

import warnings
from pathlib import Path
import numpy as np
import pandas as pd


def write_ccfs_to_npz(
    df: pd.DataFrame,
    filepath: Path,
    overwrite: bool = False,
    metadata_keys: Collection[str] = (),
    metadata_values: Collection[Any] = (),
) -> Path:
    if not overwrite and filepath.exists():
        raise FileExistsError(
            f"There already exists file with name {filepath}.Either provide different filepath or use overwrite=True"
        )

    _check_and_append_npz_suffix(filepath)

    metadata_keys_str = _convert_collection_elements_to_str(metadata_keys)
    metadata_values_str = _convert_collection_elements_to_str(metadata_values)

    np.savez(
        file=filepath,
        index=df.index.to_numpy(dtype="datetime64[ns]"),
        columns=df.columns.to_numpy(),
        data=df.to_numpy(),
        metadata_keys=metadata_keys_str,
        metadata_values=metadata_values_str,
    )
    return filepath


def _check_and_append_npz_suffix(filepath: Path) -> Path:
    """
    Checks if a provided filepath has `.npz` as suffix. If not, it appends one, if yes it does nothing.

    :param filepath: Path to be checked
    :type filepath: Path
    :return: Verified path
    :rtype: Path
    """
    if filepath.suffix != ".npz":
        filepath.with_suffix(filepath.suffix + ".npz")
        warnings.warn(
            message=f"Provided path does not have npz suffix. It will be appended and file will be saved to {filepath}",
            stacklevel=1,
        )
    return filepath


def _convert_collection_elements_to_str(col: Collection[Any]) -> Tuple[str, ...]:
    """
    Takes collection as input and convert each element of it to str.

    :param col: Collection of stuff to be converted to str
    :type col: Collection[Any]
    :return: Initial collection but converted to str
    :rtype: Tuple[str, ...]
    """
    return tuple([str(el) for el in col])
