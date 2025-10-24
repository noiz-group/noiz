# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class TempAverageGpsSoh:
    z_component_id: int
    all_components: set
    device_id: int
    time_error: float
    time_uncertainty: float
    timespan_id: int


def __calculate_mean_gps_soh(df: pd.DataFrame, timespan_id: int) -> List[TempAverageGpsSoh]:
    """
    Method that calculates mean of the DataFrame groupped by column z_component_id.
    It accepts DataFrames structured according a special query including column of
    component_id.

    :param df: DataFrame with results to be averaged
    :type df: pd.DataFrame
    :param timespan_id: Id of Timespan that those values are averaged for
    :type timespan_id: int
    :return: Returns a list of averaged data in structured with TempAverageGpsSoh
    :rtype: List[TempAverageGpsSoh]
    :raises: ValueError
    """
    if len(df) == 0:
        raise ValueError("Provided DataFrame was empty")

    # For numeric columns (time_error, time_uncertainty), calculate mean
    # For device_id (ULID string), take first value since all rows in a group should have same device_id
    averaged = (
        df.drop(["component_id", "id"], axis=1)
        .groupby("z_component_id")
        .agg(
            {
                "time_error": "mean",
                "time_uncertainty": "mean",
                "device_id": "first",  # device_id is now a ULID string, take first value
                "datetime": "first",  # Keep datetime column if present
            }
        )
    )

    res = []
    for z_component_id, row in averaged.iterrows():
        all_components = df.loc[df.loc[:, "z_component_id"] == z_component_id, "component_id"].drop_duplicates()

        res.append(
            TempAverageGpsSoh(
                z_component_id=z_component_id,
                all_components=all_components.values,
                device_id=row["device_id"],
                timespan_id=timespan_id,
                time_error=row["time_error"],
                time_uncertainty=row["time_uncertainty"],
            )
        )

    return res
