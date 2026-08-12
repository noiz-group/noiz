# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""
Module for exporting stacked cross-correlations to HDF5 format.

HDF5 File Structure:
    Dataset                 Shape                   Dtype       Description
    ---------------------   ---------------------   --------    -----------
    global_stacks           (n_couples, n_time)     float64     Time-domain cross-correlations
    time_axis_corr          (n_time_samples,)       float64     Time axis in seconds
    f_intervals             (2, 1)                  float64     Frequency band: [[fmin], [fmax]]
    id_couples              (2, n_couples)          int         Station pair indices: [i_rec; j_rec]
    position_matrix         (2, n_couples)          float64     Separation vectors: [xj-xi; yj-yi] (meters)
    loc_matrix              (2, n_stations)         float64     Station coordinates: [x; y] (meters)
"""

import datetime
import h5py
import numpy as np
import pandas as pd
import os
import numpy.typing as npt
from loguru import logger
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Collection

from noiz.models import CCFStack, StackingTimespan, Component
from noiz.api.component_pair import fetch_componentpairs_cartesian, fetch_componentpairs_cartesian_by_id


def ensure_odd_samples_with_zero(
    time_axis: npt.NDArray[np.float64],
    data: npt.NDArray[np.float64],
) -> Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """
    Ensure the time axis has an odd number of samples and contains exactly 0.

    This function adjusts (crops) the time axis and corresponding data to ensure:
    1. The number of samples is odd
    2. The time axis contains exactly 0

    No interpolation is performed - only cropping/adjustment.

    :param time_axis: Original time axis in seconds
    :type time_axis: npt.NDArray[np.float64]
    :param data: Corresponding data array (can be 1D or 2D with time as last axis)
    :type data: npt.NDArray[np.float64]
    :return: Tuple of (adjusted_time_axis, adjusted_data)
    :rtype: Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]
    """
    n_samples = len(time_axis)

    # Find the index closest to zero
    zero_idx = np.argmin(np.abs(time_axis))

    # Check if exact zero exists (within floating point tolerance)
    if not np.isclose(time_axis[zero_idx], 0.0, atol=1e-10):
        logger.warning(f"Time axis does not contain exact zero. Closest value: {time_axis[zero_idx]}")

    # Determine the symmetric range around zero
    samples_before_zero = int(zero_idx)
    samples_after_zero = int(n_samples - zero_idx - 1)

    # Use the minimum to ensure symmetry
    half_width = min(samples_before_zero, samples_after_zero)

    # Ensure odd number of samples (2*half_width + 1 is always odd)
    start_idx = int(zero_idx) - half_width
    end_idx = int(zero_idx) + half_width + 1  # +1 because slice is exclusive

    adjusted_time_axis = time_axis[start_idx:end_idx]

    # Handle both 1D and 2D data
    if data.ndim == 1:
        adjusted_data = data[start_idx:end_idx]
    else:
        # Assume time is the last axis
        adjusted_data = data[..., start_idx:end_idx]

    # Verify conditions
    assert len(adjusted_time_axis) % 2 == 1, "Time axis should have odd number of samples"
    mid_idx = len(adjusted_time_axis) // 2

    # Set the center point to exactly zero (within floating point precision)
    if not np.isclose(adjusted_time_axis[mid_idx], 0.0, atol=1e-10):
        logger.warning(f"Center of adjusted time axis is not exactly zero: {adjusted_time_axis[mid_idx]}")
        # Force it to be exactly zero
        adjusted_time_axis[mid_idx] = 0.0

    return adjusted_time_axis, adjusted_data


# def export_stacks_to_h5(
#     stacks: Collection[CCFStack],
#     stacking_timespan: StackingTimespan,
#     output_dir: Path,
#     time_axis: npt.NDArray[np.float64],
#     fmin: float,
#     fmax: float,
#     components: Optional[Dict[int, Component]] = None,
# ) -> Path:
#     """
#     Export stacked cross-correlations for a single stacking timespan to HDF5 format.

#     :param stacks: Collection of CCFStack objects for this timespan
#     :type stacks: Collection[CCFStack]
#     :param stacking_timespan: The stacking timespan object
#     :type stacking_timespan: StackingTimespan
#     :param output_dir: Directory to save the H5 file
#     :type output_dir: Path
#     :param time_axis: Time axis for the cross-correlations
#     :type time_axis: npt.NDArray[np.float64]
#     :param fmin: Minimum frequency of the processed data
#     :type fmin: float
#     :param fmax: Maximum frequency of the processed data
#     :type fmax: float
#     :param components: Optional dictionary mapping component IDs to Component objects
#     :type components: Optional[Dict[int, Component]]
#     :return: Path to the created H5 file
#     :rtype: Path
#     """
#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Format filename with start and end times
#     start_str = stacking_timespan.starttime.strftime("%Y%m%dT%H%M%S")
#     end_str = stacking_timespan.endtime.strftime("%Y%m%dT%H%M%S")
#     filename = f"stacks_{start_str}_{end_str}.h5"
#     filepath = output_dir / filename

#     stacks_list = list(stacks)
#     n_couples = len(stacks_list)

#     if n_couples == 0:
#         logger.warning(f"No stacks to export for timespan {stacking_timespan}")
#         return filepath

#     # Build the global stacks array
#     global_stacks = np.array([stack.stack for stack in stacks_list], dtype=np.float64)

#     # Ensure odd samples and exact zero in time axis => ne fonctionne pas
#     time_axis_adjusted, global_stacks_adjusted = ensure_odd_samples_with_zero(
#         time_axis=time_axis,
#         data=global_stacks,
#     )

#     n_time_samples = len(time_axis_adjusted)

#     # Build id_couples (station pair indices)
#     # We use component IDs as the station indices
#     id_couples = np.zeros((2, n_couples), dtype=np.int64)
#     for i, stack in enumerate(stacks_list):
#         componentpair = stack.stacking_schema.crosscorrelation_cartesian_params.processed_datachunk_params
#         # Access the component pair from the stack
#         if hasattr(stack, 'componentpair_id'):
#             # We need to get the component pair to access component IDs
#             pass

#     # Build position_matrix (separation vectors)
#     position_matrix = np.zeros((2, n_couples), dtype=np.float64)

#     # Build loc_matrix (station coordinates)
#     # First, collect unique components
#     unique_component_ids = set()
#     for stack in stacks_list:
#         # Access through the relationship - need to load component pair
#         pass

#     # For now, build basic arrays from available data
#     id_couples = np.zeros((2, n_couples), dtype=np.int64)
#     position_matrix = np.zeros((2, n_couples), dtype=np.float64)

#     # Collect component info from stacks
#     component_id_to_idx = {}
#     all_component_ids = []

#     for i, stack in enumerate(stacks_list):
#         # Get component pair info - need to access through componentpair relationship
#         # This requires the relationship to be loaded
#         cpair = getattr(stack, '_componentpair_cartesian', None)
#         if cpair is None:
#             # Try to get from componentpair_id using provided components dict
#             cpair_id = stack.componentpair_id
#             if components:
#                 # We'd need the ComponentPairCartesian, not just Components
#                 pass

#         # For now, use componentpair_id as placeholder
#         id_couples[0, i] = stack.componentpair_id
#         id_couples[1, i] = stack.componentpair_id

#     # Create a minimal loc_matrix
#     n_stations = len(set(id_couples.flatten()))
#     loc_matrix = np.zeros((2, max(n_stations, 1)), dtype=np.float64)

#     # Frequency interval
#     f_intervals = np.array([[fmin], [fmax]], dtype=np.float64)

#     logger.info(f"Exporting {n_couples} stacks to {filepath}")

#     with h5py.File(filepath, 'w') as hf:
#         hf.create_dataset('global_stacks', data=global_stacks_adjusted, dtype='float64')
#         hf.create_dataset('time_axis_corr', data=time_axis_adjusted, dtype='float64')
#         hf.create_dataset('f_intervals', data=f_intervals, dtype='float64')
#         hf.create_dataset('id_couples', data=id_couples, dtype='int64')
#         hf.create_dataset('position_matrix', data=position_matrix, dtype='float64')
#         hf.create_dataset('loc_matrix', data=loc_matrix, dtype='float64')

#         # Add metadata attributes
#         hf.attrs['starttime'] = start_str
#         hf.attrs['endtime'] = end_str
#         hf.attrs['n_couples'] = n_couples
#         hf.attrs['n_time_samples'] = n_time_samples
#         hf.attrs['stacking_timespan_id'] = stacking_timespan.id
#         hf.attrs['stacking_schema_id'] = stacking_timespan.stacking_schema_id

#     logger.info(f"Successfully exported stacks to {filepath}")
#     return filepath


def export_stacks_to_h5_full(
    stacks: Collection[CCFStack],
    stacking_timespan: StackingTimespan,
    output_dir: Path,
    time_axis: npt.NDArray[np.float64],
    station_to_reject,
    fmin: float,
    fmax: float,
    component_pairs_info: Dict[int, Tuple[Component, Component]],
) -> Path:
    """
    Export stacked cross-correlations with full component information to HDF5 format.

    This version requires pre-loaded component pair information for accurate
    station coordinates and separation vectors.

    :param stacks: Collection of CCFStack objects for this timespan
    :type stacks: Collection[CCFStack]
    :param stacking_timespan: The stacking timespan object
    :type stacking_timespan: StackingTimespan
    :param output_dir: Directory to save the H5 file
    :type output_dir: Path
    :param time_axis: Time axis for the cross-correlations
    :type time_axis: npt.NDArray[np.float64]
    :param fmin: Minimum frequency of the processed data
    :type fmin: float
    :param fmax: Maximum frequency of the processed data
    :type fmax: float
    :param component_pairs_info: Dictionary mapping componentpair_id to tuple of (component_a, component_b)
    :type component_pairs_info: Dict[int, Tuple[Component, Component]]
    :return: Path to the created H5 file
    :rtype: Path
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Format filename with start and end times
    start_str = stacking_timespan.starttime.strftime("%Y%m%dT%H%M%S")
    end_str = stacking_timespan.endtime.strftime("%Y%m%dT%H%M%S")
    filename = f"stacks_{start_str}_{end_str}.h5"
    filepath = output_dir / filename

    stacks_list = list(stacks)
    if station_to_reject:
        idx = []
        cpp_list = [stack.componentpair_id for stack in stacks_list]
        cpp = fetch_componentpairs_cartesian_by_id(tuple(cpp_list))
        order = {value: i for i, value in enumerate(cpp_list)}
        cpp_sorted = sorted(cpp, key=lambda x: order[x.id])
        for ns, pair in enumerate(cpp_sorted):
            if (pair.component_a.station not in station_to_reject) & (
                pair.component_b.station not in station_to_reject
            ):
                idx.append(ns)
        stacks_list_new = [stacks_list[i] for i in idx]
        stacks_list = stacks_list_new

    n_couples = len(stacks_list)
    logger.info(n_couples)

    if n_couples == 0:
        logger.warning(f"No stacks to export for timespan {stacking_timespan}")
        return filepath

    # Build the global stacks array
    global_stacks = np.array([stack.stack / np.max(np.abs(stack.stack)) for stack in stacks_list], dtype=np.float64)

    # Ensure odd samples and exact zero in time axis => ne fonctionne pas
    time_axis_adjusted, global_stacks_adjusted = ensure_odd_samples_with_zero(
        time_axis=time_axis,
        data=global_stacks,
    )
    idx_time_axis = (np.abs(time_axis_adjusted - 0)).argmin()
    time_axis_adjusted[idx_time_axis] = 0
    n_time_samples = len(time_axis_adjusted)

    # Collect unique components and build mappings
    component_id_to_idx: Dict[int, int] = {}
    unique_components: List[Component] = []

    for stack in stacks_list:
        cpair_id = stack.componentpair_id
        if cpair_id in component_pairs_info:
            comp_a, comp_b = component_pairs_info[cpair_id]
            for comp in [comp_a, comp_b]:
                if comp.id not in component_id_to_idx:
                    component_id_to_idx[comp.id] = len(unique_components)
                    unique_components.append(comp)

    n_stations = len(unique_components)

    # Build id_couples (station pair indices)
    id_couples = np.zeros((2, n_couples), dtype=np.int64)
    position_matrix = np.zeros((2, n_couples), dtype=np.float64)

    for i, stack in enumerate(stacks_list):
        cpair_id = stack.componentpair_id
        if cpair_id in component_pairs_info:
            comp_a, comp_b = component_pairs_info[cpair_id]
            id_couples[0, i] = component_id_to_idx.get(comp_a.id, 0)
            id_couples[1, i] = component_id_to_idx.get(comp_b.id, 0)
            # Separation vector: [xj - xi, yj - yi]
            position_matrix[0, i] = comp_a.x - comp_b.x  # xj - xi
            position_matrix[1, i] = comp_a.y - comp_b.y  # yj - yi
        else:
            logger.warning(f"Component pair info not found for cpair_id {cpair_id}")
            id_couples[0, i] = i
            id_couples[1, i] = i

    # Build loc_matrix (station coordinates: [x; y])
    loc_matrix = np.zeros((2, max(n_stations, 1)), dtype=np.float64)
    for comp in unique_components:
        comp_idx = component_id_to_idx[int(comp.id)]
        loc_matrix[0, comp_idx] = comp.x
        loc_matrix[1, comp_idx] = comp.y

    # Frequency interval
    f_intervals = np.array([[fmin], [fmax]], dtype=np.float64)

    logger.info(f"Exporting {n_couples} stacks ({n_stations} unique stations) to {filepath}")

    with h5py.File(filepath, "w") as hf:
        hf.create_dataset("global_stacks", data=global_stacks_adjusted, dtype="float64")
        hf.create_dataset("time_axis_corr", data=time_axis_adjusted, dtype="float64")
        hf.create_dataset("f_intervals", data=f_intervals, dtype="float64")
        hf.create_dataset("id_couples", data=id_couples, dtype="int64")
        hf.create_dataset("position_matrix", data=position_matrix, dtype="float64")
        hf.create_dataset("loc_matrix", data=loc_matrix, dtype="float64")

        # Add metadata attributes
        hf.attrs["starttime"] = start_str
        hf.attrs["endtime"] = end_str
        hf.attrs["n_couples"] = n_couples
        hf.attrs["n_stations"] = n_stations
        hf.attrs["n_time_samples"] = n_time_samples
        hf.attrs["stacking_timespan_id"] = stacking_timespan.id
        hf.attrs["stacking_schema_id"] = stacking_timespan.stacking_schema_id

    logger.info(f"Successfully exported stacks to {filepath}")

    df_component_id_to_idx = (
        pd.DataFrame.from_dict(component_id_to_idx, orient="index", columns=["idx"])
        .reset_index()
        .rename(columns={"index": "cp_idx"})
    )
    df_component_id_to_idx.to_csv(os.path.join(output_dir, "df_component_id_to_idx.csv"), index=False)
    return filepath
