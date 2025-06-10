# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.
from typing import no_type_check

from functools import lru_cache
from loguru import logger
from matplotlib import pyplot as plt
from obspy.core import AttribDict, Stream
from pathlib import Path
from scipy.optimize import nnls
from scipy.optimize import minimize
from scipy.interpolate import griddata
from scipy.signal import fftconvolve, convolve, correlate
from scipy.ndimage import filters as filters
from itertools import combinations
from scipy import ndimage as ndimage
from typing import Tuple, Collection, Optional, List, Any

import itertools
import math
import numpy as np
import pandas as pd

from noiz.processing.signal_helpers import validate_and_fix_subsample_starttime_error
from noiz.processing.obspy_derived.array_analysis import array_processing, array_transff_freqslowness_wrapper
from noiz.exceptions import ObspyError, SubobjectNotLoadedError, InconsistentDataException
from noiz.models.type_aliases import BeamformingRunnerInputs
from noiz.models import Timespan, Datachunk, Component, BeamformingParams
from noiz.models.beamforming import (
    BeamformingResult,
    BeamformingFile,
    BeamformingPeakAllRelpower,
    BeamformingPeakAllAbspower,
    BeamformingPeakAverageRelpower,
    BeamformingPeakAverageAbspower,
)


def manual_convolve(array1, array2):
    result_full = fftconvolve(array1, array2, mode="full")
    result_same = result_full[
        (array2.shape[0] // 2) : -(array2.shape[0] // 2), (array2.shape[1] // 2) : -(array2.shape[1] // 2)
    ]
    return result_same


def onebit_thresholding(g, threshold=0.5):
    g_demin = g - np.min(g)
    g = 0 * g_demin.copy()
    g[g_demin > (threshold * np.max(g_demin.copy()))] = 1
    return g


def rms_onebit(g1, g2):
    rms = np.sqrt(np.mean((onebit_thresholding(g1) - onebit_thresholding(g2)) ** 2))
    return rms


def rms_l2(g1, g2):
    rms = np.sqrt(np.mean((g1 - g2) ** 2) / np.mean((g1) ** 2))
    return rms


def gaussian_2d_kernel(s, theta, sigma_s, sigma_theta, sx, sy):
    """Generate a 2D Gaussian kernel in slowness space in polar coordinates."""

    sxgrid, sygrid = np.meshgrid(sx, sy)

    # Convert Cartesian coordinates (sx, sy) to polar coordinates (radius, angle)
    S, Theta = np.sqrt(sxgrid**2 + sygrid**2), np.arctan2(sygrid, sxgrid) * 180 / np.pi  # Theta in degrees

    # Apply periodicity to handle wrap-around for azimuthal angle
    delta_theta = np.minimum(np.abs(Theta - theta), 360 - np.abs(Theta - theta))

    # Compute Gaussian kernel in polar coordinates
    gaussian_r = np.exp(-((S - s) ** 2) / (2 * sigma_s**2))
    # gaussian_theta = np.abs(delta_theta) <=  sigma_theta
    gaussian_theta = np.exp(-(delta_theta**2) / (2 * sigma_theta**2))

    if sigma_theta <= 180:
        gaussian_out = gaussian_r * gaussian_theta
    else:
        gaussian_out = gaussian_r

    return gaussian_out


def bool_valid_basis_element(theta, sigma_theta, theta_0, theta_max_dist):
    delta_theta = np.minimum(np.abs(theta - theta_0), 360 - np.abs(theta - theta_0))
    bool_valid = (
        (sigma_theta <= theta_max_dist) and (delta_theta <= theta_max_dist) and ((sigma_theta <= 180) | (theta <= 180))
    )
    return bool_valid


@no_type_check
def construct_complete_basis(sx, sy, s_step_sol, s_bounds, sigma_theta_in, theta_bounds=None):
    """Construct a complete basis with varying azimuthal spreads (sigma_theta) up to 180° in steps of sigma_s."""
    # Generate slowness values within the specified bounds

    if len(s_bounds.shape) == 1:
        nb_s_bounds = 1
    else:
        nb_s_bounds = s_bounds.shape[0]

    if theta_bounds is None:
        theta_bounds = np.tile(np.array([0, 360], nb_s_bounds, 1))

    idx = 0
    slowness_to_basis_indices = {}
    slowness_values = np.unique(np.hstack([np.arange(s_min, s_max, s_step_sol) for s_min, s_max in s_bounds]))
    for i, ((s_min, s_max), (theta_min, theta_max)) in enumerate(zip(s_bounds, theta_bounds)):
        slowness_values_i = np.arange(s_min, s_max, s_step_sol)

        theta_bounds_i = np.array([theta_min, theta_max])
        max_dist_theta = np.abs(np.diff(theta_bounds_i)) / 2
        theta_0 = 0.5 * np.sum(theta_bounds_i)
        theta_0 = 90 - theta_0  # user's input is in geographical convention : convert to trigo
        theta_0 += 180  # user's input is BACK-azimuth
        # max_dist_theta = theta_bounds[1]
        # theta_bounds = theta_bounds % 360 # map to 0-360

        # Calculate sigma_theta values by doubling from sigma_theta_min up to 180°
        sigma_theta_values = []
        sigma_theta = sigma_theta_in
        while sigma_theta <= 360:
            # if sigma_theta <= 180: # avoid adding circular basis element if bounds on BAZ used
            sigma_theta_values.append(sigma_theta)
            sigma_theta *= 2
        sigma_theta_values = np.array(sigma_theta_values)

        count_azim_kernels = 0
        for sigma_theta in sigma_theta_values:
            for theta in np.arange(0, 360, sigma_theta):
                if bool_valid_basis_element(theta, sigma_theta, theta_0, max_dist_theta):
                    # if (sigma_theta <= 180) | (theta<=180):
                    count_azim_kernels += 1

        # Determine the total number of basis elements
        num_basis_elements = len(slowness_values_i) * count_azim_kernels
        basis_new = np.empty((num_basis_elements, len(sy), len(sx)), dtype=np.float64)
        if i == 0:
            basis = basis_new
        else:
            basis = np.concatenate((basis, basis_new), axis=0)

        for s in slowness_values_i:
            azimuth_indices = []
            for sigma_theta in sigma_theta_values:
                for theta in np.arange(0, 360, sigma_theta):  # Discretize azimuths with overlap based on sigma_theta
                    if bool_valid_basis_element(theta, sigma_theta, theta_0, max_dist_theta):
                        # if (sigma_theta <= 180) | (theta<=180):
                        basis[idx, :, :] = gaussian_2d_kernel(s, theta, s_step_sol, sigma_theta, sx, sy)
                        azimuth_indices.append(idx)
                        idx += 1

            slowness_to_basis_indices[s] = np.array(azimuth_indices)

    return basis, slowness_values, slowness_to_basis_indices


def precompute_convolved_basis(basis, arf):
    """Precompute the convolution of each basis element with the array response function (arf)."""
    num_basis_elements = basis.shape[0]
    convolved_basis = np.empty((num_basis_elements, basis.shape[1] * basis.shape[2]), dtype=np.float64)  # Flattened

    for i in range(num_basis_elements):
        convolved_element = manual_convolve(basis[i, :, :], arf)
        convolved_basis[i, :] = convolved_element.flatten()

    return convolved_basis


def select_top_n_sparse_slowness(g, convolved_basis, slowness_values, slowness_to_basis_indices, misfit_cutoff_factor):
    """Select the top n_sparse slowness values based on minimum relative misfit with g."""
    g_flat = g.flatten()
    g_norm = np.linalg.norm(g_flat)

    # Initialize an array to hold the best misfit score for each slowness
    slowness_misfits = np.full(len(slowness_values), np.inf)

    # Calculate relative misfit for each basis element and store the minimum per slowness
    for i, s in enumerate(slowness_values):
        basis_indices = slowness_to_basis_indices[s]

        for idx in basis_indices:
            # Normalize the basis element to avoid amplitude bias
            basis_element = convolved_basis[idx]
            basis_norm = np.linalg.norm(basis_element)
            if basis_norm == 0:
                continue  # Skip elements with zero norm to avoid division by zero

            # Calculate relative misfit for this basis element
            misfit = np.linalg.norm(g_flat / g_norm - basis_element / basis_norm)

            # Update the best (minimum) misfit for this slowness
            slowness_misfits[i] = min(slowness_misfits[i], misfit)

    best_misfit = np.min(slowness_misfits)

    # Get the indices of the top n_sparse lowest misfits
    i_sort_slowness_indices = np.argsort(slowness_misfits)

    # Retrieve the actual slowness values corresponding to the top indices
    top_slowness_values = slowness_values[i_sort_slowness_indices]
    top_slowness_misfits = slowness_misfits[i_sort_slowness_indices]

    top_slowness_indices = i_sort_slowness_indices[top_slowness_misfits < misfit_cutoff_factor * best_misfit]
    top_slowness_values = top_slowness_values[top_slowness_misfits < misfit_cutoff_factor * best_misfit]
    top_slowness_misfits = top_slowness_misfits[top_slowness_misfits < misfit_cutoff_factor * best_misfit]

    return top_slowness_values, top_slowness_indices, top_slowness_misfits


def loss_function(y, augmented_basis, augmented_g):
    # Compute the coefficients as squares to ensure positivity
    sparse_coeffs = y**2
    # Calculate the residual
    residual = augmented_basis.T @ sparse_coeffs - augmented_g
    # Return the sum of squared residuals
    return np.sum(residual**2)


def select_sparse_slowness(
    g,
    convolved_basis,
    slowness_values,
    slowness_to_basis_indices,
    n_sparse,
    misfit_threshold,
    alpha_reg=1e0,
    rel_rms_thresh_admissible_slowness=2,
    rel_rms_stop_crit_increase_sparsity=0.25,
    verbose=True,
    optimization_method="nnls",
):
    """Select an optimal sparse set of slowness values using NNLS, limited by RMS misfit threshold."""
    min_g = np.min(g)
    g_demin = g - min_g
    g_flat = g_demin.flatten()
    selected_indices: np.ndarray[Any, np.dtype[np.int_]] = np.empty(0, dtype=np.int_)
    selected_slowness = []

    # Initialize number of slowness values to consider
    current_sparse = 1

    top_slowness_values, top_slowness_indices, top_slowness_misfits = select_top_n_sparse_slowness(
        g_demin,
        convolved_basis,
        slowness_values,
        slowness_to_basis_indices,
        misfit_cutoff_factor=rel_rms_thresh_admissible_slowness,
    )

    if verbose:
        print("slowness candidates : ")
        print(top_slowness_values)
        print("associated best misfits : ")
        print(top_slowness_misfits)

    # Loop until the misfit is below the threshold or we reach the upper bound n_sparse
    selected_indices_out = None
    sparse_coeffs_out = None
    selected_slowness_print = None

    rms_previous_stage = 1e6
    combination_out_stage: List[int] = []

    df_out = pd.DataFrame(columns=["slowness", "rms_error", "sum_coeffs", "sparsity"])

    while current_sparse <= n_sparse:
        rms_current = 1e6
        combinations_sparse = [
            combination_out_stage.copy() + [i] for i in top_slowness_indices if i not in combination_out_stage
        ]
        # Data collection for the DataFrame
        rms_error_list = []
        coeff_sum_list = []
        slowness_list = []
        if verbose:
            print("sparsity = " + str(current_sparse))
            print("combinations : " + str(len(combinations_sparse)))

        for combination_i in combinations_sparse:
            selected_slowness = [slowness_values[j] for j in combination_i]
            # Gather indices for the selected slowness values, including all their azimuthal segments
            selected_indices = np.hstack([slowness_to_basis_indices[s] for s in selected_slowness])

            # Re-run NNLS on the selected subset of the convolved basis
            sparse_convolved_basis = convolved_basis[selected_indices]

            reg_coef = alpha_reg * np.max(sparse_convolved_basis)
            augmented_basis = np.concatenate(
                (sparse_convolved_basis, reg_coef * np.eye(sparse_convolved_basis.shape[0])), axis=1
            )
            augmented_g = np.concatenate((g_flat, np.zeros(sparse_convolved_basis.shape[0])), axis=0)

            if optimization_method == "nnls":
                # sparse_coeffs, _ = nnls(sparse_convolved_basis.T, g_flat)
                sparse_coeffs, _ = nnls(augmented_basis.T, augmented_g)

            elif optimization_method == "L-BFGS-B":
                # Initial guess for y (unconstrained variables)
                y_initial = np.zeros(augmented_basis.shape[0])

                # Optimize using the L-BFGS-B method
                result = minimize(loss_function, y_initial, args=(augmented_basis, augmented_g), method="L-BFGS-B")

                # Recover the non-negative coefficients
                sparse_coeffs = result.x**2

            # Calculate RMS error for current selection
            g_reconstructed = np.sum(
                [sparse_coeffs[i] * convolved_basis[idx] for i, idx in enumerate(selected_indices)], axis=0
            )
            g_reconstructed = np.reshape(g_reconstructed, g.shape)
            g_reconstructed += min_g

            # rms_error = np.sqrt(np.mean((g - g_reconstructed) ** 2)) / np.sqrt(np.mean(g ** 2))
            rms_error = rms_l2(g, g_reconstructed)
            # rms_error = rms_onebit(g, g_reconstructed)

            if rms_error < rms_current:
                rms_current = rms_error
                combination_out_stage = combination_i
                selected_indices_out_stage = selected_indices
                sparse_coeffs_out_stage = sparse_coeffs
                selected_slowness_print_stage = selected_slowness

            # Record slowness-specific information for the DataFrame
            for slowness in selected_slowness:
                # Find all indices in `selected_indices` corresponding to this slowness
                slowness_indices = slowness_to_basis_indices[slowness]

                # Sum the coefficients for this slowness
                slowness_coeff_sum = np.sum(
                    [
                        sparse_coeffs[np.where(selected_indices == idx)[0][0]]
                        for idx in slowness_indices
                        if idx in selected_indices
                    ]
                )

                rms_error_list.append(rms_error)
                coeff_sum_list.append(slowness_coeff_sum)
                slowness_list.append(slowness)
        # Create the DataFrame with all entries, then group by slowness to select minimal rms_error
        df_stage = pd.DataFrame({"slowness": slowness_list, "rms_error": rms_error_list, "sum_coeffs": coeff_sum_list})
        df_stage["sparsity"] = current_sparse
        # Group by slowness, selecting the row with the minimal rms_error
        df_stage = df_stage.loc[df_stage.groupby("slowness")["rms_error"].idxmin()]
        df_out = pd.concat([df_out, df_stage])

        print(selected_slowness_print_stage)
        print("RMS new " + str(rms_current))

        if (rms_previous_stage - rms_current) / rms_previous_stage < rel_rms_stop_crit_increase_sparsity:
            if verbose:
                print(
                    "stopping sparsity increase as the cost does not decrease more than by factor "
                    + str(rel_rms_stop_crit_increase_sparsity)
                )
            break
        else:
            selected_indices_out = selected_indices_out_stage
            sparse_coeffs_out = sparse_coeffs_out_stage
            selected_slowness_print = selected_slowness_print_stage
            rms_previous_stage = rms_current
            current_sparse += 1  # Otherwise, increase the number of slowness values

        # Check if the RMS error is under the stop threshold
        if rms_current <= misfit_threshold:
            if verbose:
                print("stopping iterations as the cost below threshold " + str(misfit_threshold))
            break  # Stop if the misfit is below the threshold

    if verbose:
        print(selected_slowness_print)
        print("RMS = " + str(rms_previous_stage))

    return selected_indices_out, sparse_coeffs_out, df_out


def get_next_file_number(directory):
    max_num = 0
    for file in directory.glob("convolved_basis_*.npz"):
        # Extract the number from the filename
        file_num = int(file.stem.split("_")[-1])
        max_num = max(max_num, file_num)
    return max_num + 1


def deconv_by_sparse_decomposition(
    g,
    arf,
    sx,
    sy,
    s_step_sol,
    s_bounds,
    sigma_theta,
    n_sparse,
    misfit_threshold,
    theta_bounds=None,
    alpha_reg=1e0,
    rel_rms_thresh_admissible_slowness=2,
    rel_rms_stop_crit_increase_sparsity=0.25,
    verbose=False,
    path_basis=None,
    path_convolved_basis=None,
):
    """Main function to solve the deconvolution problem with sparsity constraint on slowness values."""
    # Construct full basis with all possible slowness and azimuthal segments

    if verbose:
        print("constructing basis")
    if path_basis is None:
        basis, slowness_values, slowness_to_basis_indices = construct_complete_basis(
            sx, sy, s_step_sol, s_bounds, sigma_theta, theta_bounds
        )
    else:
        if path_basis.exists():
            basis_holder = np.load(path_basis, allow_pickle=True)
            basis = basis_holder["basis"]
            slowness_values = basis_holder["slowness_values"]
            slowness_to_basis_indices = basis_holder["slowness_to_basis_indices"].item()
        else:
            basis, slowness_values, slowness_to_basis_indices = construct_complete_basis(
                sx, sy, s_step_sol, s_bounds, sigma_theta, theta_bounds
            )
            np.savez(
                path_basis,
                basis=basis,
                slowness_values=slowness_values,
                slowness_to_basis_indices=slowness_to_basis_indices,
            )

    # Precompute the convolved basis
    if verbose:
        print("convolving basis")
    if path_convolved_basis is None:
        convolved_basis = precompute_convolved_basis(basis, arf)
    else:
        basis_files = list(path_convolved_basis.glob("**/convolved_basis*.npz"))
        found_basis = False
        for file in basis_files:
            convolved_basis_holder = np.load(file, allow_pickle=True)
            convolved_basis_i = convolved_basis_holder["convolved_basis"]
            arf_i = convolved_basis_holder["arf"]
            if np.sum((arf_i - arf) ** 2) / np.sum(arf**2) < 1e-6:
                found_basis = True
                convolved_basis = convolved_basis_i
                break
        if not found_basis:
            convolved_basis = precompute_convolved_basis(basis, arf)
            next_file_num = get_next_file_number(path_convolved_basis)
            new_file_name = path_convolved_basis / f"convolved_basis_{next_file_num}.npz"
            np.savez(new_file_name, arf=arf, convolved_basis=convolved_basis)

    # Select sparse basis (n_sparse distinct slowness values) and solve for coefficients
    if verbose:
        print("selecting optimal sparse basis")
    selected_indices, sparse_coeffs, df_all_solutions = select_sparse_slowness(
        g,
        convolved_basis,
        slowness_values,
        slowness_to_basis_indices,
        n_sparse,
        misfit_threshold,
        alpha_reg,
        rel_rms_thresh_admissible_slowness,
        rel_rms_stop_crit_increase_sparsity,
    )

    # Reconstruct f from the selected sparse basis
    if verbose:
        print("preparing outputs")
    f = np.sum([sparse_coeffs[i] * basis[idx] for i, idx in enumerate(selected_indices)], axis=0)

    # Reconstruct g using the optimized f
    g_reconstructed = fftconvolve(f, arf, mode="same")
    g_reconstructed += np.min(g)

    # Calculate RMS error
    # rms_error = np.sqrt(np.mean((g - g_reconstructed) ** 2)) / np.sqrt(np.mean(g ** 2))
    rms_error = rms_l2(g, g_reconstructed)
    # rms_error = rms_onebit(g, g_reconstructed)

    return f, g_reconstructed, rms_error, df_all_solutions


def deconvolve_beamformers(arf, beam, beamforming_params):
    sparsity_max = beamforming_params.sparsity_max  # km/s
    sigma_angle_kernels = beamforming_params.sigma_angle_kernels  # iterations
    sigma_slowness_kernels_ratio_to_ds = beamforming_params.sigma_slowness_kernels_ratio_to_ds  # degrees
    rms_threshold_deconv = beamforming_params.rms_threshold_deconv  # degrees
    sx = beamforming_params.get_xaxis()
    sy = beamforming_params.get_yaxis()

    if beamforming_params.smin1 is not None:
        s_bounds = [[beamforming_params.smin1, beamforming_params.smax1]]
        if beamforming_params.smin2 is not None:
            s_bounds.append([beamforming_params.smin2, beamforming_params.smax2])
    else:
        s_bounds = [[0, min(beamforming_params.slowness_x_max, beamforming_params.slowness_y_max)]]

    if beamforming_params.thetamin1 is not None:
        theta_bounds = [[beamforming_params.thetamin1, beamforming_params.thetamax1]]
        if beamforming_params.thetamin2 is not None:
            theta_bounds.append([beamforming_params.thetamin2, beamforming_params.thetamax2])
    else:
        theta_bounds = [[0, 360]]

    s_step_sol = sigma_slowness_kernels_ratio_to_ds * beamforming_params.slowness_step

    path_tmp_beamforming_deconv_root = Path("/processed-data-dir/tmp_beamforming_deconv")
    path_tmp_beamforming_deconv_root.mkdir(exist_ok=True)
    path_tmp_beamforming_deconv_param_id = path_tmp_beamforming_deconv_root.joinpath(str(beamforming_params.id))
    path_tmp_beamforming_deconv_param_id.mkdir(exist_ok=True)
    path_basis = path_tmp_beamforming_deconv_param_id.joinpath("basis.npz")
    path_convolved_basis = path_tmp_beamforming_deconv_param_id

    beam_deconv, g_est, rms_error, df_all_solutions = deconv_by_sparse_decomposition(
        g=beam,
        arf=arf,
        sx=sx,
        sy=sy,
        s_step_sol=s_step_sol,
        s_bounds=np.array(s_bounds),
        sigma_theta=sigma_angle_kernels,
        n_sparse=sparsity_max,
        theta_bounds=np.array(theta_bounds),
        misfit_threshold=rms_threshold_deconv,
        alpha_reg=beamforming_params.reg_coef_deconv,
        rel_rms_thresh_admissible_slowness=beamforming_params.rel_rms_thresh_admissible_slowness,
        rel_rms_stop_crit_increase_sparsity=beamforming_params.rel_rms_stop_crit_increase_sparsity,
        path_basis=path_basis,
        path_convolved_basis=path_convolved_basis,
    )

    return beam_deconv, g_est, rms_error, df_all_solutions


###########################################################################################


def calculate_beamforming_results_wrapper(inputs: BeamformingRunnerInputs) -> Tuple[BeamformingResult, ...]:
    """filldocs"""
    return tuple(
        calculate_beamforming_results(
            beamforming_params_collection=inputs["beamforming_params"],
            timespan=inputs["timespan"],
            datachunks=inputs["datachunks"],
        ),
    )


def calculate_beamforming_results(
    beamforming_params_collection: Collection[BeamformingParams],
    timespan: Timespan,
    datachunks: Tuple[Datachunk, ...],
) -> List[BeamformingResult]:
    """filldocs"""

    logger.debug(f"Loading seismic files for timespan {timespan}")

    st = Stream()

    for datachunk in datachunks:
        if not isinstance(datachunk.component, Component):
            raise SubobjectNotLoadedError("You should load Component together with the Datachunk.")
        single_st = datachunk.load_data()
        single_st[0].stats.coordinates = AttribDict(
            {
                "latitude": datachunk.component.lat,
                "elevation": datachunk.component.elevation / 1000,
                "longitude": datachunk.component.lon,
            }
        )
        st.extend(single_st)

    logger.info(f"For Timespan {timespan} there are {len(st)} traces loaded.")

    logger.debug("Checking for subsample starttime error.")
    st = validate_and_fix_subsample_starttime_error(st)

    logger.debug(f"Preparing stream metadata for beamforming for timespan {timespan}")
    first_starttime = min([tr.stats.starttime for tr in st])
    first_endtime = min([tr.stats.endtime for tr in st])
    time_vector = [pd.Timestamp.utcfromtimestamp(x).to_datetime64() for x in st[0].times("timestamp")]

    results = []

    for beamforming_params in beamforming_params_collection:
        logger.debug(f"Calculating beamforming for timespan {timespan} and params {beamforming_params}")

        logger.debug("Creating an empty BeamformingResult")
        res = BeamformingResult(timespan_id=timespan.id, beamforming_params_id=beamforming_params.id)

        if len(st) < beamforming_params.minimum_trace_count:
            logger.error(
                f"There are not enough data for beamforming. "
                f"Minimum trace count: {beamforming_params.minimum_trace_count} "
                f"Got: {len(st)}"
            )
            continue

        bk = BeamformerKeeper(
            starttime=timespan.starttime_np,
            midtime=timespan.midtime_np,
            endtime=timespan.endtime_np,
            xaxis=beamforming_params.get_xaxis(),
            yaxis=beamforming_params.get_yaxis(),
            time_vector=time_vector,
            save_relpow=beamforming_params.save_relpow,
            save_abspow=beamforming_params.save_abspow,
            save_arf=beamforming_params.save_all_arf,
        )

        array_proc_kwargs = {
            # slowness grid: X min, X max, Y min, Y max, Slow Step
            "sll_x": beamforming_params.slowness_x_min,
            "slm_x": beamforming_params.slowness_x_max,
            "sll_y": beamforming_params.slowness_y_min,
            "slm_y": beamforming_params.slowness_y_max,
            "sl_s": beamforming_params.slowness_step,
            # sliding window properties
            "win_len": beamforming_params.window_length,
            "win_frac": beamforming_params.window_fraction,
            # frequency properties
            "frqlow": beamforming_params.min_freq,
            "frqhigh": beamforming_params.max_freq,
            "prewhiten": int(beamforming_params.prewhiten),
            # restrict output
            "semb_thres": beamforming_params.semblance_threshold,
            "vel_thres": beamforming_params.velocity_threshold,
            "timestamp": "julsec",
            "stime": first_starttime,
            "etime": first_endtime,
            "method": beamforming_params.method,
            "save_arf": beamforming_params.save_all_arf,
            "n_sigma_stat_reject": beamforming_params.n_sigma_stat_reject,
            "prop_bad_freqs_stat_reject": beamforming_params.prop_bad_freqs_stat_reject,
            "nsta_min_keep_stat_reject": beamforming_params.minimum_trace_count,
            "perform_statistical_reject": beamforming_params.perform_statistical_reject,
            "store": bk.save_beamformers,
        }

        # if beamforming_params.perform_deconvolution_all or \
        # beamforming_params.perform_deconvolution_average:
        arf_enlargement_ratio = beamforming_params.arf_enlarge_ratio
        array_proc_kwargs["sll_x_arf"] = arf_enlargement_ratio * array_proc_kwargs["sll_x"]
        array_proc_kwargs["slm_x_arf"] = arf_enlargement_ratio * array_proc_kwargs["slm_x"]
        array_proc_kwargs["sll_y_arf"] = arf_enlargement_ratio * array_proc_kwargs["sll_y"]
        array_proc_kwargs["slm_y_arf"] = arf_enlargement_ratio * array_proc_kwargs["slm_y"]
        # print('done with array_proc_kwargs filling')
        ###### CK #####

        # if beamforming_params.perform_deconvolution_all:
        #     arf_proc_kwargs = array_proc_kwargs.copy()
        #     arf_proc_kwargs['store'] = bk.save_arf_and_deconv
        ##
        # array_deconv_proc_kwargs = array_proc_kwargs.copy()
        # array_deconv_proc_kwargs['store'] = bk.save_beamformers_deconv
        ##
        ###############
        # logger.info('Entering try')
        try:
            ###### CK #####
            # for (i,tr) in enumerate(st):
            #    tr.data = tr.data/np.median(np.abs(tr.data))
            #    st[i] = tr
            ###########

            # print('Calculating beam')
            _ = array_processing(st, **array_proc_kwargs)
            # _ = array_transff_freqslowness_wrapper(st, **array_proc_kwargs)

            ##### AKA 18/07/2023 ######
            # print('Calculating avg abs pow beamformer')
            if beamforming_params.save_abspow:
                # logger.info('Started calculating avg abs pow beamformer')
                bk.calculate_average_abspower_beamformer()
                # logger.info('Finished calculating avg abs pow beamformer')

            # print('Calculating avg rel pow beamformer')
            if beamforming_params.save_relpow:
                # logger.info('Started calculating avg rel pow beamformer')
                bk.calculate_average_relpower_beamformer()
                # logger.info('Finished calculating avg rel pow beamformer')

            if beamforming_params.save_average_arf:
                if beamforming_params.save_all_arf:
                    # this means that elementary ARF are available
                    # logger.info('Started calculate_average_arf_beamformer')
                    bk.calculate_average_arf_beamformer()
                    # logger.info('Finished calculate_average_arf_beamformer')
                else:
                    # this means that we compute a global arf based on the array geometry
                    # logger.info('Started array_transff_freqslowness_wrapper')
                    avg_arf = array_transff_freqslowness_wrapper(st, array_proc_kwargs)
                    bk.average_arf = avg_arf
                    # logger.info('Finished array_transff_freqslowness_wrapper')
            ###########################

            if beamforming_params.perform_deconvolution_all:
                if not beamforming_params.save_all_arf:
                    logger.info(
                        "No deconvolution performed for elementary windows! Activate beamforming_params.save_all_arf"
                    )
                else:
                    ###### CK #####
                    # tr_0 = st[0]
                    # st_arf = st.copy()
                    # for (i,tr) in enumerate(st):
                    #     st_arf[i].data = tr_0.data

                    # print('Calculating ARF')
                    # _ = array_processing(st_arf, **arf_proc_kwargs)

                    ##### AKA 18/07/2023 ######
                    bk.deconv_all_windows_from_existing_arf(beamforming_params)
                    ##### CK #####
            if beamforming_params.perform_deconvolution_average:
                if not beamforming_params.save_average_arf:
                    logger.info(
                        "No deconvolution performed for average beams ! Activate beamforming_params.save_average_arf"
                    )
                else:
                    # print('Calculating avg arf beamformer')
                    # bk.calculate_average_arf_beamformer()
                    # print('deconvolve avg abspow with arf')
                    if beamforming_params.save_abspow:
                        bk.deconvolve_average_abspower_with_arf(beamforming_params)
                    # print('deconvolve avg relpow with arf')
                    if beamforming_params.save_relpow:
                        bk.deconvolve_average_relpower_with_arf(beamforming_params)
                ##############
                ###########################

        # except ValueError as e:
        except ValueError as e:
            raise ObspyError(
                f"Ecountered error while running beamforming routine. "
                f"Error happenned for timespan: {timespan}, beamform_params: {beamforming_params} "
                f"Error was: {e}"
            ) from e

        if beamforming_params.extract_peaks_average_beamformer_abspower:
            res.average_abspower_peaks = bk.get_average_abspower_peaks(
                neighborhood_size=beamforming_params.neighborhood_size,
                maxima_threshold=beamforming_params.maxima_threshold,
                best_point_count=beamforming_params.best_point_count,
                beam_portion_threshold=beamforming_params.beam_portion_threshold,
                bool_use_deconv=beamforming_params.perform_deconvolution_average,
            )
        if beamforming_params.extract_peaks_average_beamformer_relpower:
            res.average_relpower_peaks = bk.get_average_relpower_peaks(
                neighborhood_size=beamforming_params.neighborhood_size,
                maxima_threshold=beamforming_params.maxima_threshold,
                best_point_count=beamforming_params.best_point_count,
                beam_portion_threshold=beamforming_params.beam_portion_threshold,
                bool_use_deconv=beamforming_params.perform_deconvolution_average,
            )
        if beamforming_params.extract_peaks_all_beamformers_abspower:
            res.all_abspower_peaks = bk.get_all_abspower_peaks(
                neighborhood_size=beamforming_params.neighborhood_size,
                maxima_threshold=beamforming_params.maxima_threshold,
                best_point_count=beamforming_params.best_point_count,
                beam_portion_threshold=beamforming_params.beam_portion_threshold,
                bool_use_deconv=beamforming_params.perform_deconvolution_all,
            )
        if beamforming_params.extract_peaks_all_beamformers_relpower:
            res.all_relpower_peaks = bk.get_all_relpower_peaks(
                neighborhood_size=beamforming_params.neighborhood_size,
                maxima_threshold=beamforming_params.maxima_threshold,
                best_point_count=beamforming_params.best_point_count,
                beam_portion_threshold=beamforming_params.beam_portion_threshold,
                bool_use_deconv=beamforming_params.perform_deconvolution_all,
            )

        beamforming_file = bk.save_beamforming_file(params=beamforming_params, ts=timespan)
        if beamforming_file is not None:
            res.file = beamforming_file

        res.used_component_count = len(st)
        res.datachunks = list(datachunks)

        results.append(res)

    return results


class BeamformerKeeper:
    """filldocs"""

    def __init__(
        self,
        starttime: np.datetime64,
        midtime: np.datetime64,
        endtime: np.datetime64,
        xaxis,  #: npt.ArrayLike,
        yaxis,  #: npt.ArrayLike,
        time_vector,  #: npt.ArrayLike,
        save_relpow: bool = False,
        save_abspow: bool = True,
        save_arf: bool = True,
    ):
        self.starttime: np.datetime64 = starttime
        self.midtime: np.datetime64 = midtime
        self.endtime: np.datetime64 = endtime
        self.xaxis = xaxis
        # self.xaxis: npt.ArrayLike = xaxis
        self.yaxis = yaxis
        # self.yaxis: npt.ArrayLike = yaxis
        self.save_relpow: bool = save_relpow
        self.save_abspow: bool = save_abspow
        self.save_arf: bool = save_arf

        self.rel_pows: List[Any] = []
        # self.rel_pows: List[npt.ArrayLike] = []
        self.abs_pows: List[Any] = []
        # self.abs_pows: List[npt.ArrayLike] = []
        self.midtime_samples: List[int] = []

        ###### CK #######
        # Check if bool save_arf_and_deconv necessary in init

        self.arf: List[int] = []
        self.abs_pows_deconv: List[int] = []
        self.rel_pows_deconv: List[int] = []
        self.abs_pows_reconstructed: List[int] = []
        self.rel_pows_reconstructed: List[int] = []
        self.rms_error_deconv: List[int] = []
        self.all_solutions_deconv: List[int] = []

        self.average_arf: Any = None
        self.average_abspow_deconv: Any = None
        self.average_relpow_deconv: Any = None
        self.average_abspow_reconstructed: Any = None
        self.average_relpow_reconstructed: Any = None
        self.average_abspow_rms_error_deconv: Any = None
        self.average_relpow_rms_error_deconv: Any = None
        self.average_abspow_all_sols_deconv: Any = None
        self.average_relpow_all_sols_deconv: Any = None
        # self.average_abspow_deconv_2: Any = None
        ##
        #################

        self.iteration_count: int = 0
        self.time_vector = time_vector
        # self.time_vector: npt.ArrayLike = time_vector

        self.average_relpow: Any = None
        # self.average_relpow: Optional[npt.ArrayLike] = None
        self.average_abspow: Any = None

        #################
        # self.average_abspow: Optional[npt.ArrayLike] = None

    def save_beamforming_file(self, params: BeamformingParams, ts: Timespan) -> Optional[BeamformingFile]:
        bf = BeamformingFile()
        fpath = bf.find_empty_filepath(ts=ts, params=params)

        res_to_save = {}

        if params.save_all_beamformers_abspower:
            for i, arr in enumerate(self.abs_pows):
                res_to_save[f"abs_pow_{i}"] = arr
        if params.save_all_beamformers_relpower:
            for i, arr in enumerate(self.rel_pows):
                res_to_save[f"rel_pow_{i}"] = arr
        if params.save_average_beamformer_abspower:
            res_to_save["avg_abs_pow"] = self.average_abspow
        if params.save_average_beamformer_relpower:
            res_to_save["avg_rel_pow"] = self.average_relpow
        ####### modif AKA 19/07/2023 --> arf systematically computed in obspy
        if params.save_all_arf:
            for i, arr in enumerate(self.arf):
                res_to_save[f"arf_{i}"] = arr
        if params.save_average_arf:
            res_to_save["avg_arf"] = self.average_arf

        if params.perform_deconvolution_all:
            ######## CK ####### modif AKA 19/07/2023
            if params.save_all_beamformers_abspower:
                for i, (arr, arr_reconstructed, rms_error_deconv, all_solutions_deconv) in enumerate(
                    zip(
                        self.abs_pows_deconv,
                        self.abs_pows_reconstructed,
                        self.rms_error_deconv,
                        self.all_solutions_deconv,
                    )
                ):
                    res_to_save[f"abs_pow_deconv_{i}"] = arr
                    res_to_save[f"abs_pow_reconstructed_{i}"] = arr_reconstructed
                    res_to_save[f"rms_error_deconv_{i}"] = rms_error_deconv
                    res_to_save[f"all_solutons_deconv_{i}"] = all_solutions_deconv

            if params.save_all_beamformers_relpower:
                for i, (arr, arr_reconstructed, rms_error_deconv, all_solutions_deconv) in enumerate(
                    zip(
                        self.rel_pows_deconv,
                        self.rel_pows_reconstructed,
                        self.rms_error_deconv,
                        self.all_solutions_deconv,
                    )
                ):
                    res_to_save[f"rel_pow_deconv_{i}"] = arr
                    res_to_save[f"rel_pow_reconstructed_{i}"] = arr_reconstructed
                    res_to_save[f"rms_error_deconv_{i}"] = rms_error_deconv
                    res_to_save[f"all_solutons_deconv_{i}"] = all_solutions_deconv

        if params.perform_deconvolution_average:
            if params.save_average_beamformer_abspower:
                res_to_save["avg_abs_pow_deconv"] = self.average_abspow_deconv
                res_to_save["avg_abs_pow_reconstructed"] = self.average_abspow_reconstructed
                res_to_save["avg_abs_pow_rms_error_deconv"] = self.average_abspow_rms_error_deconv
                res_to_save["average_abspow_all_sols_deconv"] = self.average_abspow_all_sols_deconv
            if params.save_average_beamformer_relpower:
                res_to_save["avg_rel_pow_deconv"] = self.average_relpow_deconv
                res_to_save["avg_rel_pow_reconstructed"] = self.average_relpow_reconstructed
                res_to_save["avg_rel_pow_rms_error_deconv"] = self.average_relpow_rms_error_deconv
                res_to_save["average_relpow_all_sols_deconv"] = self.average_relpow_all_sols_deconv
            ##
            # res_to_save["avg_abs_pow_deconv"] = self.average_abspow_deconv
            # #_2 res_to_save["avg_abs_pow_deconv_2"] = self.average_abspow_deconv_2
        ##
        ###################

        if len(res_to_save) > 0:
            logger.info(f"File will be saved at {fpath}")
            res_to_save["file"] = fpath
            res_to_save["midtimes"] = self.get_midtimes()
            np.savez_compressed(**res_to_save)
            return bf
        return None

    @lru_cache  # noqa: B019
    def get_midtimes(self):  # -> npt.ArrayLike:
        """filldocs"""
        return np.array([self.time_vector[x] for x in self.midtime_samples])

    # def save_beamformers(self, pow_map: npt.ArrayLike, apow_map: npt.ArrayLike, midsample: int) -> None:
    def save_beamformers(self, pow_map, apow_map, midsample: int, arf=None) -> None:
        """
        filldocs

        """
        self.iteration_count += 1

        pow_map = pow_map.T
        apow_map = apow_map.T

        self.midtime_samples.append(midsample)
        if self.save_relpow:
            self.rel_pows.append(pow_map.copy())
        if self.save_abspow:
            self.abs_pows.append(apow_map.copy())
        # print(self.save_arf)
        if (self.save_arf) and (arf is not None):
            # arf = arf.T # major bugfix AKA 19/07/2024
            print("I am not transposing !!")
            self.arf.append(arf.copy())

    def deconv_all_windows_from_existing_arf(self, beamforming_params):
        for _i, (abs_pow, rel_pow, arf) in enumerate(zip(self.abs_pows, self.rel_pows, self.arf)):
            abs_pow_deconv, abs_pow_reconstructed, rms_error_deconv, all_solutions_deconv = deconvolve_beamformers(
                arf, abs_pow, beamforming_params
            )
            abs_pow_deconv_max = np.max(abs_pow_deconv.flatten())
            rel_pows_max = np.max(rel_pow.flatten())
            rel_pow_deconv = (rel_pows_max / abs_pow_deconv_max) * abs_pow_deconv
            rel_pow_reconstructed = (rel_pows_max / abs_pow_deconv_max) * abs_pow_reconstructed
            self.abs_pows_deconv.append(abs_pow_deconv)
            self.rel_pows_deconv.append(rel_pow_deconv)
            self.abs_pows_reconstructed.append(abs_pow_reconstructed)
            self.rel_pows_reconstructed.append(rel_pow_reconstructed)
            self.rms_error_deconv.append(rms_error_deconv)
            self.all_solutions_deconv.append(all_solutions_deconv)

    def calculate_average_arf_beamformer(self):
        """filldocs"""
        if len(self.arf) == 0:
            raise ValueError(
                "There are no data to average in arf . "
                "Are you sure you used `save_beamformers` method to keep data from beamforming procedure"
            )
        self.average_arf = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.arf:
            self.average_arf = np.add(self.average_arf, arr)
        self.average_arf = self.average_arf / self.iteration_count

    def deconvolve_average_abspower_with_arf(self, beamforming_params):
        """filldocs"""
        (
            self.average_abspow_deconv,
            self.average_abspow_reconstructed,
            self.average_abspow_rms_error_deconv,
            self.average_abspow_all_sols_deconv,
        ) = deconvolve_beamformers(self.average_arf, self.average_abspow, beamforming_params)

    ####################################

    def deconvolve_average_relpower_with_arf(self, beamforming_params):
        """filldocs"""
        (
            self.average_relpow_deconv,
            self.average_relpow_reconstructed,
            self.average_relpow_rms_error_deconv,
            self.average_relpow_all_sols_deconv,
        ) = deconvolve_beamformers(self.average_arf, self.average_relpow, beamforming_params)

    ####################################

    def calculate_average_relpower_beamformer(self):
        """filldocs"""
        if self.save_relpow is not True:
            raise ValueError("The `save_relpow` was set to False, data were not kept")
        if len(self.rel_pows) == 0:
            raise ValueError(
                "There are no data to average. "
                "Are you sure you used `save_beamformers` method to keep data from beamforming procedure"
            )
        self.average_relpow = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.rel_pows:
            self.average_relpow = np.add(self.average_relpow, arr)
        self.average_relpow = self.average_relpow / self.iteration_count

    def calculate_average_abspower_beamformer(self):
        """filldocs"""
        if self.save_abspow is not True:
            raise ValueError("The `save_abspow` was set to False, data were not kept")
        if len(self.abs_pows) == 0:
            raise ValueError(
                "There are no data to average. "
                "Are you sure you used `save_beamformers` method to keep data from beamforming procedure"
            )

        self.average_abspow = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.abs_pows:
            self.average_abspow = np.add(self.average_abspow, arr)
        self.average_abspow = self.average_abspow / self.iteration_count

    def extract_best_maxima_from_average_relpower(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        use_deconv=False,
    ):
        """filldocs"""

        if use_deconv:
            data_use = self.average_relpow_deconv
        else:
            data_use = self.average_relpow

        maxima = select_local_maxima(
            data=data_use,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=self.midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        df = _extract_most_significant_subbeams(
            [
                maxima,
            ],
            beam_portion_threshold,
        )
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        if len(df) == 0:
            plt.pcolormesh(self.xaxis, self.yaxis, data_use)
            plt.savefig("/processed-data-dir/tmp_beamforming/" + str(self.midtime) + ".png")

        return df

    def extract_best_maxima_from_average_abspower(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        use_deconv=False,
    ):
        """filldocs"""

        if use_deconv:
            data_use = self.average_abspow_deconv
        else:
            data_use = self.average_abspow

        maxima = select_local_maxima(
            data=data_use,
            xaxis=self.xaxis,
            yaxis=self.yaxis,
            time=self.midtime,
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
        )

        df = _extract_most_significant_subbeams(
            [
                maxima,
            ],
            beam_portion_threshold,
        )
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def extract_best_maxima_from_all_relpower(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        use_deconv=False,
    ):
        """filldocs"""

        if use_deconv:
            data_use = self.rel_pows_deconv
        else:
            data_use = self.rel_pows

        all_maxima = []

        for midtime, single_beamformer in zip(self.get_midtimes(), data_use):
            maxima = select_local_maxima(
                data=single_beamformer,
                xaxis=self.xaxis,
                yaxis=self.yaxis,
                time=midtime,
                neighborhood_size=neighborhood_size,
                maxima_threshold=maxima_threshold,
                best_point_count=best_point_count,
            )
            all_maxima.append(maxima)

        df = _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def extract_best_maxima_from_all_abspower(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        use_deconv=True,
    ):
        """filldocs"""

        if use_deconv:
            data_use = self.abs_pows_deconv
        else:
            data_use = self.abs_pows

        all_maxima = []

        for midtime, single_beamformer in zip(self.get_midtimes(), data_use):
            maxima = select_local_maxima(
                data=single_beamformer,
                xaxis=self.xaxis,
                yaxis=self.yaxis,
                time=midtime,
                neighborhood_size=neighborhood_size,
                maxima_threshold=maxima_threshold,
                best_point_count=best_point_count,
            )
            all_maxima.append(maxima)

        df = _extract_most_significant_subbeams(all_maxima, beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def get_average_abspower_peaks(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        bool_use_deconv: bool,
    ) -> List[BeamformingPeakAverageAbspower]:
        df = self.extract_best_maxima_from_average_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv,
        )
        # print('Calculating avg abs pow peaks')
        res = []
        for _i, row in df.iterrows():
            res.append(
                BeamformingPeakAverageAbspower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.avg_amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return res

    def get_average_relpower_peaks(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        bool_use_deconv: bool,
    ) -> List[BeamformingPeakAverageRelpower]:
        df = self.extract_best_maxima_from_average_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv,
        )
        res = []
        for _i, row in df.iterrows():
            res.append(
                BeamformingPeakAverageRelpower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.avg_amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return res

    def get_all_abspower_peaks(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        bool_use_deconv: bool,
    ) -> List[BeamformingPeakAllAbspower]:
        df = self.extract_best_maxima_from_all_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv,
        )
        res = []
        for _i, row in df.iterrows():
            res.append(
                BeamformingPeakAllAbspower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.avg_amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return res

    def get_all_relpower_peaks(
        self,
        neighborhood_size: int,
        maxima_threshold: float,
        best_point_count: int,
        beam_portion_threshold: float,
        bool_use_deconv: bool,
    ) -> List[BeamformingPeakAllRelpower]:
        df = self.extract_best_maxima_from_all_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv,
        )
        res = []
        for _i, row in df.iterrows():
            res.append(
                BeamformingPeakAllRelpower(
                    slowness=row.slowness,
                    slowness_x=row.x,
                    slowness_y=row.y,
                    amplitude=row.avg_amplitude,
                    azimuth=row.azimuth,
                    backazimuth=row.backazimuth,
                )
            )
        return res


def _extract_most_significant_subbeams(
    all_maxima: Collection[pd.DataFrame],
    beam_portion_threshold: float,
) -> pd.DataFrame:
    """filldocs"""

    df_all = pd.concat(all_maxima).set_index("midtime")
    total_beam = df_all.loc[:, "amplitude"].groupby(level=0).sum()
    df_all.loc[:, "beam_proportion"] = df_all.apply(lambda row: row.amplitude / total_beam.loc[row.name], axis=1)
    while df_all["beam_proportion"].max() <= beam_portion_threshold:
        beam_portion_threshold /= 2
    df_res = df_all.loc[df_all.loc[:, "beam_proportion"] > beam_portion_threshold, :]
    maximum_points = df_res.groupby(by=["x", "y"]).mean()
    maximum_points["occurence_counts"] = df_res.groupby(by=["x", "y"])["amplitude"].count()
    maximum_points = maximum_points.rename(
        columns={"amplitude": "avg_amplitude", "beam_proportion": "avg_beam_proportion"}
    ).reset_index(level=[0, 1])

    return maximum_points


def select_local_maxima(
    data,  #: npt.ArrayLike,
    xaxis,  #: npt.ArrayLike,
    yaxis,  #: npt.ArrayLike,
    time: Any,
    neighborhood_size: int,
    maxima_threshold: float,
    best_point_count: int,
) -> pd.DataFrame:
    data = data
    data_max = filters.maximum_filter(data, neighborhood_size)
    maxima = data == data_max
    data_min = filters.minimum_filter(data, neighborhood_size)
    diff = (data_max - data_min) > maxima_threshold
    maxima[diff == 0] = 0
    labeled, num_objects = ndimage.label(maxima)
    slices = ndimage.find_objects(labeled)
    x, y, max_vals = [], [], []
    for dy, dx in slices:
        x_center = int((dx.start + dx.stop - 1) / 2)
        x.append(xaxis[x_center])
        y_center = int((dy.start + dy.stop - 1) / 2)
        y.append(yaxis[y_center])
        max_vals.append(data[y_center, x_center])
    df = pd.DataFrame(
        columns=["x", "y", "amplitude"],
        data=np.vstack([x, y, max_vals]).T,
    )
    if len(df) == 0:
        raise ValueError("No peaks were found. Adjust neighbourhood_size and maxima_threshold values.")
    df.loc[:, "midtime"] = time
    df = df.sort_values(by="amplitude", ascending=False)

    return df.loc[df.index[:best_point_count], :]


def _calculate_azimuth_backazimuth(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """filldocs"""
    df.loc[:, "azimuth"] = df.apply(lambda row: 180 * math.atan2(row.x, row.y) / math.pi, axis=1)
    df.loc[:, "backazimuth"] = df.apply(lambda row: row.azimuth % -360 + 180, axis=1)
    return df


def _calculate_slowness(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """filldocs"""
    try:
        df.loc[:, "slowness"] = df.apply(lambda row: np.sqrt(row.x**2 + row.y**2), axis=1)
    except Exception as e:
        df["slowness"] = 0
        print(f"There was an exception. {e}")
        print(df)
    return df


def validate_if_all_beamforming_params_use_same_component_codes(
    params: Collection[BeamformingParams],
) -> Tuple[str, ...]:
    """
    Validates if all passed :py:class:`~noiz.models.processing_params.BeamformingParams` use the same
    :py:attr:`noiz.models.processing_params.BeamformingParams.used_component_codes`.
    If yes, returns id of a common value of
    :py:attr:`noiz.models.processing_params.BeamformingParams.used_component_codes`

    :param params: Beamforming params to be validated
    :type params: Collection[BeamformingParams]
    :return: ID of a common QCOneConfig
    :rtype: int
    :raises: InconsistentDataException
    """
    component_codes_in_beam_params = [x.used_component_codes for x in params]
    unique_component_codes = list(set(component_codes_in_beam_params))
    if len(unique_component_codes) > 1:
        raise InconsistentDataException(
            f"Mass beamforming can only run if all BeamformingParams use the same use_component_codes. "
            f"Your query contains {len(unique_component_codes)} different used_component_codes attribute. "
            f"To help you with debugging, here are all ids of used configs. "
            f"Tuples of (BeamformingParams.id, BeamformingParams.used_component_codes): \n"
            f"{[(x.id, x.used_component_codes) for x in params]} "
        )
    single_used_component_codes = unique_component_codes[0]
    return single_used_component_codes


def validate_if_all_beamforming_params_use_same_qcone(params: Collection[BeamformingParams]) -> int:
    """
    Validates if all passed :py:class:`~noiz.models.processing_params.BeamformingParams` use the same
    :py:class:`~noiz.models.qc.QCOneConfig`. If yes, returns id of a common config.

    :param params: Beamforming params to be validated
    :type params: Collection[BeamformingParams]
    :return: ID of a common QCOneConfig
    :rtype: int
    :raises: InconsistentDataException
    """
    qcone_ids_in_beam_params = [x.qcone_config_id for x in params]
    unique_qcone_config_ids = list(set(qcone_ids_in_beam_params))
    if len(unique_qcone_config_ids) > 1:
        raise InconsistentDataException(
            f"Mass beamforming can only run if all BeamformingParams use the same QCOneConfig. "
            f"Your query contains {len(unique_qcone_config_ids)} different QCOneConfigs. "
            f"To help you with debugging, here are all ids of used configs. "
            f"Tuples of (BeamformingParams.id, BeamformingParams.qcone_config_id): \n"
            f"{[(x.id, x.qcone_config_id) for x in params]} "
        )
    single_qcone_config_id = unique_qcone_config_ids[0]
    return single_qcone_config_id
