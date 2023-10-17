# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from functools import lru_cache
from loguru import logger
from matplotlib import pyplot as plt
from obspy.core import AttribDict, Stream
from pathlib import Path
from scipy.interpolate import griddata
from scipy.signal import convolve, correlate
from scipy.ndimage import filters as filters
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
from noiz.models.beamforming import BeamformingResult, BeamformingFile, BeamformingPeakAllRelpower, \
    BeamformingPeakAllAbspower, BeamformingPeakAverageRelpower, BeamformingPeakAverageAbspower


def deconv_rlc_stepwise_circles_v_polar( # noqa: max-complexity: 24
        g, arf, sx, sy, vmin, n_iter=50,
        slowness_width_ratio_to_ds=1, slowness_step_ratio_to_ds=1,
        angle_step_min=10, angle_width_start=180,
        theta_overlap_kernel=0.5,
        stop_crit_rel=0.99, stop_crit_rms=0, verbose=True,
        standard_rlc=False, f_start=None, f_sum=None,
        s_stages=[0, 1. / 2, np.infty],
        water_level_rms_pct=0,
        lambda_herve=0,
        reg_coef=0,
):
    """
    Modified RLC deconvolution adapted to seismic array processing.
    Iterative correction of the initial image with constant correction
    applied to arc portions of progressively decreasing azimuthal extent (by stages).

    A. Kazantsev (Storengy, ENGIE) 2022

    :param g:                               input image (beamformer), size n x n
    :param arf:                             array response function, size n x n
    :param sx:                              slowness axis x, size n x 1
    :param sy:                              slowness axis y, size n x 1
    :param vmin:                            minimal physical velocity in the waves (m/s)
    :param n_iter:                          maximum number of iterations per stage
    :param slowness_width_ratio_to_ds:      std of the gaussian kernel over slowness
    :param slowness_step_ratio_to_ds:       step of the gaussian kernel centers over slowness axis
    :param angle_step_mode:                 manages the step of the gaussian kernel centers over angles
                                                if 'auto' : the step is (1-theta_overlap_kernel) * std of the kernel
                                                if 'uniform' : the step is angle_step_min whatever the std of the kernel
    :param angle_step_min:                  minimal bound of the step of the gaussian kernel centers over angles
    :param angle_width_start:               starting value of std of the gaussian kernel over angles
                                                the full range of std over deconvolution stages is
                                                [angle_width_start, angle_width_start/2, ..., angle_step_min/2]
    :param random_angle_at_each_iteration:  boolean, if True then the gaussian kernels over angles will be recomputed
                                                at each iteration with a slight random angular shift
                                                to avoid domain-limit artifacts. This is very time-consuming.
    :param stop_crit_rel:                       criterion of I(n+1)/I(n) for stopping iterations at each stage,
                                                with I the Csiszar cost function

    :return: f:             deconvolved image, such that conv(f, arf) aprroximates g, size n x n
    :return: g_est:         reconstructed g (result of conv(f, arf)), size n x n
    :return: cost_hist:     evolution of Csiszar misfit over iterations, size = total number of iterations
    :return: rms_hist:      evolution of rms misfit over iterations, size = total number of iterations
    """

    # check inputs

    _check_beamformer_shape(g, sx, sy)
    n = g.shape[0]

    arf_initial = arf.copy()
    arf_min = np.min(arf[:])
    if arf_min < 0:
        const_arf_offset = - 1.01 * arf_min
        const_arf_offset_array = np.ones_like(arf) * const_arf_offset
        arf = arf_initial + const_arf_offset_array

    _check_arf_to_be_square(arf)

    # mask for weight computation (same size as ARF)
    m = np.ones((n, n))

    # initialize output
    if f_start is None:
        f = np.ones((n, n))
    else:
        f = f_start

    if not standard_rlc:
        # slowness and azimuth grid
        ds = np.median(np.diff(sx))
        dtheta = (np.pi / 180) * (angle_step_min / 5)
        smax = np.min([np.max(sx), np.max(sy)])
        xv, yv = np.meshgrid(sx, sy)
        s_norm = np.sqrt(xv ** 2 + yv ** 2)

        # polar grids
        theta_axis, s_axis, theta_grid, s_grid, sx_polar, sy_polar = generate_slowness_grid_polar(
            0,
            smax,
            ds,
            dtheta,
        )

        sx_grid_polar = s_grid * np.cos(theta_grid)
        sy_grid_polar = s_grid * np.sin(theta_grid)

        # discard non-physical values from deconvolved image (any pixel initialized at 0 will remain 0 while iterating)
        smax_tolerated = 1 / vmin
        width_muting_taper = 0.25
        muting_taper = 1 - slowness_muting_taper((1 - width_muting_taper) * smax_tolerated, s_norm,
                                                 water_level=0, taper_width=width_muting_taper)
        # f[s_norm > smax_tolerated] = 0
        # m[s_norm > smax_tolerated] = 0
        f *= muting_taper
        m *= muting_taper

        # define kernels for stepwise uniform update of result (circles, than azimuthal portions) :
        # circles :
        s_domain_step = slowness_step_ratio_to_ds * ds
        s_domain_width = slowness_width_ratio_to_ds * ds
        s_domain_centers = np.arange(0, smax + s_domain_step, s_domain_step)

        # azimuthal portions :
        angle_domain_step = angle_step_min * np.pi / 180
        angle_domain_width_list = [360]
        angle_width = angle_width_start * np.pi / 180
        while angle_width >= angle_domain_step * (1 - theta_overlap_kernel):
            angle_domain_width_list.append(angle_width)
            angle_width /= 2

        # NEW KERNELS

        kernel_width_to_std_ratio = 3
        s_domain_width = kernel_width_to_std_ratio * slowness_width_ratio_to_ds * ds
        s_axis_kernel = s_axis[s_axis < s_domain_width]
        s_axis_kernel = s_axis_kernel - np.max(s_axis_kernel) / 2
        [s_grid_kernel_0, theta_grid_kernel_0] = np.meshgrid(s_axis_kernel, theta_axis)
        angle_kernel_0 = np.ones(theta_grid_kernel_0.shape)
        s_kernel_0 = slowness_kernel(0, s_domain_width, s_grid_kernel_0)[0]
        full_kernels = []
        full_norms = []
        kernel_0 = multiply_kernels(angle_kernel_0, s_kernel_0)
        norm_factor_0 = get_norm_factor_for_kernel(kernel_0, theta_grid)
        full_kernels.append(kernel_0)
        full_norms.append(norm_factor_0)
        for angle_width in angle_domain_width_list[1:]:
            kernel_angle_width = kernel_width_to_std_ratio * angle_width

            theta_axis_kernel = theta_axis[theta_axis < kernel_angle_width]
            theta_axis_kernel = theta_axis_kernel - np.max(theta_axis_kernel) / 2
            [s_grid_kernel, theta_grid_kernel] = np.meshgrid(s_axis_kernel, theta_axis_kernel)
            angle_kernel = angular_kernel(angle_center=0, angle_width=angle_width, s_angle=theta_grid_kernel)[0]
            s_kernel = slowness_kernel(0, s_domain_width, s_grid_kernel)[0]
            kernel_i = multiply_kernels(angle_kernel, s_kernel)
            norm_i = get_norm_factor_for_kernel(kernel_i, theta_grid)
            full_kernels.append(multiply_kernels(angle_kernel, s_kernel))
            full_norms.append(norm_i)

    g_initial = g.copy()
    g_min = np.min(g[:])
    if np.sign(g_min * arf_min) == -1:
        raise ValueError('the minima of input ARF and beam do not have the same sign. This is very unlikely')

    if f_sum is not None:
        f = (f / np.sum(f[:])) * f_sum
        assert np.sum(f[:]) == f_sum
    if arf_min < 0:
        const_g_offset = const_arf_offset * np.sum(f[:])
        if const_g_offset <= -g_min:
            raise ValueError('arf_shift * f0 insufficient for g positivity. check implementation.')
        const_g_offset_array = np.ones_like(g) * const_g_offset
        g = g_initial + const_g_offset_array

    f_init_val = min(0.99, 1e-3 * np.sqrt(np.mean(f[:] ** 2)))

    # weight term
    alpha = compute_arf_action(arf, m)

    # initialize cost function history
    cost_hist = []
    rms_hist = []

    # copy of input image for cost function evaluation at each iteration
    g_for_cost_evol = g.copy()
    g_for_update = g.copy()

    # initial cost function
    cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol, nonpositive_arf=(arf_min < 0),
                                              reg_coef=reg_coef)

    rms_hist.append(rms_new)
    cost_hist.append(cost_new)
    cost_old = cost_new

    i_iter = 0  # iteration counter
    cost_new = 0

    f_by_stages = []
    if standard_rlc:
        print('standard rlc')
        while (np.abs(cost_new / cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter):
            cost_old = cost_hist[-1]
            i_iter += 1
            corr_term = compute_rlc_corr_term(arf, f, g_for_update, m, alpha, reg_coef=reg_coef,
                                              water_level=water_level_rms_pct)
            f = corr_term * f
            if lambda_herve > 0:
                f_max = 1 * np.quantile(f[:], 1)
                i_downweight = (f > 0) & (f < f_max)
                f[i_downweight] = (f[i_downweight] ** 2) / \
                                  (f[i_downweight] + lambda_herve * (f_max - f[i_downweight]))
            cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol, nonpositive_arf=(arf_min < 0),
                                                      reg_coef=reg_coef)

            cost_hist.append(cost_new)
            rms_hist.append(rms_new)

        f_by_stages.append(f)
    else:
        no_stage_flag = s_stages is None
        if s_stages is None:
            s_stages = [min(s_domain_centers), max(s_domain_centers)]
        i_stage = -1

        while i_stage < len(s_stages) - 1:

            cost_hist_this_stage = [cost_hist[-1]]
            rms_hist_this_stage = [rms_hist[-1]]

            s_start = min(s_domain_centers) - (0.1 * ds)

            if i_stage == -1:
                s_start_stage = s_start
            else:
                s_start_stage = s_stages[-i_stage - 2] - (0.1 * ds)

            if i_stage >= 0:
                f_previous_stage = f.copy()
                g_est_previous_stage = g_est.copy()

                s_kernel_mute = slowness_muting_taper(s_start_stage, s_norm, water_level=f_init_val)

                f = f * s_kernel_mute
                cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol,
                                                          nonpositive_arf=(arf_min < 0),
                                                          reg_coef=reg_coef)
                cost_hist_this_stage.append(cost_new)
                rms_hist_this_stage.append(rms_new)

            for j in range(len(angle_domain_width_list)):
                angle_width = angle_domain_width_list[j]
                i_iter = 0  # iteration counter
                cost_new = 0
                kernel_j = full_kernels[j]
                norm_j = full_norms[j]
                ntheta_kernel = kernel_j.shape[0]
                npadding_periodic_conv = int(np.ceil(ntheta_kernel / 2))

                if verbose:
                    print('stage delta_theta = ' + str(angle_width))
                while (np.abs(cost_new / cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter):
                    cost_old = cost_hist_this_stage[-1]
                    i_iter += 1
                    corr_term = compute_rlc_corr_term(arf, f, g_for_update, m, alpha, reg_coef=reg_coef,
                                                      water_level=water_level_rms_pct)
                    corr_term_polar = griddata(np.vstack((xv.flatten(), yv.flatten())).T, corr_term.flatten(),
                                               np.vstack((sx_grid_polar.flatten(), sy_grid_polar.flatten())).T,
                                               fill_value=1)
                    corr_term_polar = np.reshape(corr_term_polar, sx_grid_polar.shape).T
                    padding_top = corr_term_polar[-npadding_periodic_conv:, :]
                    padding_bottom = corr_term_polar[:npadding_periodic_conv, :]
                    corr_term_polar_padded = np.vstack((padding_top, corr_term_polar, padding_bottom))

                    #   polar to do : convolve by polar kernel
                    corr_term_polar_padded = convolve(corr_term_polar_padded, kernel_j, mode="same")
                    corr_term_polar_padded = corr_term_polar_padded[npadding_periodic_conv:-npadding_periodic_conv, :]
                    corr_term_polar_padded = corr_term_polar_padded / norm_j

                    #   polar to do : go back to cartesian coordinates
                    corr_term_copy = griddata(np.vstack((sx_grid_polar.flatten(), sy_grid_polar.flatten())).T,
                                              corr_term_polar_padded.T.flatten(),
                                              np.vstack((xv.flatten(), yv.flatten())).T,
                                              fill_value=1)
                    corr_term_copy = np.reshape(corr_term_copy, xv.shape)
                    f = corr_term_copy * f

                    if lambda_herve > 0:
                        f_max = 1 * np.quantile(f[:], 1)
                        i_downweight = (f > 0) & (f < f_max)
                        f[i_downweight] = (f[i_downweight] ** 2) / \
                                          (f[i_downweight] + lambda_herve * (f_max - f[i_downweight]))
                    cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol,
                                                              nonpositive_arf=(arf_min < 0),
                                                              reg_coef=reg_coef)

                    cost_hist_this_stage.append(cost_new)
                    rms_hist_this_stage.append(rms_new)

            f_out = f.copy()

            if i_stage == -1:
                cost_hist = cost_hist_this_stage
                rms_hist = rms_hist_this_stage
            else:
                if (rms_hist[-1] / rms_hist_this_stage[-1]) > stop_crit_rel:
                    cost_hist = [*cost_hist, *cost_hist_this_stage]
                    rms_hist = [*rms_hist, *rms_hist_this_stage]
                else:
                    f = f_previous_stage.copy()
                    g_est = g_est_previous_stage.copy()
            f_by_stages.append(f)
            if no_stage_flag:
                i_stage = np.infty
            else:
                i_stage += 1

    cost_hist = np.array(cost_hist)
    rms_hist = np.array(rms_hist)

    return f_out, g_est, cost_hist, rms_hist, f_by_stages


def _check_arf_to_be_square(arf):
    if arf.shape[0] != arf.shape[1]:
        raise ValueError('The arf needs to be be a square')


def _check_beamformer_shape(g, sx, sy):
    if not g.shape[0] == len(sy):
        raise ValueError(f"Shape of input beamformer needs to be equal to slowness axes. g.shape[0] != len(sy) "
                         f"{g.shape[0]} != {len(sy)}")
    if not g.shape[1] == len(sx):
        raise ValueError(f"Shape of input beamformer needs to be equal to slowness axes. g.shape[1] != len(sx)"
                         f"{g.shape[1]} != {len(sx)}")
    if not len(sy) == len(sx):
        raise ValueError(f"Input beamformer needs to be a square. len(sy) != len(sx) {len(sy)} != {len(sx)}")


# def deconv_rlc_stepwise_circles(g,
#                                 arf,
#                                 sx,
#                                 sy,
#                                 vmin,
#                                 n_iter=50,
#                                 slowness_width_ratio_to_ds=1,
#                                 slowness_step_ratio_to_ds=1,
#                                 angle_step_min=10,
#                                 angle_width_start=180,
#                                 angle_step_mode='auto',
#                                 theta_overlap_kernel=0.5,
#                                 random_angle_at_each_iteration=False,
#                                 stop_crit_rel=0.99,
#                                 stop_crit_rms=0,
#                                 verbose=True,
#                                 standard_rlc=False,
#                                 f_start=None,
#                                 f_sum=None,
#                                 s_stages=[0, 1. / 2, np.infty],
#                                 water_level_rms_pct=0,
#                                 lambda_herve=0,
#                                 reg_coef=0,
#                                 path_save_intermediate_figures=None):
#     # s_stages = None):
#     """
#     Modified RLC deconvolution adapted to seismic array processing.
#     Iterative correction of the initial image with constant correction
#     applied to arc portions of progressively decreasing azimuthal extent (by stages).
#
#     A. Kazantsev (Storengy, ENGIE) 2022
#
#     :param g:                               input image (beamformer), size n x n
#     :param arf:                             array response function, size n x n
#     :param sx:                              slowness axis x, size n x 1
#     :param sy:                              slowness axis y, size n x 1
#     :param vmin:                            minimal physical velocity in the waves (m/s)
#     :param n_iter:                          maximum number of iterations per stage
#     :param slowness_width_ratio_to_ds:      std of the gaussian kernel over slowness
#     :param slowness_step_ratio_to_ds:       step of the gaussian kernel centers over slowness axis
#     :param angle_step_mode:                 manages the step of the gaussian kernel centers over angles
#                                                 if 'auto' : the step is (1-theta_overlap_kernel) * std of the kernel
#                                               if 'uniform' : the step is angle_step_min whatever the std of the kernel
#     :param angle_step_min:                  minimal bound of the step of the gaussian kernel centers over angles
#     :param angle_width_start:               starting value of std of the gaussian kernel over angles
#                                                 the full range of std over deconvolution stages is
#                                                 [angle_width_start, angle_width_start/2, ..., angle_step_min/2]
#     :param random_angle_at_each_iteration:  boolean, if True then the gaussian kernels over angles will be recomputed
#                                                 at each iteration with a slight random angular shift
#                                                 to avoid domain-limit artifacts. This is very time-consuming.
#     :param stop_crit_rel:                       criterion of I(n+1)/I(n) for stopping iterations at each stage,
#                                                 with I the Csiszar cost function
#
#     :return: f:             deconvolved image, such that conv(f, arf) aprroximates g, size n x n
#     :return: g_est:         reconstructed g (result of conv(f, arf)), size n x n
#     :return: cost_hist:     evolution of Csiszar misfit over iterations, size = total number of iterations
#     :return: rms_hist:      evolution of rms misfit over iterations, size = total number of iterations
#     """
#
#     # check inputs
#     # assert g.shape == arf.shape
#     # logger.info("l.96 deconv_rlc_stepwise_circles")
#     assert g.shape[0] == len(sy)
#     assert g.shape[1] == len(sx)
#
#     # normalize images
#     # norm_arf = np.sum(np.abs(arf[:]))
#     # norm_g = np.max(np.abs(g[:]))
#     # arf = arf / norm_arf
#     # g = g / norm_g
#
#     arf_initial = arf.copy()
#     arf_min = np.min(arf[:])
#     # arf_min = np.infty
#     if arf_min < 0:
#         const_arf_offset = - 1.01 * arf_min
#         const_arf_offset_array = np.ones_like(arf) * const_arf_offset
#         arf = arf_initial + const_arf_offset_array
#
#     # size of beamformer
#     # logger.info("l.115 deconv_rlc_stepwise_circles")
#     size_g = g.shape  # image
#     if size_g[0] != size_g[1]:
#         raise Exception('the input image should be square')
#     else:
#         n = size_g[0]
#
#     # size of ARF
#     # logger.info("l.123 deconv_rlc_stepwise_circles")
#     size_arf = arf.shape
#     if size_arf[0] != size_arf[1]:
#         raise Exception('the arf should be square')
#     else:
#         n_arf = size_arf[0]
#
#     # mask for weight computation (same size as ARF)
#     m = np.ones((n, n))
#
#     # initialize output
#     if f_start is None:
#         f = np.ones((n, n))  # * np.sqrt(np.mean(g**2))
#     else:
#         f = f_start
#
#     if not standard_rlc:
#         # slowness and azimuth grid
#         ds = np.median(np.diff(sx))
#         smax = np.min([np.max(sx), np.max(sy)])
#         xv, yv = np.meshgrid(sx, sy)
#         s_norm = np.sqrt(xv ** 2 + yv ** 2)
#         s_angle = np.arctan2(yv, xv)
#
#         # discard non-physical values from deconvolved image(any pixel initialized at 0 will remain 0 while iterating)
#         smax_tolerated = 1 / vmin
#         width_muting_taper = 0.25
#         muting_taper = 1 - slowness_muting_taper((1 - width_muting_taper) * smax_tolerated, s_norm,
#                                                  water_level=0, taper_width=width_muting_taper)
#         # f[s_norm > smax_tolerated] = 0
#         # m[s_norm > smax_tolerated] = 0
#         f *= muting_taper
#         m *= muting_taper
#
#         # define kernels for stepwise uniform update of result (circles, than azimuthal portions) :
#         # circles :
#         s_domain_step = slowness_step_ratio_to_ds * ds
#         s_domain_width = slowness_width_ratio_to_ds * ds
#         s_domain_centers = np.arange(0, smax + s_domain_step, s_domain_step)
#
#         circle_list = []
#         for s in s_domain_centers[:-2]:
#             circle_i = plt.Circle((0, 0), s + s_domain_step, color='w', fill=False, linewidth=0.4)
#             circle_list.append(circle_i)
#
#         s_kernels = [slowness_kernel(s_domain_center_i, s_domain_width, s_norm)[0]
#                      for s_domain_center_i in s_domain_centers]
#         s_ranges = [np.abs(s_norm - s_domain_center_i) <= (s_domain_step / 2)
#                     for s_domain_center_i in s_domain_centers]
#
#         # azimuthal portions :
#         angle_domain_step = angle_step_min * np.pi / 180
#         angle_domain_width_list = []
#         angle_width = angle_width_start * np.pi / 180
#         while angle_width >= angle_domain_step * (1 - theta_overlap_kernel):
#             angle_domain_width_list.append(angle_width)
#             angle_width /= 2
#         if angle_step_mode == 'auto':
#             angle_domain_centers_list = [np.arange(-np.pi, np.pi, angle_domain_width_i * (1 - theta_overlap_kernel))
#                                          for angle_domain_width_i in angle_domain_width_list]
#             random_angle_shift = (np.random.random(size=len(angle_domain_width_list)) - 0.5) * \
#                 angle_domain_width_list * (1 - theta_overlap_kernel)
#         else:
#             angle_domain_centers_list = [np.arange(-np.pi, np.pi, angle_domain_step)
#                                          for _ in angle_domain_width_list]
#             random_angle_shift = (np.random.random(size=len(angle_domain_width_list)) - 0.5) * \
#                 angle_domain_step
#
#         angle_kernels = [[angular_kernel(angle_center, angle_width, s_angle)[0]
#                           for angle_center in (angle_domain_centers_j + random_angle)]
#                          for (angle_width, random_angle, angle_domain_centers_j)
#                          in zip(angle_domain_width_list, random_angle_shift, angle_domain_centers_list)]
#         angle_ranges = [[np.abs(angular_distance_grid(angle_center, s_angle)) <=
#                          (np.median(np.diff(angle_domain_centers_j)) / 2)
#                          for angle_center in (angle_domain_centers_j + random_angle)]
#                         for (angle_width, random_angle, angle_domain_centers_j)
#                         in zip(angle_domain_width_list, random_angle_shift, angle_domain_centers_list)]
#
#         # combinations :
#         full_kernels = [[[multiply_kernels(angle_kernels[j][k], s_kernels[i])
#                           for k in range(len(angle_domain_centers_list[j]))]
#                          for j in range(len(angle_domain_width_list))]
#                         for i in range(len(s_domain_centers))]
#         full_ranges = [[[(angle_ranges[j][k] & s_ranges[i])
#                          for k in range(len(angle_domain_centers_list[j]))]
#                         for j in range(len(angle_domain_width_list))]
#                        for i in range(len(s_domain_centers))]
#
#     g_initial = g.copy()
#     g_min = np.min(g[:])
#     if np.sign(g_min * arf_min) == -1:
#         raise Exception('the minima of input ARF and beam do not have the same sign. This is very unlikely')
#     # arf_min = np.infty
#
#     water_level = water_level_rms_pct * np.sqrt(np.mean(g[:] ** 2))
#
#     if f_sum is not None:
#         f = (f / np.sum(f[:])) * f_sum
#         assert np.sum(f[:]) == f_sum
#     if arf_min < 0:
#         const_g_offset = const_arf_offset * np.sum(f[:])
#         if const_g_offset <= -g_min:
#             raise Exception('arf_shift * f0 insufficient for g positivity. check implementation.')
#         const_g_offset_array = np.ones_like(g) * const_g_offset
#         g = g_initial + const_g_offset_array
#
#     f_init_val = min(0.99, 1e-3 * np.sqrt(np.mean(f[:] ** 2)))
#
#     # weight term
#     m_init = m.copy()
#     alpha = compute_arf_action(arf, m)
#     #     w = 1. / alpha
#     #     w[alpha < 0.1] = 0
#
#     # initialize cost function history
#     cost_hist = []
#     rms_hist = []
#
#     # copy of input image for cost function evaluation at each iteration
#     g_for_cost_evol = g.copy()
#     # if arf_min < 0:
#     #     g_for_update = g.copy() + compute_arf_action(const_arf_offset_array, f)
#     # else:
#     g_for_update = g.copy()
#
#     # initial cost function
#     cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol, nonpositive_arf=(arf_min < 0),
#                                               reg_coef=reg_coef)
#
#     rms_hist.append(rms_new)
#     cost_hist.append(cost_new)
#     cost_old = cost_new
#
#     i_iter = 0  # iteration counter
#     cost_new = 0
#
#     # value if iter for illustration plot
#     if path_save_intermediate_figures is not None:
#         ax_list_rms_plots_per_j = []
#         fig_list_rms_plots_per_j = []
#         i_iter_plot = 1
#         angle_kernel_plot = np.pi/4
#         s_kernel_plot = 1   # s/km
#         i_s_plot = np.min(np.where(s_domain_centers > (s_kernel_plot - s_domain_width))[0])
#
#     if standard_rlc:
#         print('standard rlc')
#         while (np.abs(cost_new / cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter):
#             cost_old = cost_hist[-1]
#             i_iter += 1
#             # if arf_min < 0:
#             #     g_for_update = g + compute_arf_action(const_arf_offset_array, f)
#             corr_term = compute_rlc_corr_term(arf, f, g_for_update, m, alpha, reg_coef=reg_coef,
#                                               water_level=water_level_rms_pct)
#             f = corr_term * f
#             if lambda_herve > 0:
#                 f_max = 1 * np.quantile(f[:], 1)
#                 i_downweight = (f > 0) & (f < f_max)
#                 # i_downweight = (f > 0) & (f < 0.5 * f_max)
#                 # i_downweight = (f > 0) & (f < 1)
#                 # f[i_downweight] = (f[i_downweight] ** 2) / (f[i_downweight] + lambda_herve)
#                 f[i_downweight] = (f[i_downweight] ** 2) / \
#                                   (f[i_downweight] + lambda_herve * (f_max - f[i_downweight]))
#                 # f[f > 2] = 2
#             cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol, nonpositive_arf=(arf_min < 0),
#                                                       reg_coef=reg_coef)
#
#             cost_hist.append(cost_new)
#             rms_hist.append(rms_new)
#
#             if verbose:
#                 print('it. ' + str(i_iter) + ': ' + str(cost_new / cost_old) + ', rms = ' + str(rms_new))
#
#     else:
#
#         no_stage_flag = s_stages is None
#         if s_stages is None:
#             s_stages = [min(s_domain_centers), max(s_domain_centers)]
#         i_stage = -1
#
#         while i_stage < len(s_stages) - 1:
#
#             cost_hist_this_stage = [cost_hist[-1]]
#             rms_hist_this_stage = [rms_hist[-1]]
#
#             s_start = min(s_domain_centers) - (0.1 * ds)
#             s_stop = max(s_domain_centers) + (0.1 * ds)
#
#             if i_stage == -1:
#                 s_start_stage = s_start
#                 s_stop_stage = s_stop
#             else:
#                 s_start_stage = s_stages[-i_stage - 2] - (0.1 * ds)
#                 s_stop_stage = s_stages[-i_stage - 1] + (0.1 * ds)
#
#             if verbose:
#                 print('slowness stage ' + str(i_stage) + ': s < ' + str(s_stop_stage))
#
#             # f[s_norm < s_start] = 0
#             i_s = np.where((s_domain_centers > s_start) & (s_domain_centers < s_stop))[0]
#             i_s_inner = np.where(s_domain_centers < s_stop_stage)[0]
#             # i_s_outer = np.where((s_domain_centers >= s_stop_stage) & (s_domain_centers <= s_stop))[0]
#
#             if i_stage >= 0:
#                 f_previous_stage = f.copy()
#                 g_est_previous_stage = g_est.copy()
#
#                 # if ((np.abs(cost_new/cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter)):
#                 s_kernel_mute = slowness_muting_taper(s_start_stage, s_norm, water_level=f_init_val)
#                 f = f * s_kernel_mute
#                 cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol,
#                                                           nonpositive_arf=(arf_min < 0),
#                                                           reg_coef=reg_coef)
#                 cost_hist_this_stage.append(cost_new)
#                 rms_hist_this_stage.append(rms_new)
#
#             # circle correction stage
#             if verbose:
#                 print('stage cicle stage')
#             while (np.abs(cost_new / cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter):
#                 cost_old = cost_hist_this_stage[-1]
#                 i_iter += 1
#                 # if arf_min < 0:
#                 #     g_for_update = g + compute_arf_action(const_arf_offset_array, f)
#                 corr_term = compute_rlc_corr_term(arf, f, g_for_update, m, alpha, reg_coef=reg_coef,
#                                                   water_level=water_level_rms_pct)
#                 corr_term_copy = np.ones_like(corr_term)
#                 for i in i_s:
#                     corr_term_copy[s_ranges[i]] = np.sum(corr_term * s_kernels[i])
#                     # f[s_ranges[i]] = np.sum(f * s_kernels[i])
#                 f0 = f.copy()
#                 f = corr_term_copy * f
#                 if path_save_intermediate_figures is not None:
#                     if i_iter_plot == i_iter:
#                         corr_term_copy[f == 0] = np.nan
#                         plot_intermediate_sequence_rlc(sx, sy, f0, f, corr_term, corr_term_copy, i_iter,
#                                                        i_slowness_stage=i_stage,
#                                                        smax=smax,
#                                                        circle_list=circle_list, s_kernel_plot=s_kernel_plot,
#                                                        angle_kernel_plot=angle_kernel_plot, i_s_plot=i_s_plot,
#                                                        s_kernels=s_kernels, full_kernels=full_kernels,
#                                                        angle_domain_centers_list=None, j=None, angle_step_j=None,
#                                                        circle_stage=True,
#                                                        path_save=path_save_intermediate_figures)
#
#                 if lambda_herve > 0:
#                     f_max = 1 * np.quantile(f[:], 1)
#                     i_downweight = (f > 0) & (f < f_max)
#                     # i_downweight = (f > 0) & (f < 0.5 * f_max)
#                     # i_downweight = (f > 0) & (f < 1)
#                     # f[i_downweight] = (f[i_downweight] ** 2) / (f[i_downweight] + lambda_herve)
#                     f[i_downweight] = (f[i_downweight] ** 2) / \
#                                       (f[i_downweight] + lambda_herve * (f_max - f[i_downweight]))
#                     # f[f > 2] = 2
#                 cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol,
#                                                           nonpositive_arf=(arf_min < 0),
#                                                           reg_coef=reg_coef)
#
#                 cost_hist_this_stage.append(cost_new)
#                 rms_hist_this_stage.append(rms_new)
#
#                 if verbose:
#                     print('it. ' + str(i_iter) + ': ' + str(cost_new / cost_old) + ', rms = ' + str(rms_new))
#
#             if path_save_intermediate_figures is not None:
#                 fig_list_rms_plots_per_j, ax_list_rms_plots_per_j = plot_end_of_stage_rlc(
#                   sx, sy, f, i_iter,
#                   i_stage,
#                   rms_hist_this_stage,
#                   fig_list_rms_plots_per_j,
#                   ax_list_rms_plots_per_j, j=None,
#                   circle_stage=True,
#                   path_save=path_save_intermediate_figures
#                  )
#
#             # stepwise decreasing azimuthal range stage
#             for j in range(len(angle_domain_width_list)):
#                 # if arf_min < 0:
#                 #     arf = arf_initial + const_g_offset / np.sum(f)
#                 #     print('min(arf) new = ' + str(np.min(arf[:])))
#                 #     alpha = compute_arf_action(arf, m)
#                 #     w = 1. / alpha
#                 angle_step_j = np.median(np.diff(angle_domain_centers_list[j]))
#                 angle_width = angle_domain_width_list[j]
#                 if verbose:
#                     print('stage delta_theta = ' + str(angle_width))
#                 i_iter = 0  # iteration counter
#                 cost_new = 0
#                 while (np.abs(cost_new / cost_old) < stop_crit_rel) & (rms_new > stop_crit_rms) & (i_iter < n_iter):
#                     cost_old = cost_hist_this_stage[-1]
#                     i_iter += 1
#                     # if arf_min < 0:
#                     #     g_for_update = g + compute_arf_action(const_arf_offset_array, f)
#                     corr_term = compute_rlc_corr_term(arf, f, g_for_update, m, alpha, reg_coef=reg_coef,
#                                                       water_level=water_level_rms_pct)
#                     corr_term_copy = np.ones_like(corr_term)
#                     if random_angle_at_each_iteration:
#                         random_angle_shift = (np.random.random(size=1) - 0.5) * angle_width
#                     for i in i_s:
#                         if (i in i_s_inner):
#                             for k in range(len(angle_domain_centers_list[j])):
#                                 if random_angle_at_each_iteration:
#                                     angle_kernel = \
#                                         angular_kernel(angle_domain_centers_list[j][k] + random_angle_shift,
#                                                        angle_width,
#                                                        s_angle)[0]
#                                     angle_range = np.abs(
#                                         angular_distance_grid(angle_domain_centers_list[j][k] + random_angle_shift,
#                                                               s_angle)) \
#                                         <= np.median(np.diff(angle_domain_centers_list[j])) / 2
#                                     full_range = angle_range & s_ranges[i]
#                                     full_kernel = multiply_kernels(angle_kernel, s_kernels[i])
#                                 else:
#                                     full_kernel = full_kernels[i][j][k]
#                                     full_range = full_ranges[i][j][k]
#                                 corr_term_copy[full_range] = np.sum(corr_term * full_kernel)
#                         else:
#                             # angle_width_default = np.pi/4
#                             # j_default = np.argmin(np.abs(angle_width_default - np.array(angle_domain_width_list)))
#                             # full_kernel = full_kernels[i][j_default][k]
#                             # full_range = full_ranges[i][j_default][k]
#                             full_kernel = s_kernels[i]
#                             full_range = s_ranges[i]
#                             corr_term_copy[full_range] = np.sum(corr_term * full_kernel)
#                             # f[angle_range] = np.sum(f * full_kernel)
#                     f0 = f.copy()
#                     f = corr_term_copy * f
#
#                     if path_save_intermediate_figures is not None:
#                         if i_iter == i_iter_plot:
#                             corr_term_copy[f == 0] = np.nan
#                             plot_intermediate_sequence_rlc(sx, sy, f0, f, corr_term, corr_term_copy, i_iter,
#                                                            i_slowness_stage=i_stage,
#                                                            smax=smax,
#                                                            circle_list=circle_list, s_kernel_plot=s_kernel_plot,
#                                                            angle_kernel_plot=angle_kernel_plot, i_s_plot=i_s_plot,
#                                                            s_kernels=s_kernels, full_kernels=full_kernels,
#                                                            angle_domain_centers_list=angle_domain_centers_list, j=j,
#                                                            angle_step_j=angle_step_j,
#                                                            circle_stage=False,
#                                                            path_save=path_save_intermediate_figures)
#
#                     if lambda_herve > 0:
#                         f_max = 1 * np.quantile(f[:], 1)
#                         i_downweight = (f > 0) & (f < f_max)
#                         # i_downweight = (f > 0) & (f < 0.5 * f_max)
#                         # i_downweight = (f > 0) & (f < 1)
#                         # f[i_downweight] = (f[i_downweight] ** 2) / (f[i_downweight] + lambda_herve)
#                         f[i_downweight] = f[i_downweight] = (
#                                                         f[i_downweight] ** 2) / \
#                                                         (f[i_downweight] + lambda_herve * (f_max - f[i_downweight]))
#                         # f[f > 2] = 2
#                     cost_new, rms_new, g_est = update_misfits(arf, f, g_for_cost_evol,
#                                                               nonpositive_arf=(arf_min < 0),
#                                                               reg_coef=reg_coef)
#
#                     cost_hist_this_stage.append(cost_new)
#                     rms_hist_this_stage.append(rms_new)
#                     if verbose:
#                         print('it. ' + str(i_iter) + ': ' + str(cost_new / cost_old) + ', rms = ' + str(rms_new))
#                 if path_save_intermediate_figures is not None:
#                     fig_list_rms_plots_per_j, ax_list_rms_plots_per_j = plot_end_of_stage_rlc(sx, sy, f, i_iter,
#                                                                                               i_stage,
#                                                                                               rms_hist_this_stage,
#                                                                                               fig_list_rms_plots_per_j,
#                                                                                               ax_list_rms_plots_per_j,
#                                                                                               j=j,
#                                                                                               circle_stage=False,
#                                                                                               path_save=path_save_intermediate_figures)
#
#             if path_save_intermediate_figures is not None:
#                 plots_rms_rescale_and_save(fig_list_rms_plots_per_j, ax_list_rms_plots_per_j, i_stage,
#                                            len(rms_hist_this_stage),
#                                            path_save=path_save_intermediate_figures)
#
#             if i_stage == -1:
#                 cost_hist = cost_hist_this_stage
#                 rms_hist = rms_hist_this_stage
#             else:
#                 if (cost_hist[-1] / cost_hist_this_stage[-1]) > stop_crit_rel:
#                     cost_hist = [*cost_hist, *cost_hist_this_stage]
#                     rms_hist = [*rms_hist, *rms_hist_this_stage]
#                 else:
#                     f = f_previous_stage
#                     g_est = g_est_previous_stage
#             # print('i_stage ' + str(i_stage))
#             #             print(rms_hist)
#             if no_stage_flag:
#                 i_stage = np.infty
#             else:
#                 i_stage += 1
#
#     cost_hist = np.array(cost_hist)
#     rms_hist = np.array(rms_hist)
#
#     # return f * norm_g / norm_arf, g_est * norm_g, cost_hist, rms_hist
#     return f, g_est, cost_hist, rms_hist


def get_norm_factor_for_kernel(kernel_j, theta_grid):
    ntheta_kernel = kernel_j.shape[0]
    npadding_periodic_conv = int(np.ceil(ntheta_kernel / 2))
    unit_matrix = np.ones(theta_grid.T.shape)
    padding_top = unit_matrix[-npadding_periodic_conv:, :]
    padding_bottom = unit_matrix[:npadding_periodic_conv, :]
    unit_matrix_padded = np.vstack((padding_top, unit_matrix, padding_bottom))
    norm_factor_padded = convolve(unit_matrix_padded, kernel_j, mode='same')
    norm_factor_padded = norm_factor_padded[npadding_periodic_conv:-npadding_periodic_conv, :]
    return norm_factor_padded


# all necessary functions for deconvolution by RLC
def find_single_sources_rms(g, arf, threshold_significant):
    g_norm = g.copy() / max(g.flatten())
    bool_significant = (g_norm > threshold_significant * max(g_norm.flatten()))
    g_thresh = 0 * g_norm
    g_thresh[bool_significant] = 1
    rms_matrix_norm = 0 * g.copy()
    rms_matrix_thresh = 0 * g.copy()
    for i in range(g.shape[0]):
        for j in range(g.shape[1]):
            test_base = 0 * g.copy()
            test_base[i, j] = 1
            test_g = convolve(test_base, arf, mode='same')
            bool_significant_test = (test_g > threshold_significant * max(test_g.flatten()))
            test_g_thresh = 0 * test_g
            test_g_thresh[bool_significant_test] = 1
            test_g_norm = test_g / max(test_g.flatten())
            square_diff_thresh = (test_g_thresh - g_thresh) ** 2
            square_diff_norm = (test_g_norm - g_norm) ** 2
            rms_matrix_thresh[i, j] = np.sqrt(np.sum(square_diff_thresh[bool_significant].flatten()) / np.sum(
                (g_thresh[bool_significant] ** 2).flatten()))
            rms_matrix_norm[i, j] = np.sqrt(np.sum(square_diff_norm.flatten()) / np.sum((g_norm ** 2).flatten()))
            # rms_matrix_norm[i,j] = np.sqrt(np.sum(square_diff_norm[bool_significant].flatten()) / np.sum((g_norm[bool_significant]**2).flatten()))
            # if rms_norm < threshold_rms:
            #     i_sources.append(i)
            #     j_sources.append(j)
    return rms_matrix_thresh, rms_matrix_norm


def generate_single_source_list(df_minima, ns):
    f_list = []
    f_initial = np.zeros((ns, ns))
    for (j, df_source) in df_minima.iterrows():
        f_test = f_initial.copy()
        f_test[int(df_source['i_y']), int(df_source['i_x'])] = 1
        f_list.append(f_test)

    return f_list


def optimize_relative_weights_v1(g, arf, f_list):
    weight_list = np.logspace(-1, 1, 21)
    weight_list = np.hstack((0., weight_list))
    n_sources = len(f_list)

    combinations = itertools.combinations(weight_list, r=n_sources - 1)
    combination_list = [combi for combi in combinations]
    rms_list = np.nan * np.zeros(len(combination_list))
    factor_list = np.nan * np.zeros(len(combination_list))
    for (i, weights_i) in enumerate(combination_list):
        f_test = np.zeros(g.shape)
        for (j, f_j) in enumerate(f_list):
            if j == 0:
                f_test += f_j
            else:
                f_test += weights_i[j - 1] * f_j
        g_test = convolve(f_test, arf, mode='same')
        factor = max(g.flatten()) / max(g_test.flatten())
        g_test_scaled = factor * g_test.copy()
        factor_list[i] = factor
        rms_list[i] = np.sqrt(np.sum((g_test_scaled - g) ** 2) / np.sum(g ** 2))
    i_best = np.argmin(rms_list)
    weight_list_best = factor_list[i_best] * np.hstack((np.array([1]), np.array(combination_list[i_best])))

    return weight_list_best


def slowness_kernel(s_domain_center, s_domain_width, s_norm):
    s_kernel = np.exp(-(s_norm - s_domain_center) ** 2 / s_domain_width ** 2)
    s_kernel = s_kernel / np.sum(s_kernel[:])
    d_s_angle_kernel = -2 * ((s_norm - s_domain_center) / s_domain_width) * (1 / s_domain_width) * s_kernel
    d2_s_angle_kernel = (-2 + 4 * ((s_norm - s_domain_center) / s_domain_width) ** 2) * (
            1 / s_domain_width ** 2) * s_kernel
    return s_kernel, d_s_angle_kernel, d2_s_angle_kernel


def angular_kernel(angle_center, angle_width, s_angle, taper_type='gauss', sin_taper_width=0.1):
    angle_distance_grid = angular_distance_grid(angle_center, s_angle)
    if taper_type == 'gauss':
        # https://stackoverflow.com/questions/1878907/how-can-i-find-the-difference-between-two-angles
        angle_kernel = np.exp(-(angle_distance_grid ** 2) / (angle_width ** 2))
        d_theta_angle_kernel = -2 * (angle_distance_grid / angle_width) * (1 / angle_width) * angle_kernel
        d2_theta_angle_kernel = (-2 + 4 * (angle_distance_grid / angle_width) ** 2) * (
                1 / angle_width ** 2) * angle_kernel
    elif taper_type == 'boxcar':
        angle_kernel = np.zeros_like(s_angle)
        angle_kernel[np.abs(angle_distance_grid) < angle_width / 2] = 1
        d_theta_angle_kernel = np.nan * angle_kernel
        d2_theta_angle_kernel = np.nan * angle_kernel
    elif taper_type == 'sine':
        angle_half_width = angle_width / 2
        width_transition = sin_taper_width * angle_half_width
        coef_in_sin = np.pi / 2 / width_transition
        angle_kernel = np.zeros_like(s_angle)
        range_ones = np.abs(angle_distance_grid) <= (angle_half_width - width_transition)
        range_transition_1 = (angle_distance_grid >= -angle_half_width) & \
                             (angle_distance_grid <= -angle_half_width + width_transition)
        range_transition_2 = (angle_distance_grid <= angle_half_width) & \
                             (angle_distance_grid >= angle_half_width - width_transition)
        angle_kernel[range_ones] = 1
        angle_kernel[range_transition_1] = \
            np.sin(coef_in_sin * (angle_distance_grid[range_transition_1] + angle_half_width)) ** 2
        angle_kernel[range_transition_2] = \
            np.sin(coef_in_sin * (angle_half_width - angle_distance_grid[range_transition_2])) ** 2
        d_theta_angle_kernel = np.nan * angle_kernel
        d2_theta_angle_kernel = np.nan * angle_kernel

    angle_kernel = angle_kernel / np.sum(angle_kernel[:])
    d_theta_angle_kernel = d_theta_angle_kernel / np.sum(angle_kernel[:])
    d2_theta_angle_kernel = d2_theta_angle_kernel / np.sum(angle_kernel[:])
    return angle_kernel, d_theta_angle_kernel, d2_theta_angle_kernel


def detect_boundaries(img):
    # from https://towardsdatascience.com/edge-detection-in-python-a3c263a13e03
    # define the vertical filter
    vertical_filter = [[-1, -2, -1], [0, 0, 0], [1, 2, 1]]

    # define the horizontal filter
    horizontal_filter = [[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]]

    # read in the pinwheel image
    # img = plt.imread('pinwheel.jpg')

    # get the dimensions of the image
    n, m = img.shape

    # initialize the edges image
    edges_img = img.copy()

    # loop over all pixels in the image
    for row in range(3, n - 2):
        for col in range(3, m - 2):
            # create little local 3x3 box
            local_pixels = img[row - 1:row + 2, col - 1:col + 2]

            # apply the vertical filter
            vertical_transformed_pixels = vertical_filter * local_pixels
            # remap the vertical score
            vertical_score = vertical_transformed_pixels.sum() / 4

            # apply the horizontal filter
            horizontal_transformed_pixels = horizontal_filter * local_pixels
            # remap the horizontal score
            horizontal_score = horizontal_transformed_pixels.sum() / 4

            # combine the horizontal and vertical scores into a total edge score
            edge_score = (vertical_score ** 2 + horizontal_score ** 2) ** .5

            # insert this edge score into the edges image
            edges_img[row, col] = edge_score * 3

    # remap the values in the 0-1 range in case they went out of bounds
    edges_img = edges_img / edges_img.max()

    return edges_img


def angular_distance_grid(angle_center, s_angle):
    angle_distance_grid = angle_center - s_angle
    angle_distance_grid = (angle_distance_grid + np.pi) % (2 * np.pi) - np.pi
    return angle_distance_grid


def multiply_kernels(kernel_1, kernel_2):
    kernel_prod = kernel_1 * kernel_2
    kernel_prod = kernel_prod / np.sum(kernel_prod[:])
    return kernel_prod


def generate_regular_rectangular_array(step_x, step_y, len_x, len_y, rotation_deg, rotation_origin=[0, 0]):
    "return x_st, y_st"
    rotation_rad = - rotation_deg * np.pi / 180
    x_axis = np.arange(0, len_x + step_x, step_x)
    y_axis = np.arange(0, len_y + step_y, step_y)
    x_mesh, y_mesh = np.meshgrid(x_axis, y_axis)
    x_mesh_shift = x_mesh - rotation_origin[0]
    y_mesh_shift = y_mesh - rotation_origin[1]

    x_mesh_rot = np.cos(rotation_rad) * x_mesh_shift + np.sin(rotation_rad) * y_mesh_shift + rotation_origin[0]
    y_mesh_rot = - np.sin(rotation_rad) * x_mesh_shift + np.cos(rotation_rad) * y_mesh_shift + rotation_origin[1]
    x_st = x_mesh_rot.flatten(order='F')
    y_st = y_mesh_rot.flatten(order='F')
    return x_st, y_st


def generate_regular_circular_array(step_x, step_y, radius):
    "return x_st, y_st"
    len_x = 2 * radius
    len_y = 2 * radius
    x_st, y_st = generate_regular_rectangular_array(step_x, step_y, len_x, len_y, rotation_deg=0,
                                                    rotation_origin=[0, 0])
    x_mean = np.mean(x_st)
    y_mean = np.mean(y_st)
    r_st = np.sqrt((x_st - x_mean) ** 2 + (y_st - y_mean) ** 2)
    bool_cond_circular = (r_st <= radius)
    x_st = x_st[bool_cond_circular]
    y_st = y_st[bool_cond_circular]
    return x_st, y_st


def generate_syntetic_arf(x_st, y_st, smax_arf, ds, freq):
    sx_arf, sy_arf, sx_grid_arf, sy_grid_arf, _, _ = generate_slowness_grid(smax_arf, ds)
    ns_y, ns_x = sx_grid_arf.shape
    arf = 0 * sx_grid_arf
    arf = arf.astype('complex128')

    if ns_y * ns_x > len(x_st):
        for (x, y) in zip(x_st, y_st):
            arf += np.exp(-1j * 2 * np.pi * freq * (x * sx_grid_arf + y * sy_grid_arf))
    else:
        for (i_y, s_y) in enumerate(sy_arf):
            arf_y_term = y_st * s_y
            for (i_x, s_x) in enumerate(sx_arf):
                arf_x_term = x_st * s_x
                arf[i_y, i_x] = np.sum(np.exp(-1j * 2 * np.pi * freq * (arf_x_term + arf_y_term)))

    arf = np.abs(arf) ** 2
    arf = arf / np.max(arf.flatten())

    return arf, sx_arf, sy_arf, sx_grid_arf, sy_grid_arf


def generate_slowness_grid(smax, ds):
    sx = np.arange(-smax, smax + ds, ds)
    sy = np.arange(-smax, smax + ds, ds)
    sx_grid, sy_grid = np.meshgrid(sx, sy)
    s_norm = np.sqrt(sx_grid ** 2 + sy_grid ** 2)
    s_angle = np.arctan2(sy_grid, sx_grid)
    return sx, sy, sx_grid, sy_grid, s_angle, s_norm


def generate_slowness_grid_polar(smin, smax, ds, dtheta):
    theta = np.arange(0, 2 * np.pi, dtheta)
    s = np.arange(smin, smax + ds, ds)
    theta_grid, s_grid = np.meshgrid(theta, s)
    sx_polar = s_grid * np.cos(theta_grid)
    sy_polar = s_grid * np.sin(theta_grid)
    return theta, s, theta_grid, s_grid, sx_polar, sy_polar


def generate_synthetic_distribution(amplitude_list, s_center_list, s_width_list, theta_center_list_deg,
                                    theta_width_list_deg,
                                    s_angle, s_norm):
    "generate a wavefield scenario in sx, sy plane"
    beam = 0 * s_angle
    for i in range(len(amplitude_list)):
        beam_i = amplitude_list[i] * generate_elemetary_wave(s_center_list[i], s_width_list[i],
                                                             theta_center_list_deg[i], theta_width_list_deg[i],
                                                             s_angle, s_norm)
        beam += beam_i
    return beam


def generate_synthetic_beam(energy_true, arf):
    "convolve energy by arf"
    # beam_synth = np.abs(convolve(energy_true, arf, mode='same'))
    beam_synth = convolve(energy_true, arf, mode='same')
    return beam_synth


def generate_elemetary_wave(s_center, s_width, theta_center_deg, theta_width_deg, s_angle, s_norm):
    "return a wavefield scenario in sx, sy plane"
    theta_center_rad = theta_center_deg * np.pi / 180
    theta_width_rad = theta_width_deg * np.pi / 180
    beam = 0 * s_norm
    i_range = (np.abs(s_norm - s_center) <= s_width) & \
              (np.abs(angular_distance_grid(theta_center_rad, s_angle)) <= theta_width_rad)
    beam[i_range] = 1
    return beam


def costfun_reg(p, d_matrix, b_vector, reg_coef, reg_matrix, power=2):
    if power == 1:
        reg_term = np.sum(np.abs(np.matmul(reg_matrix, p)))
    else:
        reg_term = np.sum(np.matmul(reg_matrix, p) ** power)
    cost = 0.5 * np.sum((np.matmul(d_matrix, p) - b_vector) ** 2) + (reg_coef * reg_term)
    return cost


def grad_costfun_reg(p, d_matrix, b_vector, reg_coef, reg_matrix, power=2):
    if power < 1:
        raise Exception('for now lp norm is unstable (division by small values in the denominator')
    if power == 1:
        reg_term = power * np.matmul(reg_matrix.T, np.sign(np.matmul(reg_matrix, p)))
    else:
        reg_term = power * np.matmul(reg_matrix.T, np.matmul(reg_matrix, p) ** (power - 1))
    grad_cost = np.matmul(d_matrix.T, (np.matmul(d_matrix, p) - b_vector)) + (reg_coef * reg_term)
    return grad_cost


def csiszar_i_divergence(g_true, g_est, thresh=0.001, reg_coef=0, f=None):
    i_mat = g_true * np.log(g_true / g_est) + (g_est - g_true)
    i_mat[g_est < np.quantile(g_est[:], thresh)] = 0
    i_mat[np.isnan(i_mat)] = 0
    i_sum = np.sum(i_mat[:])
    if reg_coef > 0:
        i_sum += reg_coef * np.sum(f[:] ** 2)
    return i_sum


def compute_rlc_corr_term(arf, f, g, m, alpha, reg_coef=0, water_level=0, alpha_cut=0.1):
    w = 1. / (alpha + reg_coef * f)
    w[alpha < 0.1] = 0
    conv_denum = compute_arf_action(arf, f)
    # conv_denum = convolve(f, arf, mode='same')
    # conv_denum = np.fft.ifftshift(np.real(np.fft.ifft2(np.fft.fft2(arf) * np.fft.fft2(f))))   # convolution of P with O in the denum (Gal et al. 2016 eq 9)
    fract = g / (conv_denum + water_level)
    conv_full = compute_arf_action(arf, m * fract, type='corr')
    # conv_full = convolve(m * fract, arf, mode='same')
    # conv_full = np.fft.ifftshift(np.real(np.fft.ifft2(np.fft.fft2(arf) * np.fft.fft2(fract))))   # convolution in the num (Gal et al. 2016 eq 9)
    corr_term = w * conv_full
    # corr_term[corr_term <= 0] = 1
    return corr_term


def compute_arf_action(arf_in, x_in, type='conv'):
    if type == 'conv':
        y_out = convolve(x_in, arf_in, mode='same')
    elif type == 'corr':
        y_out = correlate(x_in, arf_in, mode='same')
    return y_out


def slowness_muting_taper(s_max, s_norm, water_level, taper_width=0.5):
    range_ones = (s_norm > (1 + taper_width) * s_max)
    range_transition = (s_norm >= s_max) & (s_norm <= (1 + taper_width) * s_max)
    transition_width = taper_width * s_max
    coef_in_sine = np.pi / 2 / transition_width
    s_kernel_mute = np.zeros_like(s_norm) + water_level
    s_kernel_mute[range_transition] = (water_level * (
            1 - np.sin(coef_in_sine * (s_norm[range_transition] - s_max)) ** 2) +
                                       np.sin(coef_in_sine * (s_norm[range_transition] - s_max)) ** 2)
    s_kernel_mute[range_ones] = 1
    return s_kernel_mute


def update_misfits(arf_initial, f, g_for_cost_evol, nonpositive_arf=False, reg_coef=0):
    g_est = compute_arf_action(arf_initial, f)
    if nonpositive_arf:
        g_shift = min(0, min(np.min(g_for_cost_evol), np.min(g_est)))
    else:
        g_shift = 0
    cost_new = csiszar_i_divergence(g_for_cost_evol - (1.01 * g_shift), g_est - (1.01 * g_shift),
                                    f=f, reg_coef=reg_coef)
    rms_new = np.sum((g_est[:] - g_for_cost_evol[:]) ** 2 / np.sum(g_for_cost_evol[:] ** 2))
    return cost_new, rms_new, g_est


# final plot function
def plot_results(beam, beam_deconv, beam_synth, beam_est, sx_beam, sy_beam, sx_arf, sy_arf, arf, rms_hist):
    fig, ax_list = plt.subplots(3, 2, figsize=(10, 10 * 3 / 2))

    vmin_beam = min(np.min(beam[:]), np.min(beam_deconv[:]))
    vmax_beam = max(np.max(beam[:]), np.max(beam_deconv[:]))
    h = ax_list[0, 0].pcolormesh(sx_beam, sy_beam, beam,
                                 vmin=vmin_beam,
                                 vmax=vmax_beam)
    ax_list[0, 0].set_title('Input power')
    plt.colorbar(h, ax=ax_list[0, 0])
    ax_list[0, 0].set_aspect(1.)

    h = ax_list[0, 1].pcolormesh(sx_beam, sy_beam, beam_deconv,
                                 vmin=vmin_beam,
                                 vmax=vmax_beam)
    ax_list[0, 1].set_title('Deconv. result')
    plt.colorbar(h, ax=ax_list[0, 1])
    ax_list[0, 1].set_aspect(1.)

    vmin_beam_reconst = min(np.min(beam_synth[:]), np.min(beam_est[:]))
    vmax_beam_reconst = max(np.max(beam_synth[:]), np.max(beam_est[:]))

    h = ax_list[1, 0].pcolormesh(sx_beam, sy_beam, beam_synth,
                                 vmin=vmin_beam_reconst,
                                 vmax=vmax_beam_reconst)
    ax_list[1, 0].set_title('Synthetic beam')
    plt.colorbar(h, ax=ax_list[1, 0])
    ax_list[1, 0].set_aspect(1.)

    h = ax_list[1, 1].pcolormesh(sx_beam, sy_beam, beam_est,
                                 vmin=vmin_beam_reconst,
                                 vmax=vmax_beam_reconst)
    ax_list[1, 1].set_title('Reconst. beam')
    plt.colorbar(h, ax=ax_list[1, 1])
    ax_list[1, 1].set_aspect(1.)

    h = ax_list[2, 0].pcolormesh(sx_arf, sy_arf, arf)
    ax_list[2, 0].set_title('real(Full ARF)')
    plt.colorbar(h, ax=ax_list[2, 0])
    ax_list[2, 0].set_aspect(1.)

    h = ax_list[2, 1].semilogy(rms_hist)
    ax_list[2, 1].set_title('RMS error evol.')
    ax_list[2, 1].set_xlabel('stages')
    ax_list[2, 1].set_ylim(1e-2, 1)
    ax_list[2, 1].grid(True)


def plot_beam_column(vmin_beam, vmax_beam, vmin_beam_reconst, vmax_beam_reconst,
                     beam_deconv, beam_est, sx_beam, sy_beam, rms_hist,
                     col_num, col_title,
                     fig=None, ax_list=None, rms_plot_bounds=[1e-2, 1], str_letter=''):
    if (fig is None) | (ax_list is None):
        fig, ax_list = plt.subplots(3, 2, figsize=(10, 10 * 3 / 2))
        col_num = 1

    if len(str_letter) > 1:
        postfix = str_letter[1:]
    else:
        postfix = ''

    smax = np.round(np.max(sx_beam[:]))

    h = ax_list[0, col_num].pcolormesh(sx_beam, sy_beam, beam_deconv,
                                       vmin=vmin_beam,
                                       vmax=vmax_beam)
    if len(str_letter) > 0:
        ax_list[0, col_num].set_title(chr(ord(str_letter[0]) + 0) + postfix + 'Deconv. ' + col_title)
    else:
        ax_list[0, col_num].set_title('Deconv. ' + col_title)
    plt.colorbar(h, ax=ax_list[0, col_num])
    ax_list[0, col_num].set_aspect(1.)
    # ax_list[0, col_num].set_xlabel('sx (s/km)')
    # ax_list[0, col_num].set_ylabel('sy (s/km)')
    ax_list[0, col_num].set_xticks(np.arange(-smax, 1.001 * smax))
    ax_list[0, col_num].set_yticks(np.arange(-smax, 1.001 * smax))

    h = ax_list[1, col_num].pcolormesh(sx_beam, sy_beam, beam_est,
                                       vmin=vmin_beam_reconst,
                                       vmax=vmax_beam_reconst)
    if len(str_letter) > 0:
        ax_list[1, col_num].set_title(chr(ord(str_letter[0]) + 1) + postfix + 'Reconst. beam \n')
    else:
        ax_list[1, col_num].set_title('Reconst. beam \n')
    plt.colorbar(h, ax=ax_list[1, col_num])
    ax_list[1, col_num].set_aspect(1.)
    ax_list[1, col_num].set_xlabel('sx (s/km)')
    # ax_list[0, col_num].set_ylabel('sy (s/km)')
    ax_list[1, col_num].set_xticks(np.arange(-smax, 1.001 * smax))
    ax_list[1, col_num].set_yticks(np.arange(-smax, 1.001 * smax))

    if len(rms_hist) > 1:
        h = ax_list[2, col_num].semilogy(rms_hist)
    else:
        h = ax_list[2, col_num].semilogy(rms_hist, marker='o')
    if len(str_letter) > 0:
        ax_list[2, col_num].set_title(chr(ord(str_letter[0]) + 2) + postfix + 'RMS error evol.')
    else:
        ax_list[2, col_num].set_title('RMS error evol.')
    ax_list[2, col_num].set_xlabel('stages or iterations')
    ax_list[2, col_num].set_ylim(rms_plot_bounds)
    ax_list[2, col_num].grid(True)

    return fig, ax_list


def plot_beam_column_4_lines(vmin_beam, vmax_beam, vmin_beam_reconst, vmax_beam_reconst,
                             beam_deconv, beam_est, beam_input, sx_beam, sy_beam, rms_hist,
                             col_num, col_title,
                             fig=None, ax_list=None, rms_plot_bounds=[1e-2, 1], str_letter=''):
    if (fig is None) | (ax_list is None):
        fig, ax_list = plt.subplots(3, 2, figsize=(10, 10 * 3 / 2))
        col_num = 1

    if len(str_letter) > 1:
        postfix = str_letter[1:]
    else:
        postfix = ''

    smax = np.round(np.max(sx_beam[:]))

    h = ax_list[0, col_num].pcolormesh(sx_beam, sy_beam, beam_deconv,
                                       vmin=vmin_beam,
                                       vmax=vmax_beam)
    if len(str_letter) > 0:
        ax_list[0, col_num].set_title(chr(ord(str_letter[0]) + 0) + postfix + 'Deconv. ' + col_title)
    else:
        ax_list[0, col_num].set_title('Deconv. ' + col_title)
    plt.colorbar(h, ax=ax_list[0, col_num])
    ax_list[0, col_num].set_aspect(1.)
    if col_num == 0:
        # ax_list[0, col_num].set_xlabel('sx (s/km)')
        ax_list[0, col_num].set_ylabel('sy (s/km)')
    ax_list[0, col_num].set_xticks(np.arange(-smax, 1.001 * smax))
    ax_list[0, col_num].set_yticks(np.arange(-smax, 1.001 * smax))

    h = ax_list[1, col_num].pcolormesh(sx_beam, sy_beam, beam_est,
                                       vmin=vmin_beam_reconst,
                                       vmax=vmax_beam_reconst)
    if len(str_letter) > 0:
        ax_list[1, col_num].set_title(chr(ord(str_letter[0]) + 1) + postfix + 'Reconst. beam \n')
    else:
        ax_list[1, col_num].set_title('Reconst. beam \n')
    plt.colorbar(h, ax=ax_list[1, col_num])
    ax_list[1, col_num].set_aspect(1.)
    if col_num == 0:
        # ax_list[1, col_num].set_xlabel('sx (s/km)')
        ax_list[1, col_num].set_ylabel('sy (s/km)')
    ax_list[1, col_num].set_xticks(np.arange(-smax, 1.001 * smax))
    ax_list[1, col_num].set_yticks(np.arange(-smax, 1.001 * smax))

    h = ax_list[2, col_num].pcolormesh(sx_beam, sy_beam, beam_input,
                                       vmin=vmin_beam_reconst,
                                       vmax=vmax_beam_reconst)
    if len(str_letter) > 0:
        ax_list[2, col_num].set_title(chr(ord(str_letter[0]) + 2) + postfix + 'Input. beam \n')
    else:
        ax_list[2, col_num].set_title('Input. beam \n')
    plt.colorbar(h, ax=ax_list[2, col_num])
    ax_list[2, col_num].set_aspect(1.)
    ax_list[2, col_num].set_xlabel('sx (s/km)')
    if col_num == 0:
        ax_list[2, col_num].set_ylabel('sy (s/km)')
    ax_list[2, col_num].set_xticks(np.arange(-smax, 1.001 * smax))
    ax_list[2, col_num].set_yticks(np.arange(-smax, 1.001 * smax))

    if len(rms_hist) > 1:
        h = ax_list[3, col_num].semilogy(rms_hist)
    else:
        h = ax_list[3, col_num].semilogy(rms_hist, marker='o')
    if len(str_letter) > 0:
        ax_list[3, col_num].set_title(chr(ord(str_letter[0]) + 3) + postfix + 'RMS error evol.')
    else:
        ax_list[3, col_num].set_title('RMS error evol.')
    ax_list[3, col_num].set_xlabel('iterations')
    ax_list[3, col_num].set_ylim(rms_plot_bounds)
    ax_list[3, col_num].grid(True)

    return fig, ax_list


# final plot function
def plot_arf_column(vmin_beam, vmax_beam, vmin_beam_reconst, vmax_beam_reconst,
                    beam, beam_synth, sx_beam, sy_beam, sx_arf, sy_arf, arf, col_num=0,
                    str_beam='Synthetic beam', fig=None, ax_list=None, str_letter=''):
    if (fig is None) | (ax_list is None):
        fig, ax_list = plt.subplots(3, 2, figsize=(10, 10 * 3 / 2))
        col_num = 0

    smax_beam = np.round(np.max(sx_beam[:]))
    smax_arf = np.round(np.max(sx_arf[:]))

    h = ax_list[0, col_num].pcolormesh(sx_beam, sy_beam, beam,
                                       vmin=vmin_beam,
                                       vmax=vmax_beam)
    if len(str_letter) > 1:
        postfix = str_letter[1:]
    else:
        postfix = ''
    if len(str_letter) > 0:
        ax_list[0, col_num].set_title(chr(ord(str_letter[0]) + 0) + postfix + 'Input power')
    else:
        ax_list[0, col_num].set_title('Input power')
    plt.colorbar(h, ax=ax_list[0, col_num])
    ax_list[0, col_num].set_aspect(1.)
    # ax_list[0, col_num].set_xlabel('sx (s/km)')
    ax_list[0, col_num].set_ylabel('sy (s/km)')
    ax_list[0, col_num].set_xticks(np.arange(-smax_beam, 1.001 * smax_beam))
    ax_list[0, col_num].set_yticks(np.arange(-smax_beam, 1.001 * smax_beam))

    h = ax_list[1, col_num].pcolormesh(sx_beam, sy_beam, beam_synth,
                                       vmin=vmin_beam_reconst,
                                       vmax=vmax_beam_reconst)
    if len(str_letter) > 0:
        ax_list[1, col_num].set_title(chr(ord(str_letter[0]) + 1) + postfix + str_beam)
    else:
        ax_list[1, col_num].set_title(str_beam)
    plt.colorbar(h, ax=ax_list[1, col_num])
    ax_list[1, col_num].set_aspect(1.)
    # ax_list[1, col_num].set_xlabel('sx (s/km)')
    ax_list[1, col_num].set_ylabel('sy (s/km)')
    ax_list[1, col_num].set_xticks(np.arange(-smax_beam, 1.001 * smax_beam))
    ax_list[1, col_num].set_yticks(np.arange(-smax_beam, 1.001 * smax_beam))

    h = ax_list[2, col_num].pcolormesh(sx_arf, sy_arf, arf)
    if len(str_letter) > 0:
        ax_list[2, col_num].set_title(chr(ord(str_letter[0]) + 2) + postfix + 'Full ARF')
    else:
        ax_list[2, col_num].set_title('Full ARF')
    plt.colorbar(h, ax=ax_list[2, col_num])
    ax_list[2, col_num].set_aspect(1.)
    ax_list[2, col_num].set_aspect(1.)
    ax_list[2, col_num].set_xlabel('sx (s/km)')
    ax_list[2, col_num].set_ylabel('sy (s/km)')
    ax_list[2, col_num].set_xticks(np.arange(-smax_arf, 1.001 * smax_arf, 2.))
    ax_list[2, col_num].set_yticks(np.arange(-smax_arf, 1.001 * smax_arf, 2.))

    return fig, ax_list


def plot_end_of_stage_rlc(sx, sy, f, i_iter, i_slowness_stage, rms_hist_this_stage, fig_list_rms_plots_per_j,
                          ax_list_rms_plots_per_j,
                          j=None, circle_stage=False, path_save=None):
    if circle_stage:
        str_stage = 'slowness stage ' + str(i_slowness_stage) + '\n END of azim. stage 0 (circles)'
        str_stage_filename = 's_stage' + str(i_slowness_stage) + '_endof_stage0'
    else:
        str_stage = 'slowness stage ' + str(i_slowness_stage) + '\n END of azim. stage ' + str(j + 1)
        str_stage_filename = 's_stage' + str(i_slowness_stage) + '_endof_stage' + str(j + 1)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    h_plot = ax.pcolormesh(sx, sy, f)
    ax.set_aspect(1.)
    ax.set_xlabel('sx (s/km)')
    ax.set_ylabel('sy (s/km)')
    ax.set_xticks(np.arange(-2, 2.1))
    ax.set_yticks(np.arange(-2, 2.1))
    ax.grid('on')
    plt.colorbar(h_plot, ax=ax)
    ax.set_title('solution f \n iteration ' + str(i_iter) + '\n' + str_stage)
    if path_save is not None:
        str_file = 'sol_f_' + str_stage_filename + 'iter_' + str(i_iter) + '.png'
        path_save_file = Path(path_save).joinpath(str_file)
        plt.savefig(path_save_file)
        plt.close(fig)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    ax.semilogy(rms_hist_this_stage)
    ax.set_xlabel('iterations')
    ax.set_ylabel('relative RMS')
    ax.grid(True)
    ax.set_ylim([1e-3, 1])
    ax.set_title('RMS error evol. \n iteration ' + str(i_iter) + '\n' + str_stage)
    ax_list_rms_plots_per_j.append(ax)
    fig_list_rms_plots_per_j.append(fig)
    return fig_list_rms_plots_per_j, ax_list_rms_plots_per_j


def plots_rms_rescale_and_save(fig_list_rms_plots_per_j, ax_list_rms_plots_per_j, i_slowness_stage, n_iter_tot,
                               path_save=None):
    for i, (ax, fig) in enumerate(zip(ax_list_rms_plots_per_j, fig_list_rms_plots_per_j)):
        ax.set_xlim(0, n_iter_tot)
        if path_save is not None:
            str_file = 'rms_evol_s_stage' + str(i_slowness_stage) + \
                       '_stage' + str(i) + '.png'
            path_save_file = Path(path_save).joinpath(str_file)
            fig.savefig(path_save_file)
    for fig in fig_list_rms_plots_per_j:
        plt.close(fig)


def plot_intermediate_sequence_rlc(sx, sy, f0, f, corr_term, corr_term_copy, i_iter, i_slowness_stage,
                                   angle_domain_centers_list, j, angle_step_j, smax,
                                   circle_list, s_kernel_plot, angle_kernel_plot, i_s_plot,
                                   s_kernels, full_kernels, circle_stage=False, path_save=None,
                                   random_angle_at_each_iteration=False):
    if circle_stage:
        str_stage = 'slowness stage ' + str(i_slowness_stage) + '\n azim. stage 0 (circles)'
        str_stage_filename = 's_stage' + str(i_slowness_stage) + '_stage0'
    else:
        str_stage = 'slowness stage ' + str(i_slowness_stage) + '\n azim. stage ' + str(j + 1)
        str_stage_filename = 's_stage' + str(i_slowness_stage) + '_stage' + str(j + 1)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    h_plot = ax.pcolormesh(sx, sy, f0)
    ax.set_aspect(1.)
    ax.set_xlabel('sx (s/km)')
    ax.set_ylabel('sy (s/km)')
    ax.set_xticks(np.arange(-smax, 1.001 * smax))
    ax.set_yticks(np.arange(-smax, 1.001 * smax))
    ax.grid('on')
    plt.colorbar(h_plot, ax=ax)
    ax.set_title('solution f \n iteration ' + str(i_iter - 1) + '\n' + str_stage)

    if path_save is not None:
        str_file = 'sol_f_' + str_stage_filename + 'iter' + str(i_iter - 1) + '.png'
        path_save_file = Path(path_save).joinpath(str_file)
        plt.savefig(path_save_file)
        plt.close(fig)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    h_plot = ax.pcolormesh(sx, sy, corr_term)
    ax.set_aspect(1.)
    ax.set_xlabel('sx (s/km)')
    ax.set_ylabel('sy (s/km)')
    ax.set_xticks(np.arange(-smax, 1.001 * smax))
    ax.set_yticks(np.arange(-smax, 1.001 * smax))
    if not circle_stage:
        for angle_center in angle_domain_centers_list[j]:
            x1 = - smax * np.cos(angle_center - angle_step_j / 2)
            x2 = smax * np.cos(angle_center - angle_step_j / 2)
            y1 = - smax * np.sin(angle_center - angle_step_j / 2)
            y2 = smax * np.sin(angle_center - angle_step_j / 2)
            ax.plot([x1, x2], [y1, y2], c='w', linewidth=0.1)
    for circle_patch in circle_list:
        ax.add_patch(circle_patch)
    ax.scatter(s_kernel_plot * np.cos(angle_kernel_plot), s_kernel_plot * np.cos(angle_kernel_plot),
               facecolors='none', edgecolors='r')
    ax.set_xlim(-smax, smax)
    ax.set_ylim(-smax, smax)
    plt.colorbar(h_plot, ax=ax)
    ax.set_title('correction term (raw) \n iteration ' + str(i_iter - 1) + '\n' + str_stage)
    if path_save is not None:
        str_file = 'corr_term_raw_' + str_stage_filename + 'iter' + str(i_iter - 1) + '.png'
        path_save_file = Path(path_save).joinpath(str_file)
        plt.savefig(path_save_file)
        plt.close(fig)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    h_plot = ax.pcolormesh(sx, sy, corr_term_copy)
    ax.set_aspect(1.)
    ax.set_xlabel('sx (s/km)')
    ax.set_ylabel('sy (s/km)')
    ax.set_xticks(np.arange(-smax, 1.001 * smax))
    ax.set_yticks(np.arange(-smax, 1.001 * smax))
    if not circle_stage:
        for angle_center in angle_domain_centers_list[j]:
            x1 = - smax * np.cos(angle_center - angle_step_j / 2)
            x2 = smax * np.cos(angle_center - angle_step_j / 2)
            y1 = - smax * np.sin(angle_center - angle_step_j / 2)
            y2 = smax * np.sin(angle_center - angle_step_j / 2)
            ax.plot([x1, x2], [y1, y2], c='w', linewidth=0.1)
    for circle_patch in circle_list:
        ax.add_patch(circle_patch)
    ax.scatter(s_kernel_plot * np.cos(angle_kernel_plot), s_kernel_plot * np.cos(angle_kernel_plot),
               facecolors='none', edgecolors='r')
    ax.set_xlim(-smax, smax)
    ax.set_ylim(-smax, smax)
    plt.colorbar(h_plot, ax=ax)
    ax.set_title('correction term (averaged) \n iteration ' + str(i_iter - 1) + '\n' + str_stage)
    if path_save is not None:
        str_file = 'corr_term_aver_' + str_stage_filename + 'iter' + str(i_iter - 1) + '.png'
        path_save_file = Path(path_save).joinpath(str_file)
        plt.savefig(path_save_file)
        plt.close(fig)

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))
    h_plot = ax.pcolormesh(sx, sy, f)
    ax.set_aspect(1.)
    ax.set_xlabel('sx (s/km)')
    ax.set_ylabel('sy (s/km)')
    ax.set_xticks(np.arange(-smax, 1.001 * smax))
    ax.set_yticks(np.arange(-smax, 1.001 * smax))
    ax.grid('on')
    plt.colorbar(h_plot, ax=ax)
    ax.set_title('solution f \n iteration ' + str(i_iter) + '\n' + str_stage)
    if path_save is not None:
        str_file = 'sol_f_' + str_stage_filename + 'iter' + str(i_iter) + '.png'
        path_save_file = Path(path_save).joinpath(str_file)
        plt.savefig(path_save_file)
        plt.close(fig)

    if not random_angle_at_each_iteration:
        if circle_stage:
            kernel_to_plot = s_kernels[i_s_plot]
        else:
            k_kernel = np.min(np.where((angle_domain_centers_list[j] + angle_step_j / 2) >
                                       angle_kernel_plot)[0])
            kernel_to_plot = full_kernels[i_s_plot][j][k_kernel]
        fig, ax = plt.subplots(1, 1, figsize=(5, 5))
        h_plot = ax.pcolormesh(sx, sy, kernel_to_plot)
        ax.scatter(s_kernel_plot * np.cos(angle_kernel_plot), s_kernel_plot * np.cos(angle_kernel_plot),
                   facecolors='none', edgecolors='r')
        ax.set_aspect(1.)
        ax.set_xlabel('sx (s/km)')
        ax.set_ylabel('sy (s/km)')
        ax.set_xticks(np.arange(-smax, 1.001 * smax))
        ax.set_yticks(np.arange(-smax, 1.001 * smax))
        ax.grid('on')
        plt.colorbar(h_plot, ax=ax)
        ax.set_title('kernel for s=' + str(s_kernel_plot) + ' s/km at ' +
                     str(angle_kernel_plot / np.pi * 180) + '°' + '\n' + str_stage)
        if path_save is not None:
            str_file = 'kernel_s' + "{:.2f}".format(s_kernel_plot) + '_angle' + "{:.2f}".format(angle_kernel_plot) + \
                       '_' + str_stage_filename + 'iter' + str(i_iter - 1) + '.png'
            path_save_file = Path(path_save).joinpath(str_file)
            plt.savefig(path_save_file)
            plt.close(fig)


########################################################################


def deconvolve_beamformers(arf, beam, beamforming_params):
    vmin = beamforming_params.vmin  # km/s
    n_iter_max = beamforming_params.n_iter_max  # iterations
    angle_step_min = beamforming_params.angle_step_min  # degrees
    angle_width_start = beamforming_params.angle_width_start  # degrees
    theta_overlap_kernel = beamforming_params.theta_overlap_kernel
    slowness_width_ratio_to_ds = beamforming_params.slowness_width_ratio_to_ds  # step of gaussian slowness kernels
    slowness_step_ratio_to_ds = beamforming_params.slowness_step_ratio_to_ds  # width (std) of gaussian slowness kernels
    # shifted at each iteration so that there are no region-boundary artifacts.
    stop_crit_rel = beamforming_params.stop_crit_rel  # criterion of I(n+1)/I(n) for stopping iterations at each stage,with I the Csiszar cost function
    sx = beamforming_params.get_xaxis()
    sy = beamforming_params.get_yaxis()

    beam_deconv, g_est, cost_hist, rms_hist, f_by_stages = deconv_rlc_stepwise_circles_v_polar(
        beam,
        arf,
        sx,
        sy,
        vmin,
        n_iter=n_iter_max,
        slowness_width_ratio_to_ds=slowness_width_ratio_to_ds,
        slowness_step_ratio_to_ds=slowness_step_ratio_to_ds,
        angle_step_min=angle_step_min,
        angle_width_start=angle_width_start,
        theta_overlap_kernel=theta_overlap_kernel,
        stop_crit_rel=stop_crit_rel,
        stop_crit_rms=0,
        verbose=False,
        standard_rlc=False,
        f_start=None,
        f_sum=None,
        s_stages=None,
        water_level_rms_pct=0,
        lambda_herve=0,
        reg_coef=0,
        )

    return beam_deconv


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


def calculate_beamforming_results( # noqa: max-complexity: 22
        beamforming_params_collection: Collection[BeamformingParams],
        timespan: Timespan,
        datachunks: Tuple[Datachunk, ...],

) -> List[BeamformingResult]:
    """filldocs
    """

    logger.debug(f"Loading seismic files for timespan {timespan}")

    st = Stream()

    for datachunk in datachunks:
        if not isinstance(datachunk.component, Component):
            raise SubobjectNotLoadedError('You should load Component together with the Datachunk.')
        single_st = datachunk.load_data()
        single_st[0].stats.coordinates = AttribDict({
            'latitude': datachunk.component.lat,
            'elevation': datachunk.component.elevation / 1000,
            'longitude': datachunk.component.lon})
        st.extend(single_st)

    logger.info(f"For Timespan {timespan} there are {len(st)} traces loaded.")

    logger.debug("Checking for subsample starttime error.")
    st = validate_and_fix_subsample_starttime_error(st)

    logger.debug(f"Preparing stream metadata for beamforming for timespan {timespan}")
    first_starttime = min([tr.stats.starttime for tr in st])
    first_endtime = min([tr.stats.endtime for tr in st])
    time_vector = [pd.Timestamp.utcfromtimestamp(x).to_datetime64() for x in st[0].times('timestamp')]

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

        array_proc_kwargs = dict(
            # slowness grid: X min, X max, Y min, Y max, Slow Step
            sll_x=beamforming_params.slowness_x_min,
            slm_x=beamforming_params.slowness_x_max,
            sll_y=beamforming_params.slowness_y_min,
            slm_y=beamforming_params.slowness_y_max,
            sl_s=beamforming_params.slowness_step,
            # sliding window properties
            win_len=beamforming_params.window_length,
            win_frac=beamforming_params.window_fraction,
            # frequency properties
            frqlow=beamforming_params.min_freq,
            frqhigh=beamforming_params.max_freq,
            prewhiten=int(beamforming_params.prewhiten),
            # restrict output
            semb_thres=beamforming_params.semblance_threshold,
            vel_thres=beamforming_params.velocity_threshold,
            timestamp='julsec',
            stime=first_starttime,
            etime=first_endtime,
            method=beamforming_params.method,
            save_arf=beamforming_params.save_all_arf,
            store=bk.save_beamformers,
        )

        # if beamforming_params.perform_deconvolution_all or \
        # beamforming_params.perform_deconvolution_average:
        arf_enlargement_ratio = beamforming_params.arf_enlarge_ratio
        array_proc_kwargs['sll_x_arf'] = arf_enlargement_ratio * array_proc_kwargs['sll_x']
        array_proc_kwargs['slm_x_arf'] = arf_enlargement_ratio * array_proc_kwargs['slm_x']
        array_proc_kwargs['sll_y_arf'] = arf_enlargement_ratio * array_proc_kwargs['sll_y']
        array_proc_kwargs['slm_y_arf'] = arf_enlargement_ratio * array_proc_kwargs['slm_y']
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
                        "No deconvolution performed for elementary windows! Activate beamforming_params.save_all_arf")
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
                        "No deconvolution performed for average beams ! Activate beamforming_params.save_average_arf")
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
            raise ObspyError(f"Ecountered error while running beamforming routine. "
                             f"Error happenned for timespan: {timespan}, beamform_params: {beamforming_params} "
                             f"Error was: {e}")

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

        self.average_arf: Any = None
        self.average_abspow_deconv: Any = None
        self.average_relpow_deconv: Any = None
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

    def save_beamforming_file(self, params: BeamformingParams, ts: Timespan) -> Optional[BeamformingFile]: # noqa: max-complexity: 19
        bf = BeamformingFile()
        fpath = bf.find_empty_filepath(ts=ts, params=params)

        res_to_save = dict()

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
                for i, arr in enumerate(self.abs_pows_deconv):
                    res_to_save[f"abs_pow_deconv_{i}"] = arr
            if params.save_all_beamformers_relpower:
                for i, arr in enumerate(self.rel_pows_deconv):
                    res_to_save[f"rel_pow_deconv_{i}"] = arr

        if params.perform_deconvolution_average:
            if params.save_average_beamformer_abspower:
                res_to_save["avg_abs_pow_deconv"] = self.average_abspow_deconv
            if params.save_average_beamformer_relpower:
                res_to_save["avg_rel_pow_deconv"] = self.average_relpow_deconv
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

    @lru_cache
    def get_midtimes(self):  # -> npt.ArrayLike:
        """filldocs"""
        return np.array([self.time_vector[x] for x in self.midtime_samples])

    # def save_beamformers(self, pow_map: npt.ArrayLike, apow_map: npt.ArrayLike, midsample: int) -> None:
    def save_beamformers(self, pow_map, apow_map, midsample: int,
                         arf=None) -> None:
        """
        filldocs

        """
        self.iteration_count += 1

        self.midtime_samples.append(midsample)
        if self.save_relpow:
            self.rel_pows.append(pow_map.copy())
        if self.save_abspow:
            self.abs_pows.append(apow_map.copy())
        # print(self.save_arf)
        if (self.save_arf) and (arf is not None):
            self.arf.append(arf.copy())

    def deconv_all_windows_from_existing_arf(self, beamforming_params):
        for i, (abs_pow, rel_pow, arf) in enumerate(zip(self.abs_pows, self.rel_pows, self.arf)):
            abs_pow_deconv = deconvolve_beamformers(arf, abs_pow, beamforming_params)
            abs_pow_deconv_max = np.max(abs_pow_deconv.flatten())
            rel_pows_max = np.max(rel_pow.flatten())
            rel_pow_deconv = (rel_pows_max / abs_pow_deconv_max) * abs_pow_deconv
            self.abs_pows_deconv.append(abs_pow_deconv)
            self.rel_pows_deconv.append(rel_pow_deconv)

    def calculate_average_arf_beamformer(self):
        """filldocs"""
        if len(self.arf) == 0:
            raise ValueError("There are no data to average in arf . "
                             "Are you sure you used `save_beamformers` method to keep data from beamforming procedure")
        self.average_arf = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.arf:
            self.average_arf = np.add(self.average_arf, arr)
        self.average_arf = self.average_arf / self.iteration_count

    def deconvolve_average_abspower_with_arf(self, beamforming_params):
        """filldocs"""
        self.average_abspow_deconv = deconvolve_beamformers(self.average_arf, self.average_abspow, beamforming_params)

    ####################################

    def deconvolve_average_relpower_with_arf(self, beamforming_params):
        """filldocs"""
        self.average_relpow_deconv = deconvolve_beamformers(self.average_arf, self.average_relpow, beamforming_params)

    ####################################

    def calculate_average_relpower_beamformer(self):
        """filldocs"""
        if self.save_relpow is not True:
            raise ValueError("The `save_relpow` was set to False, data were not kept")
        if len(self.rel_pows) == 0:
            raise ValueError("There are no data to average. "
                             "Are you sure you used `save_beamformers` method to keep data from beamforming procedure")
        self.average_relpow = np.zeros((len(self.xaxis), len(self.yaxis)))
        for arr in self.rel_pows:
            self.average_relpow = np.add(self.average_relpow, arr)
        self.average_relpow = self.average_relpow / self.iteration_count

    def calculate_average_abspower_beamformer(self):
        """filldocs"""
        if self.save_abspow is not True:
            raise ValueError("The `save_abspow` was set to False, data were not kept")
        if len(self.abs_pows) == 0:
            raise ValueError("There are no data to average. "
                             "Are you sure you used `save_beamformers` method to keep data from beamforming procedure")

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
            use_deconv=False
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

        df = _extract_most_significant_subbeams([maxima, ], beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def extract_best_maxima_from_average_abspower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
            use_deconv=False
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

        df = _extract_most_significant_subbeams([maxima, ], beam_portion_threshold)
        df = _calculate_slowness(df=df)
        df = _calculate_azimuth_backazimuth(df=df)

        return df

    def extract_best_maxima_from_all_relpower(
            self,
            neighborhood_size: int,
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
            use_deconv=False
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
            use_deconv=True
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
            bool_use_deconv: bool
    ) -> List[BeamformingPeakAverageAbspower]:
        df = self.extract_best_maxima_from_average_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv
        )
        # print('Calculating avg abs pow peaks')
        res = []
        for i, row in df.iterrows():
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
            bool_use_deconv: bool
    ) -> List[BeamformingPeakAverageRelpower]:
        df = self.extract_best_maxima_from_average_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv
        )
        res = []
        for i, row in df.iterrows():
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
            bool_use_deconv: bool
    ) -> List[BeamformingPeakAllAbspower]:
        df = self.extract_best_maxima_from_all_abspower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv
        )
        res = []
        for i, row in df.iterrows():
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
            bool_use_deconv: bool
    ) -> List[BeamformingPeakAllRelpower]:
        df = self.extract_best_maxima_from_all_relpower(
            neighborhood_size=neighborhood_size,
            maxima_threshold=maxima_threshold,
            best_point_count=best_point_count,
            beam_portion_threshold=beam_portion_threshold,
            use_deconv=bool_use_deconv
        )
        res = []
        for i, row in df.iterrows():
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

    df_all = pd.concat(all_maxima).set_index('midtime')
    total_beam = df_all.loc[:, 'amplitude'].groupby(level=0).sum()
    df_all.loc[:, 'beam_proportion'] = df_all.apply(lambda row: row.amplitude / total_beam.loc[row.name], axis=1)
    df_res = df_all.loc[df_all.loc[:, 'beam_proportion'] > beam_portion_threshold, :]
    maximum_points = df_res.groupby(by=['x', 'y']).mean()
    maximum_points['occurence_counts'] = df_res.groupby(by=['x', 'y'])['amplitude'].count()
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
        best_point_count: int
) -> pd.DataFrame:
    data = data.T
    data_max = filters.maximum_filter(data, neighborhood_size)
    maxima = (data == data_max)
    data_min = filters.minimum_filter(data, neighborhood_size)
    diff = ((data_max - data_min) > maxima_threshold)
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
    df = pd.DataFrame(columns=["x", "y", "amplitude"], data=np.vstack([x, y, max_vals]).T, )
    if len(df) == 0:
        raise ValueError("No peaks were found. Adjust neighbourhood_size and maxima_threshold values.")
    df.loc[:, 'midtime'] = time
    df = df.sort_values(by='amplitude', ascending=False)

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
    df.loc[:, 'slowness'] = df.apply(lambda row: np.sqrt(row.x ** 2 + row.y ** 2), axis=1)
    return df


def validate_if_all_beamforming_params_use_same_component_codes(
        params: Collection[BeamformingParams]
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
