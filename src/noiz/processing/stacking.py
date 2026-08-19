# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import numpy as np
from typing import Any, Generator, Collection, List, Optional, Tuple
import numpy.typing as npt
import pandas as pd

from noiz.models import CrosscorrelationCartesian, CrosscorrelationCylindrical, StackingSchema, StackingTimespan
from noiz.processing.timespan import generate_starttimes_endtimes


def _planck_taper(n: int, epsilon: float = 0.05) -> npt.NDArray:
    """Planck-taper window: flat in the middle, exponential rolloff at edges.

    *epsilon* is the fraction tapered on each side (0.05 = 5% each end, 90% flat).
    """
    w = np.ones(n, dtype=np.float64)
    m = max(int(epsilon * n), 1)
    for i in range(1, m):
        z = m * (1.0 / i + 1.0 / (i - m))
        w[i] = 1.0 / (1.0 + np.exp(z))
        w[n - 1 - i] = w[i]
    w[0] = 0.0
    w[n - 1] = 0.0
    return w


def _generate_stacking_timespans(stacking_schema: StackingSchema) -> Generator[StackingTimespan, None, None]:
    """
    Generates StackingTimespan objects based on provided StackingSchema

    :param stacking_schema: StackingSchema on which generation should be based
    :type stacking_schema: StackingSchema
    :return: Generator with all possible StackingTimespans for that schema
    :rtype: Generator[StackingTimespan, None, None]
    """

    timespans = generate_starttimes_endtimes(
        startdate=stacking_schema.starttime,
        enddate=stacking_schema.endtime,
        window_length=pd.Timedelta(stacking_schema.stacking_length),
        window_overlap=pd.Timedelta(stacking_schema.stacking_overlap),
        generate_midtimes=True,
    )

    for starttime, midtime, endtime in zip(*timespans):
        yield StackingTimespan(
            starttime=starttime,
            midtime=midtime,
            endtime=endtime,
            stacking_schema_id=stacking_schema.id,
        )


def do_linear_stack_of_crosscorrelations_cartesian(ccfs: Collection[CrosscorrelationCartesian]) -> npt.ArrayLike:
    """
    Takes a collection of :py:class:`~noiz.models.crosscorrelation.CrosscorrelationCartesian` objects and performs
    a linear stack on all of them.
    Returns raw array with the stack itself.

    :param ccfs: CrosscorrelationCartesians to stack
    :type ccfs: Collection[CrosscorrelationCartesian]
    :return: Array with stacked crosscorrelation_cartesian
    :rtype: np.array
    """
    mean_ccf = np.array([x.ccf for x in ccfs]).mean(axis=0)
    return mean_ccf


def bandpass_filter_ccf(
    data: npt.NDArray,
    sampling_rate: float,
    freq_low: float,
    freq_high: float,
    transition_width_hz: float = 0.1,
) -> npt.NDArray:
    """Zero-phase frequency-domain bandpass with cosine transition edges.

    transition_width_hz is in Hz (capped at 50% of bandwidth).
    Transitions are inside the band.
    """
    n = len(data)
    spectrum = np.fft.rfft(data)
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_rate)

    bw = freq_high - freq_low
    tw = min(transition_width_hz, 0.5 * bw)
    tw = max(tw, 1e-6)

    mask = np.zeros(len(freqs), dtype=data.dtype)
    for i, f in enumerate(freqs):
        if (freq_low + tw) <= f <= (freq_high - tw):
            mask[i] = 1.0
        elif freq_low <= f < (freq_low + tw):
            mask[i] = 0.5 * (1.0 - np.cos(np.pi * (f - freq_low) / tw))
        elif (freq_high - tw) < f <= freq_high:
            mask[i] = 0.5 * (1.0 - np.cos(np.pi * (freq_high - f) / tw))

    filtered = np.fft.irfft(spectrum * mask, n=n)
    return filtered.astype(data.dtype)


def spectral_whitening(
    data: npt.NDArray,
    sampling_rate: float,
    whiten_bands: List[Tuple[float, float]],
    notch_factor: float = 1.0,
    water_level: float = 1e-10,
    taper_fraction: float = 0.05,
    transition_width: float = 0.1,
) -> npt.NDArray:
    """Whiten spectrum within specified frequency bands.

    Within each band, amplitude is divided out (FFT / |FFT|) then rescaled to
    the average amplitude at the band edges, divided by *notch_factor*.
    Cosine transitions taper inward from band edges (never leak outside).
    Assumes input is already time-domain tapered.
    """
    n = len(data)
    spectrum = np.fft.rfft(data)
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_rate)
    amplitude = np.abs(spectrum)
    df = freqs[1] - freqs[0] if len(freqs) > 1 else 1.0

    for f_low, f_high in whiten_bands:
        bw = f_high - f_low
        tw = min(transition_width * bw, bw / 2)

        # Target level: average amplitude in 3-bin neighborhoods just outside edges
        edge_width = max(3 * df, 0.01)
        lo_mask = (freqs >= (f_low - edge_width)) & (freqs < f_low)
        hi_mask = (freqs > f_high) & (freqs <= (f_high + edge_width))
        edge_amps = []
        if np.any(lo_mask):
            edge_amps.append(np.mean(amplitude[lo_mask]))
        if np.any(hi_mask):
            edge_amps.append(np.mean(amplitude[hi_mask]))
        target_amp = (np.mean(edge_amps) if edge_amps else np.mean(amplitude)) / notch_factor
        target_amp = max(target_amp, water_level)

        safe_amp = np.where(amplitude > water_level, amplitude, water_level)
        for i, f in enumerate(freqs):
            if f < f_low or f > f_high:
                continue
            whiten_scale = target_amp / safe_amp[i]
            # Inward cosine taper at edges
            if f < f_low + tw:
                blend = 0.5 * (1.0 - np.cos(np.pi * (f - f_low) / tw))
            elif f > f_high - tw:
                blend = 0.5 * (1.0 - np.cos(np.pi * (f_high - f) / tw))
            else:
                blend = 1.0
            spectrum[i] *= (1.0 - blend) + blend * whiten_scale

    return np.fft.irfft(spectrum, n=n).astype(data.dtype)


def spectral_notch_cut(
    data: npt.NDArray,
    sampling_rate: float,
    notch_bands: List[Tuple[float, float]],
    transition_width_hz: float = 0.1,
) -> npt.NDArray:
    """Brick-wall band-reject: zero spectrum inside notch_bands.

    transition_width_hz is in Hz (capped at 50% of notch bandwidth).
    Transitions are outside the notch band.
    """
    n = len(data)
    spectrum = np.fft.rfft(data)
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_rate)

    mask = np.ones(len(freqs), dtype=np.float64)
    for f_low, f_high in notch_bands:
        tw = min(transition_width_hz, 0.5 * (f_high - f_low))
        for i, f in enumerate(freqs):
            if f_low <= f <= f_high:
                mask[i] = 0.0
            elif (f_low - tw) < f < f_low:
                # 1 at f_low-tw, 0 at f_low (same shape as bandpass high-side rolloff)
                mask[i] = min(mask[i], 0.5 * (1.0 - np.cos(np.pi * (f_low - f) / tw)))
            elif f_high < f < (f_high + tw):
                # 0 at f_high, 1 at f_high+tw (same shape as bandpass low-side rolloff)
                mask[i] = min(mask[i], 0.5 * (1.0 - np.cos(np.pi * (f - f_high) / tw)))

    spectrum *= mask
    return np.fft.irfft(spectrum, n=n).astype(data.dtype)


def spectral_notch_interpolate(
    data: npt.NDArray,
    sampling_rate: float,
    notch_bands: List[Tuple[float, float]],
    transition_width_hz: float = 0.1,
) -> npt.NDArray:
    """Band-reject by interpolating spectrum across notch_bands.

    Instead of zeroing, replaces the spectrum inside each band with a
    linear interpolation (in complex amplitude) between the band edges.
    Same transition taper as spectral_notch_cut.
    """
    n = len(data)
    spectrum = np.fft.rfft(data)
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_rate)

    for f_low, f_high in notch_bands:
        tw = min(transition_width_hz, 0.5 * (f_high - f_low))
        # Find edge bin values for interpolation
        i_lo = int(np.searchsorted(freqs, f_low, side="left"))
        i_hi = int(np.searchsorted(freqs, f_high, side="right")) - 1
        val_lo = spectrum[max(i_lo - 1, 0)]
        val_hi = spectrum[min(i_hi + 1, len(spectrum) - 1)]

        for i, f in enumerate(freqs):
            if f_low <= f <= f_high:
                # Linear interpolation between edge values
                t = (f - f_low) / (f_high - f_low) if f_high > f_low else 0.5
                spectrum[i] = val_lo * (1.0 - t) + val_hi * t
            elif (f_low - tw) < f < f_low:
                interp_val = val_lo
                blend = 0.5 * (1.0 - np.cos(np.pi * (f_low - f) / tw))
                spectrum[i] = spectrum[i] * blend + interp_val * (1.0 - blend)
            elif f_high < f < (f_high + tw):
                interp_val = val_hi
                blend = 0.5 * (1.0 - np.cos(np.pi * (f - f_high) / tw))
                spectrum[i] = spectrum[i] * blend + interp_val * (1.0 - blend)

    return np.fft.irfft(spectrum, n=n).astype(data.dtype)


def parse_frequency_bands(bands_str: str) -> List[Tuple[float, float]]:
    """Parse a frequency bands string like '0.1-0.5,0.5-1.0,1.0-2.0'."""
    bands = []
    for part in bands_str.split(","):
        part = part.strip()
        if not part:
            continue
        low_s, high_s = part.split("-")
        bands.append((float(low_s.strip()), float(high_s.strip())))
    return bands


def do_linear_stack_of_crosscorrelations_cylindrical(ccfs: Collection[CrosscorrelationCylindrical]) -> npt.ArrayLike:
    """
    Takes a collection of :py:class:`~noiz.models.crosscorrelation.CrosscorrelationCylindrical` objects and performs
    a linear stack on all of them.
    Returns raw array with the stack itself.

    :param ccfs: CrosscorrelationCylindricals to stack
    :type ccfs: Collection[CrosscorrelationCylindrical]
    :return: Array with stacked crosscorrelation_cylindrical
    :rtype: np.array
    """
    mean_ccf = np.array([x.ccf for x in ccfs]).mean(axis=0)
    return mean_ccf


def compute_rms_relative_difference(stack_current: npt.NDArray, stack_previous: npt.NDArray) -> float:
    """Compute RMS relative difference (in %) between two stacks.

    Returns 100 * sqrt(mean((current - previous)^2)) / sqrt(mean(previous^2)).
    If the previous stack is all zeros, returns NaN.
    """
    rms_prev = np.sqrt(np.mean(stack_previous**2))
    if rms_prev == 0.0:
        return float("nan")
    diff = stack_current - stack_previous
    rms_diff = np.sqrt(np.mean(diff**2))
    return 100.0 * rms_diff / rms_prev


def plot_convergence_global(
    cumulative_durations: list,
    global_rms: list,
    output_path,
    per_pair_rms: Optional[npt.NDArray] = None,
    title: Optional[str] = None,
) -> None:
    """Plot global RMS relative difference vs calendar days.

    If per_pair_rms is provided (shape n_pairs x n_increments matching
    global_rms length), the mean, median, Q10 and Q90 of individual pair
    RMS values are also plotted.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(cumulative_durations, global_rms, "o-", linewidth=1.5, markersize=4, label="Global RMS", zorder=5)

    if per_pair_rms is not None and len(per_pair_rms) > 0 and len(per_pair_rms[0]) == len(global_rms):
        with np.errstate(invalid="ignore"):
            mean_vals = np.nanmean(per_pair_rms, axis=0)
            median_vals = np.nanmedian(per_pair_rms, axis=0)
            q10_vals = np.nanpercentile(per_pair_rms, 10, axis=0)
            q90_vals = np.nanpercentile(per_pair_rms, 90, axis=0)
        ax.plot(cumulative_durations, mean_vals, "--", linewidth=1.0, color="tab:orange", label="Mean")
        ax.plot(cumulative_durations, median_vals, "-", linewidth=1.0, color="tab:green", label="Median")
        ax.fill_between(cumulative_durations, q10_vals, q90_vals, alpha=0.15, color="tab:blue", label="Q10-Q90")
        ax.legend(loc="upper right", fontsize=8)

    ax.set_xlabel("Calendar days")
    ax.set_ylabel("RMS relative difference (%)")
    ax.set_title(title if title else "Convergence study - Global RMS relative difference")
    ax.grid(True, alpha=0.3)
    ax.set_xlim(left=0)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_convergence_pcolor(
    cumulative_durations: list,
    per_pair_rms: npt.NDArray,
    output_path,
    pair_labels: Optional[List[Any]] = None,
) -> None:
    """Plot 2D pcolor of per-pair RMS relative difference vs calendar days.

    Pairs are sorted so the fastest converging appear at the bottom.
    per_pair_rms has shape (n_pairs, n_increments).
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LogNorm

    n_pairs = len(per_pair_rms)

    # Sort pairs by mean RMS (ascending = fastest convergent at bottom)
    with np.errstate(invalid="ignore"):
        mean_rms = np.nanmean(per_pair_rms, axis=1)
    sort_idx = np.argsort(mean_rms)[::-1]  # descending so bottom = fastest
    sorted_rms = per_pair_rms[sort_idx, :]

    # Log-scale needs positive values; clamp floor at smallest nonzero value
    valid_vals = sorted_rms[np.isfinite(sorted_rms) & (sorted_rms > 0)]
    if len(valid_vals) > 0:
        vmin = valid_vals.min()
        vmax = valid_vals.max()
    else:
        vmin, vmax = 1e-3, 1e2
    plot_data = np.where(np.isfinite(sorted_rms) & (sorted_rms > 0), sorted_rms, np.nan)

    fig_height = min(40, max(4, n_pairs * 0.3))
    fig, ax = plt.subplots(figsize=(fig_height, fig_height))
    x_edges = np.concatenate([[0], cumulative_durations])
    y_edges = np.arange(n_pairs + 1)
    mesh = ax.pcolormesh(
        x_edges,
        y_edges,
        plot_data,
        shading="flat",
        cmap="viridis",
        norm=LogNorm(vmin=vmin, vmax=vmax),
    )
    fig.colorbar(mesh, ax=ax, label="RMS relative difference (%)")
    ax.set_xlabel("Calendar days")
    ax.set_ylabel("Station pair")
    ax.set_title("Convergence study - Per-pair RMS relative difference")
    ax.set_xlim(left=0)

    if pair_labels is not None:
        sorted_labels = [pair_labels[i] for i in sort_idx]
        y_centers = np.arange(n_pairs) + 0.5
        ax.set_yticks(y_centers)
        pixel_height = max(1, fig.get_size_inches()[1] * fig.dpi / n_pairs)
        font_size = max(4, min(8, pixel_height * 0.7))
        ax.set_yticklabels(sorted_labels, fontsize=font_size)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_convergence_gather(
    time_axis: npt.NDArray,
    stacks: dict,
    pair_distances: dict,
    pair_labels: dict,
    cumul_days: float,
    output_path,
    psd_window_times: Optional[dict] = None,
    title: Optional[str] = None,
    pair_trace_labels: Optional[dict] = None,
    ref_station: Optional[str] = None,
) -> None:
    """Plot a gather of stacked CCFs at a given increment.

    Each CCF is plotted at its inter-station distance on the y axis,
    with amplitude normalized for readability.

    If psd_window_times is provided as {pair_id: (t_start, t_end)},
    dots are plotted at the window boundaries on each trace.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not stacks:
        return

    # Sort by distance
    sorted_pair_ids = sorted(stacks.keys(), key=lambda pid: pair_distances.get(pid, 0))

    offsets = [pair_distances[pid] for pid in sorted_pair_ids]
    n_traces = len(sorted_pair_ids)

    # Compute trace spacing as fraction of total y range
    y_min = min(offsets)
    y_max = max(offsets)
    y_range = y_max - y_min if y_max > y_min else 1.0
    trace_half_height = y_range / max(1, n_traces - 1) * 1.0 if n_traces > 1 else y_range * 0.8

    fig, ax = plt.subplots(figsize=(8, 8))

    for pid in sorted_pair_ids:
        trace = stacks[pid]
        peak = np.max(np.abs(trace))
        if peak > 0:
            normed = trace / peak * trace_half_height
        else:
            normed = trace
        offset = pair_distances[pid]
        ax.plot(time_axis, normed + offset, linewidth=0.5, color="black")

        # Label trace with station name on the right
        if pair_trace_labels is not None and pid in pair_trace_labels:
            ax.text(
                time_axis[-1],
                offset,
                f" {pair_trace_labels[pid]}",
                fontsize=6,
                va="center",
                ha="left",
                color="dimgray",
            )

        # Mark PSD window boundaries with short vertical ticks
        if psd_window_times is not None and pid in psd_window_times:
            t_start, t_end = psd_window_times[pid]
            tick_h = trace_half_height * 0.15
            for t_mark in (t_start, t_end):
                idx = np.argmin(np.abs(time_axis - t_mark))
                y_center = normed[idx] + offset
                ax.plot(
                    [time_axis[idx], time_axis[idx]],
                    [y_center - tick_h, y_center + tick_h],
                    linewidth=0.5,
                    color="black",
                )

    ax.set_xlabel("Time lag (s)")
    ax.set_ylabel("Inter-station distance (m)")
    default_title = f"CCF gather - {cumul_days:.1f} days stacked"
    if ref_station and not title:
        default_title = f"CCF gather [{ref_station}] - {cumul_days:.1f} days stacked"
    elif ref_station and title:
        title = f"{title} [{ref_station}]"
    ax.set_title(title if title else default_title)
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def _get_window_bounds(
    trace: npt.NDArray,
    sampling_rate: float,
    f_low: float,
) -> Tuple[int, int]:
    """Return (left, right) sample indices for a window centered on max|trace|.

    Window length is 2/f_low, centered on the sample with maximum absolute value.
    """
    peak_idx = int(np.argmax(np.abs(trace)))
    half_len = int(1.0 / f_low * sampling_rate)
    left = max(0, peak_idx - half_len)
    right = min(len(trace) - 1, peak_idx + half_len)

    return int(left), int(right)


def _apply_window(
    trace: npt.NDArray,
    left: int,
    right: int,
) -> npt.NDArray:
    """Apply Tukey taper between left and right, zero elsewhere."""
    from scipy.signal.windows import tukey

    windowed = np.zeros_like(trace)
    seg_len = right - left + 1
    taper = tukey(seg_len, alpha=0.3)
    windowed[left : right + 1] = trace[left : right + 1] * taper
    return windowed


def _window_main_arrival(
    trace: npt.NDArray,
    sampling_rate: float,
    f_low: float,
) -> npt.NDArray:
    """Window the main arrival of a CCF trace using its envelope."""
    left, right = _get_window_bounds(trace, sampling_rate, f_low)
    return _apply_window(trace, left, right)


def _compute_psd_db(
    trace: npt.NDArray,
    sampling_rate: float,
    f_low: float,
    f_high: float,
    window_bounds: Optional[Tuple[int, int]] = None,
) -> tuple:
    """Compute one-sided PSD in dB as |FFT|^2.

    If window_bounds is provided, use those (left, right) sample indices.
    Otherwise uses the full trace with no windowing.
    """
    if window_bounds is not None:
        sig = _apply_window(trace, window_bounds[0], window_bounds[1])
    else:
        sig = trace

    n = len(sig)
    spectrum = np.fft.rfft(sig)
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_rate)
    psd = np.abs(spectrum) ** 2 / n
    with np.errstate(divide="ignore"):
        psd_db = 10.0 * np.log10(psd)
    return freqs, psd_db


def plot_convergence_psd_gather(
    stacks: dict,
    pair_distances: dict,
    pair_labels: dict,
    sampling_rate: float,
    f_low: float,
    f_high: float,
    cumul_days: float,
    output_path,
    pair_window_bounds: Optional[dict] = None,
) -> None:
    """Plot a PSD gather: 10*log10(PSD) at inter-station distance offsets."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not stacks:
        return

    sorted_pair_ids = sorted(stacks.keys(), key=lambda pid: pair_distances.get(pid, 0))
    offsets = [pair_distances[pid] for pid in sorted_pair_ids]
    n_traces = len(sorted_pair_ids)

    y_min = min(offsets)
    y_max = max(offsets)
    y_range = y_max - y_min if y_max > y_min else 1.0
    trace_half_height = y_range / max(1, n_traces - 1) * 1.0 if n_traces > 1 else y_range * 0.8

    fig, ax = plt.subplots(figsize=(8, 8))

    for pid in sorted_pair_ids:
        wb = pair_window_bounds.get(pid) if pair_window_bounds else None
        freqs, psd_db = _compute_psd_db(stacks[pid], sampling_rate, f_low, f_high, window_bounds=wb)
        # Normalize PSD trace for display
        valid = psd_db[np.isfinite(psd_db)]
        if len(valid) > 0:
            psd_range = valid.max() - valid.min()
            if psd_range > 0:
                normed = (psd_db - valid.min()) / psd_range * trace_half_height
                colors = (psd_db - valid.min()) / psd_range
            else:
                normed = np.zeros_like(psd_db)
                colors = np.zeros_like(psd_db)
        else:
            normed = np.zeros_like(psd_db)
            colors = np.zeros_like(psd_db)
        offset = pair_distances[pid]
        # Color segments by PSD value using jet colormap
        from matplotlib.collections import LineCollection

        points = np.column_stack([freqs, normed + offset])
        segments = np.array([points[:-1], points[1:]]).transpose(1, 0, 2)
        seg_colors = 0.5 * (colors[:-1] + colors[1:])
        lc = LineCollection(segments, cmap="jet", linewidths=0.8)
        lc.set_array(seg_colors)
        lc.set_clim(0, 1)
        ax.add_collection(lc)

    ax.autoscale()
    ax.set_xlabel("Frequency (Hz)")
    ax.set_ylabel("Inter-station distance (m)")
    ax.set_title(f"CCF PSD gather - {cumul_days:.1f} days stacked")
    bandwidth = f_high - f_low
    margin = 0.4 * bandwidth
    ax.set_xlim(max(0, f_low - margin), f_high + margin)
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_convergence_psd_pcolor(
    stacks: dict,
    eligible_pair_ids: list,
    pair_labels: list,
    sampling_rate: float,
    f_low: float,
    f_high: float,
    cumul_days: float,
    output_path,
    pair_window_bounds: Optional[dict] = None,
    normalize_rows: bool = False,
) -> None:
    """Plot 2D image of 10*log10(PSD) with y=pairs, x=frequency."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not stacks:
        return

    # Compute PSD for each pair
    n_pairs = len(eligible_pair_ids)
    freqs = None
    psd_matrix: List[Optional[npt.NDArray]] = []
    for cpair_id in eligible_pair_ids:
        if cpair_id in stacks:
            wb = pair_window_bounds.get(cpair_id) if pair_window_bounds else None
            f, psd_db = _compute_psd_db(stacks[cpair_id], sampling_rate, f_low, f_high, window_bounds=wb)
            if freqs is None:
                freqs = f
            psd_matrix.append(psd_db)
        else:
            if freqs is not None:
                psd_matrix.append(np.full(len(freqs), np.nan))
            else:
                psd_matrix.append(None)

    if freqs is None or len(freqs) == 0:
        return

    # Replace None entries
    for i in range(len(psd_matrix)):
        if psd_matrix[i] is None:
            psd_matrix[i] = np.full(len(freqs), np.nan)

    psd_matrix_array = np.array(psd_matrix)

    if normalize_rows:
        with np.errstate(invalid="ignore"):
            row_max = np.nanmax(psd_matrix_array, axis=1, keepdims=True)
            psd_matrix_array = psd_matrix_array - row_max  # dB relative to row max

    fig_height = min(40, max(4, n_pairs * 0.3))
    fig, ax = plt.subplots(figsize=(fig_height, fig_height))
    mesh = ax.pcolormesh(
        freqs,
        np.arange(n_pairs),
        psd_matrix_array,
        shading="nearest",
        cmap="inferno",
    )
    cbar_label = "dB (normalized per pair)" if normalize_rows else "10*log10(PSD)"
    fig.colorbar(mesh, ax=ax, label=cbar_label)
    ax.set_xlabel("Frequency (Hz)")
    ax.set_ylabel("Station pair")
    title_suffix = " [normalized]" if normalize_rows else ""
    ax.set_title(f"CCF PSD{title_suffix} - {cumul_days:.1f} days stacked")
    bandwidth = f_high - f_low
    margin = 0.4 * bandwidth
    ax.set_xlim(max(0, f_low - margin), f_high + margin)

    if pair_labels is not None:
        y_centers = np.arange(n_pairs) + 0.5
        ax.set_yticks(y_centers)
        pixel_height = max(1, fig.get_size_inches()[1] * fig.dpi / n_pairs)
        font_size = max(4, min(8, pixel_height * 0.7))
        ax.set_yticklabels(pair_labels, fontsize=font_size)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_convergence_psd_stats(
    stacks: dict,
    eligible_pair_ids: list,
    sampling_rate: float,
    f_low: float,
    f_high: float,
    cumul_days: float,
    output_path,
    pair_window_bounds: Optional[dict] = None,
) -> None:
    """Plot mean, median, Q10, Q90 of 10*log10(PSD) across all pairs."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not stacks:
        return

    freqs = None
    psd_list = []
    for cpair_id in eligible_pair_ids:
        if cpair_id in stacks:
            wb = pair_window_bounds.get(cpair_id) if pair_window_bounds else None
            f, psd_db = _compute_psd_db(stacks[cpair_id], sampling_rate, f_low, f_high, window_bounds=wb)
            if freqs is None:
                freqs = f
            psd_list.append(psd_db)

    if freqs is None or len(psd_list) == 0:
        return

    psd_array = np.array(psd_list)
    with np.errstate(invalid="ignore"):
        mean_psd = np.nanmean(psd_array, axis=0)
        median_psd = np.nanmedian(psd_array, axis=0)
        q10_psd = np.nanpercentile(psd_array, 10, axis=0)
        q90_psd = np.nanpercentile(psd_array, 90, axis=0)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(freqs, mean_psd, "-", linewidth=1.5, color="tab:orange", label="Mean")
    ax.plot(freqs, median_psd, "-", linewidth=1.5, color="tab:green", label="Median")
    ax.fill_between(freqs, q10_psd, q90_psd, alpha=0.2, color="tab:blue", label="Q10-Q90")
    ax.set_xlabel("Frequency (Hz)")
    ax.set_ylabel("10*log10(PSD)")
    ax.set_title(f"CCF PSD statistics - {cumul_days:.1f} days stacked")
    bandwidth = f_high - f_low
    margin = 0.4 * bandwidth
    ax.set_xlim(max(0, f_low - margin), f_high + margin)
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_psd_vs_raw(
    raw_psd_matrix: Optional[npt.NDArray],
    raw_psd_dates: list,
    raw_psd_freqs: npt.NDArray,
    ccf_psd_freqs: npt.NDArray,
    ccf_psd_values: dict,
    station: str,
    window_label: str,
    output_path,
    f_low: float = 0.0,
    f_high: float = 50.0,
    notch_bands: Optional[List[Tuple[float, float]]] = None,
    notch_color: str = "red",
) -> None:
    """Plot raw PSD spectrogram (left) vs CCF PSD stats (right) for one station/window.

    Parameters
    ----------
    raw_psd_matrix : 2D array (n_timespans x n_freqs), PSD in linear units
    raw_psd_dates : list of datetime for each row
    raw_psd_freqs : 1D array of frequency values
    ccf_psd_freqs : 1D array of frequency values for CCF PSD
    ccf_psd_values : dict with keys 'mean', 'median', 'q10', 'q90' (1D arrays)
    station : station name
    window_label : label for the window (e.g. center day)
    output_path : path to save PNG
    f_low, f_high : frequency limits for y-axis
    notch_bands : list of (lo, hi) frequency bands to shade
    notch_color : color for shading (default "red", use "violet" for interpolated)
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, (ax_spec, ax_ccf) = plt.subplots(1, 2, figsize=(12, 5), gridspec_kw={"width_ratios": [3, 1]}, sharey=True)

    # Left: raw PSD spectrogram
    if raw_psd_matrix is not None and len(raw_psd_dates) > 0 and raw_psd_matrix.size > 0:
        psd_db = 10.0 * np.log10(np.clip(raw_psd_matrix, 1e-30, None))
        # Build time edges for pcolormesh
        dates_num = mdates.date2num(raw_psd_dates)
        if len(dates_num) > 1:
            dt = float(np.median(np.diff(dates_num)))
        else:
            dt = 1.0
        time_edges = np.append(dates_num - dt / 2, dates_num[-1] + dt / 2)
        freq_edges = np.append(
            raw_psd_freqs,
            raw_psd_freqs[-1] + (raw_psd_freqs[-1] - raw_psd_freqs[-2])
            if len(raw_psd_freqs) > 1
            else raw_psd_freqs[-1] + 1,
        )

        im = ax_spec.pcolormesh(time_edges, freq_edges, psd_db.T, shading="flat", cmap="inferno")
        ax_spec.xaxis_date()
        ax_spec.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax_spec.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(raw_psd_dates) // 7)))
        fig.colorbar(im, ax=ax_spec, label="PSD (dB)", pad=0.02, fraction=0.04)
    else:
        ax_spec.text(
            0.5,
            0.5,
            "No raw PSD data",
            transform=ax_spec.transAxes,
            ha="center",
            va="center",
            fontsize=12,
            color="gray",
        )

    ax_spec.set_xlabel("Date")
    ax_spec.set_ylabel("Frequency (Hz)")
    freq_margin = (f_high - f_low) * 0.03
    ax_spec.set_ylim(max(0, f_low - freq_margin), f_high + freq_margin)
    ax_spec.set_title(f"Raw PSD - {station}")

    # Notch bands: black lines on spectrogram, colored shading on PSD panel
    if notch_bands:
        for nb_lo, nb_hi in notch_bands:
            ax_spec.axhline(nb_lo, color="black", linewidth=0.7, linestyle="-", alpha=0.8)
            ax_spec.axhline(nb_hi, color="black", linewidth=0.7, linestyle="-", alpha=0.8)
            ax_ccf.axhspan(nb_lo, nb_hi, color=notch_color, alpha=0.15)

    # Right: CCF PSD stats
    if ccf_psd_values and "mean" in ccf_psd_values:
        f = ccf_psd_freqs
        ax_ccf.plot(ccf_psd_values["mean"], f, "-", color="tab:orange", linewidth=1.0, label="Mean")
        ax_ccf.plot(ccf_psd_values["median"], f, "-", color="tab:green", linewidth=1.0, label="Median")
        if "q10" in ccf_psd_values and "q90" in ccf_psd_values:
            ax_ccf.fill_betweenx(
                f, ccf_psd_values["q10"], ccf_psd_values["q90"], alpha=0.2, color="tab:blue", label="Q10-Q90"
            )
        ax_ccf.legend(loc="upper right", fontsize=7)
        # Clamp x-axis to 30 dB dynamic range
        psd_max = np.nanmax(ccf_psd_values["mean"])
        ax_ccf.set_xlim(left=psd_max - 30)
    else:
        ax_ccf.text(
            0.5, 0.5, "No CCF PSD", transform=ax_ccf.transAxes, ha="center", va="center", fontsize=12, color="gray"
        )

    ax_ccf.set_xlabel("PSD (dB)")
    ax_ccf.set_title(f"CCF PSD - {station}")
    ax_ccf.grid(True, alpha=0.3)

    fig.suptitle(f"{station} - {window_label}", fontsize=11)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)


def plot_raw_psd_vs_gather(
    raw_psd_matrix: Optional[npt.NDArray],
    raw_psd_dates: list,
    raw_psd_freqs: npt.NDArray,
    time_axis: npt.NDArray,
    stacks: dict,
    pair_distances: dict,
    pair_trace_labels: Optional[dict],
    station: str,
    window_label: str,
    output_path,
    f_low: float = 0.0,
    f_high: float = 50.0,
    band_low: Optional[float] = None,
    band_high: Optional[float] = None,
    ccf_psd_freqs: Optional[npt.NDArray] = None,
    ccf_psd_values: Optional[dict] = None,
    notch_bands: Optional[List[Tuple[float, float]]] = None,
    notch_color: str = "red",
) -> None:
    """Left: raw PSD spectrogram; middle: CCF PSD stats; right: time-domain gather."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, (ax_spec, ax_psd, ax_gather) = plt.subplots(
        1,
        3,
        figsize=(16, 6),
        gridspec_kw={"width_ratios": [3, 1, 2]},
        sharey=False,
    )
    # Share y-axis between spectrogram and PSD stats
    ax_psd.sharey(ax_spec)

    # Use the full nominal band for ylim with margin so band lines are visible
    freq_margin = (f_high - f_low) * 0.03
    y_lo, y_hi = max(0, f_low - freq_margin), f_high + freq_margin

    # Left: raw PSD spectrogram
    if raw_psd_matrix is not None and len(raw_psd_dates) > 0 and raw_psd_matrix.size > 0:
        psd_db = 10.0 * np.log10(np.clip(raw_psd_matrix, 1e-30, None))
        dates_num = mdates.date2num(raw_psd_dates)
        dt = float(np.median(np.diff(dates_num))) if len(dates_num) > 1 else 1.0
        time_edges = np.append(dates_num - dt / 2, dates_num[-1] + dt / 2)
        freq_edges = np.append(
            raw_psd_freqs,
            raw_psd_freqs[-1] + (raw_psd_freqs[-1] - raw_psd_freqs[-2])
            if len(raw_psd_freqs) > 1
            else raw_psd_freqs[-1] + 1,
        )
        im = ax_spec.pcolormesh(time_edges, freq_edges, psd_db.T, shading="flat", cmap="inferno")
        ax_spec.xaxis_date()
        ax_spec.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax_spec.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(raw_psd_dates) // 7)))
        fig.colorbar(im, ax=ax_spec, label="PSD (dB)", pad=0.02, fraction=0.04)
    else:
        ax_spec.text(
            0.5,
            0.5,
            "No raw PSD data",
            transform=ax_spec.transAxes,
            ha="center",
            va="center",
            fontsize=12,
            color="gray",
        )

    # Band boundary lines on spectrogram and PSD stats
    for ax_band in (ax_spec, ax_psd):
        if band_low is not None:
            ax_band.axhline(band_low, color="cyan", linewidth=1.0, linestyle="--", alpha=0.8)
        if band_high is not None:
            ax_band.axhline(band_high, color="cyan", linewidth=1.0, linestyle="--", alpha=0.8)

    # Notch bands: black lines on spectrogram, colored shading on PSD panel
    if notch_bands:
        for nb_lo, nb_hi in notch_bands:
            ax_spec.axhline(nb_lo, color="black", linewidth=0.7, linestyle="-", alpha=0.8)
            ax_spec.axhline(nb_hi, color="black", linewidth=0.7, linestyle="-", alpha=0.8)
            ax_psd.axhspan(nb_lo, nb_hi, color=notch_color, alpha=0.15)

    ax_spec.set_xlabel("Date")
    ax_spec.set_ylabel("Frequency (Hz)")
    ax_spec.set_ylim(y_lo, y_hi)
    ax_spec.set_title(f"Raw PSD - {station}")

    # Middle: CCF PSD stats (subband)
    if ccf_psd_values and "mean" in ccf_psd_values and ccf_psd_freqs is not None:
        f = ccf_psd_freqs
        ax_psd.plot(ccf_psd_values["mean"], f, "-", color="tab:orange", linewidth=1.0, label="Mean")
        ax_psd.plot(ccf_psd_values["median"], f, "-", color="tab:green", linewidth=1.0, label="Median")
        if "q10" in ccf_psd_values and "q90" in ccf_psd_values:
            ax_psd.fill_betweenx(
                f, ccf_psd_values["q10"], ccf_psd_values["q90"], alpha=0.2, color="tab:blue", label="Q10-Q90"
            )
        ax_psd.legend(loc="upper right", fontsize=7)
        # Clamp x-axis to 30 dB dynamic range
        psd_max = np.nanmax(ccf_psd_values["mean"])
        ax_psd.set_xlim(left=psd_max - 30)
    else:
        ax_psd.text(
            0.5, 0.5, "No CCF PSD", transform=ax_psd.transAxes, ha="center", va="center", fontsize=12, color="gray"
        )

    ax_psd.set_xlabel("PSD (dB)")
    band_str = ""
    if band_low is not None and band_high is not None:
        band_str = f" [{band_low:.3g}-{band_high:.3g} Hz]"
    ax_psd.set_title(f"CCF PSD{band_str}")
    ax_psd.grid(True, alpha=0.3)
    plt.setp(ax_psd.get_yticklabels(), visible=False)

    # Right: time-domain gather
    if stacks:
        sorted_pids = sorted(stacks.keys(), key=lambda pid: pair_distances.get(pid, 0))
        offsets = [pair_distances[pid] for pid in sorted_pids]
        y_min, y_max = min(offsets), max(offsets)
        y_range = y_max - y_min if y_max > y_min else 1.0
        n_traces = len(sorted_pids)
        trace_hh = y_range / max(1, n_traces - 1) * 1.0 if n_traces > 1 else y_range * 0.8

        for pid in sorted_pids:
            trace = stacks[pid]
            peak = np.max(np.abs(trace))
            normed = trace / peak * trace_hh if peak > 0 else trace
            offset = pair_distances[pid]
            ax_gather.plot(time_axis, normed + offset, linewidth=0.5, color="black")
            if pair_trace_labels and pid in pair_trace_labels:
                ax_gather.text(
                    time_axis[-1],
                    offset,
                    f" {pair_trace_labels[pid]}",
                    fontsize=6,
                    va="center",
                    ha="left",
                    color="dimgray",
                )

        ax_gather.set_ylabel("Inter-station distance (m)")
    else:
        ax_gather.text(
            0.5, 0.5, "No stacks", transform=ax_gather.transAxes, ha="center", va="center", fontsize=12, color="gray"
        )

    ax_gather.set_xlabel("Time lag (s)")
    ax_gather.set_title(f"CCF gather{band_str} - {station}")
    ax_gather.grid(True, alpha=0.2)

    fig.suptitle(f"{station} - {window_label}", fontsize=11)
    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)
