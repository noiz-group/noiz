from typing import List, Any

import numpy as np


def statistical_reject(
        fft_all,
        f_axis,
        fcut2=1.5,
        fcut1=0.25,
        n_thresh_std=2.5,
        prop_bad_freqs=0.5,
        nsta_min_keep=3,
):

    nf = len(f_axis)

    assert nf == fft_all.shape[1]
    # FIXME: Convert to if/else check

    ind_fcut2 = np.where(np.abs(f_axis-fcut2) == np.min(np.abs(f_axis-fcut2)))[0][0]
    ind_fcut1 = np.where(np.abs(f_axis-fcut1) == np.min(np.abs(f_axis-fcut1)))[0][0]

    i_st_on = np.where(~np.isnan(np.mean(fft_all, 1)))[0]
    fft_all_on = fft_all
    fft_all_on_log = 10*np.log10(fft_all_on)

    i_good_stations = i_st_on.copy()

    i_good_stations_old: List[Any] = []
    # FIXME: add a proper type here instead of Any

    while len(i_good_stations) != len(i_good_stations_old):
        i_st_kept = []
        i_st_rejected = []
        for f in range(ind_fcut2):
            mean_fft = np.nanmean(fft_all_on_log[i_good_stations, f])
            std_fft = np.nanstd(fft_all_on_log[i_good_stations, f])

            i_st_kept.append(
                np.where(
                    np.abs(fft_all_on_log[i_good_stations, f] - mean_fft) < n_thresh_std*std_fft
                )[0]
            )
            i_st_rejected.append(
                np.where(
                    np.abs(fft_all_on_log[i_good_stations, f] - mean_fft) >= n_thresh_std*std_fft
                )[0]
            )

        counts = np.zeros(len(i_good_stations))
        for i in range(len(i_good_stations)):
            for f in range(ind_fcut1, ind_fcut2):
                if len(i_st_rejected[f]) > 0:
                    for j in range(len(i_st_rejected[f])):
                        if i == i_st_rejected[f][j]:
                            counts[i] += 1

        i_bad_stations_2 = []
        i_good_stations_2 = []
        for (i, ist) in enumerate(i_good_stations):
            if counts[i] > (ind_fcut2-ind_fcut1)*prop_bad_freqs:
                i_bad_stations_2.append(ist)
            if counts[i] <= (ind_fcut2-ind_fcut1)*prop_bad_freqs:
                i_good_stations_2.append(ist)

        i_good_stations_old = i_good_stations
        if len(i_good_stations_2) > nsta_min_keep:
            i_good_stations = i_good_stations_2

    return i_good_stations, i_st_on
