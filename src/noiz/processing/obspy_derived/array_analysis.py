# OBPSY DERIVED MODULE
# original version:
# https://github.com/obspy/obspy/blob/6f18a18fbe0f778af04f1821523cc18a3d1a0dc0/obspy/signal/array_analysis.py
# Noiz developers thank all the Obpsy contributors

# ------------------------------------------------------------------
# Filename: array.py
#  Purpose: Functions for Array Analysis
#   Author: Martin van Driel, Moritz Beyreuther
#    Email: driel@geophysik.uni-muenchen.de
#
# Copyright (C) 2010 Martin van Driel, Moritz Beyreuther
# --------------------------------------------------------------------
"""
Functions for Array Analysis

:copyright:
    The ObsPy Development Team (devs@obspy.org)
:license:
    GNU Lesser General Public License, Version 3
    (https://www.gnu.org/copyleft/lesser.html)
"""
import math
import warnings
from typing import List

from matplotlib.dates import datestr2num
import numpy as np
from scipy.integrate import cumtrapz

from obspy.core import Stream
from obspy.signal.headers import clibsignal
from obspy.signal.invsim import cosine_taper
from obspy.signal.util import next_pow_2, util_geo_km

from noiz.processing.signal_utils import statistical_reject


def get_geometry(stream, coordsys='lonlat', return_center=False,
                 verbose=False):
    """
    Method to calculate the array geometry and the center coordinates in km

    :param stream: Stream object, the trace.stats dict like class must
        contain an :class:`~obspy.core.util.attribdict.AttribDict` with
        'latitude', 'longitude' (in degrees) and 'elevation' (in km), or 'x',
        'y', 'elevation' (in km) items/attributes. See param ``coordsys``
    :param coordsys: valid values: 'lonlat' and 'xy', choose which stream
        attributes to use for coordinates
    :param return_center: Returns the center coordinates as extra tuple
    :return: Returns the geometry of the stations as 2d :class:`numpy.ndarray`
            The first dimension are the station indexes with the same order
            as the traces in the stream object. The second index are the
            values of [lat, lon, elev] in km
            last index contains center [lat, lon, elev] in degrees and km if
            return_center is true
    """
    nstat = len(stream)
    center_lat = 0.
    center_lon = 0.
    center_h = 0.
    geometry = np.empty((nstat, 3))

    if isinstance(stream, Stream):
        for i, tr in enumerate(stream):
            if coordsys == 'lonlat':
                geometry[i, 0] = tr.stats.coordinates.longitude
                geometry[i, 1] = tr.stats.coordinates.latitude
                geometry[i, 2] = tr.stats.coordinates.elevation
            elif coordsys == 'xy':
                geometry[i, 0] = tr.stats.coordinates.x
                geometry[i, 1] = tr.stats.coordinates.y
                geometry[i, 2] = tr.stats.coordinates.elevation
    elif isinstance(stream, np.ndarray):
        geometry = stream.copy()
    else:
        raise TypeError('only Stream or numpy.ndarray allowed')

    if verbose:
        print("coordsys = " + coordsys)

    if coordsys == 'lonlat':
        center_lon = geometry[:, 0].mean()
        center_lat = geometry[:, 1].mean()
        center_h = geometry[:, 2].mean()
        for i in np.arange(nstat):
            x, y = util_geo_km(center_lon, center_lat, geometry[i, 0],
                               geometry[i, 1])
            geometry[i, 0] = x
            geometry[i, 1] = y
            geometry[i, 2] -= center_h
    elif coordsys == 'xy':
        geometry[:, 0] -= geometry[:, 0].mean()
        geometry[:, 1] -= geometry[:, 1].mean()
        geometry[:, 2] -= geometry[:, 2].mean()
    else:
        raise ValueError("Coordsys must be one of 'lonlat', 'xy'")

    if return_center:
        return np.c_[geometry.T,
                     np.array((center_lon, center_lat, center_h))].T
    else:
        return geometry


def get_timeshift(geometry, sll_x, sll_y, sl_s, grdpts_x, grdpts_y):
    """
    Returns timeshift table for given array geometry

    :param geometry: Nested list containing the arrays geometry, as returned by
            get_group_geometry
    :param sll_x: slowness x min (lower)
    :param sll_y: slowness y min (lower)
    :param sl_s: slowness step
    :param grdpts_x: number of grid points in x direction
    :param grdpts_x: number of grid points in y direction
    """
    # unoptimized version for reference
    # nstat = len(geometry)  # last index are center coordinates
    #
    # time_shift_tbl = np.empty((nstat, grdpts_x, grdpts_y), dtype=np.float32)
    # for k in xrange(grdpts_x):
    #    sx = sll_x + k * sl_s
    #    for l in xrange(grdpts_y):
    #        sy = sll_y + l * sl_s
    #        time_shift_tbl[:,k,l] = sx * geometry[:, 0] + sy * geometry[:,1]
    # time_shift_tbl[:, k, l] = sx * geometry[:, 0] + sy * geometry[:, 1]
    # return time_shift_tbl
    # optimized version
    mx = np.outer(geometry[:, 0], sll_x + np.arange(grdpts_x) * sl_s)
    my = np.outer(geometry[:, 1], sll_y + np.arange(grdpts_y) * sl_s)
    return np.require(
        mx[:, :, np.newaxis].repeat(grdpts_y, axis=2) +
        my[:, np.newaxis, :].repeat(grdpts_x, axis=1),
        dtype=np.float32)


def get_spoint(stream, stime, etime):
    """
    Calculates start and end offsets relative to stime and etime for each
    trace in stream in samples.

    :type stime: :class:`~obspy.core.utcdatetime.UTCDateTime`
    :param stime: Start time
    :type etime: :class:`~obspy.core.utcdatetime.UTCDateTime`
    :param etime: End time
    :returns: start and end sample offset arrays
    """
    spoint = np.empty(len(stream), dtype=np.int32, order="C")
    epoint = np.empty(len(stream), dtype=np.int32, order="C")
    for i, tr in enumerate(stream):
        if tr.stats.starttime > stime:
            msg = "Specified stime %s is smaller than starttime %s in stream"
            raise ValueError(msg % (stime, tr.stats.starttime))
        if tr.stats.endtime < etime:
            msg = "Specified etime %s is bigger than endtime %s in stream"
            raise ValueError(msg % (etime, tr.stats.endtime))
        # now we have to adjust to the beginning of real start time
        spoint[i] = int((stime - tr.stats.starttime) *
                        tr.stats.sampling_rate + .5)
        epoint[i] = int((tr.stats.endtime - etime) *
                        tr.stats.sampling_rate + .5)
    return spoint, epoint


def array_transff_wavenumber(coords, klim, kstep, coordsys='lonlat'):
    """
    Returns array transfer function as a function of wavenumber difference

    :type coords: numpy.ndarray
    :param coords: coordinates of stations in longitude and latitude in degrees
        elevation in km, or x, y, z in km
    :type coordsys: str
    :param coordsys: valid values: 'lonlat' and 'xy', choose which coordinates
        to use
    :param klim: either a float to use symmetric limits for wavenumber
        differences or the tuple (kxmin, kxmax, kymin, kymax)
    """
    coords = get_geometry(coords, coordsys)
    if isinstance(klim, float):
        kxmin = -klim
        kxmax = klim
        kymin = -klim
        kymax = klim
    elif isinstance(klim, tuple):
        if len(klim) == 4:
            kxmin = klim[0]
            kxmax = klim[1]
            kymin = klim[2]
            kymax = klim[3]
    else:
        raise TypeError('klim must either be a float or a tuple of length 4')

    nkx = int(np.ceil((kxmax + kstep / 10. - kxmin) / kstep))
    nky = int(np.ceil((kymax + kstep / 10. - kymin) / kstep))

    # careful with meshgrid indexing
    kygrid, kxgrid = np.meshgrid(np.linspace(kymin, kymax, nky),
                                 np.linspace(kxmin, kxmax, nkx))

    ks = np.transpose(np.vstack((kxgrid.flatten(), kygrid.flatten())))

    # z coordinate is not used
    # Bug with numpy 1.14.0 (https://github.com/numpy/numpy/issues/10343)
    # Nothing we can do.
    if np.__version__ == "1.14.0":  # pragma: no cover
        k_dot_r = np.einsum('ni,mi->nm', ks, coords[:, :2], optimize=False)
    else:
        k_dot_r = np.einsum('ni,mi->nm', ks, coords[:, :2])

    transff = np.abs(np.sum(np.exp(1j * k_dot_r), axis=1))**2 / len(coords)**2

    return transff.reshape(nkx, nky)


def array_transff_freqslowness_wrapper(st, array_proc_kwargs):
    geometry = get_geometry(st)
    fs = st[0].stats.sampling_rate
    nsamp = int(array_proc_kwargs['win_len'] * fs)
    nfft = next_pow_2(nsamp)
    deltaf = fs / float(nfft)
    avg_arf = array_transff_freqslowness(
        coords=geometry,
        slim=(array_proc_kwargs["sll_x_arf"],
              array_proc_kwargs["slm_x_arf"],
              array_proc_kwargs["sll_y_arf"],
              array_proc_kwargs["slm_y_arf"]),
        sstep=array_proc_kwargs["sl_s"],
        fmin=array_proc_kwargs["frqlow"],
        fmax=array_proc_kwargs["frqhigh"],
        fstep=deltaf,
        coordsys="xy")
    return avg_arf


def array_transff_freqslowness(coords, slim, sstep, fmin, fmax, fstep,
                               coordsys='lonlat'):
    """
    Returns array transfer function as a function of slowness difference and
    frequency.

    :type coords: numpy.ndarray
    :param coords: coordinates of stations in longitude and latitude in degrees
        elevation in km, or x, y, z in km
    :type coordsys: str
    :param coordsys: valid values: 'lonlat' and 'xy', choose which coordinates
        to use
    :param slim: either a float to use symmetric limits for slowness
        differences or the tupel (sxmin, sxmax, symin, symax)
    :type fmin: float
    :param fmin: minimum frequency in signal
    :type fmax: float
    :param fmin: maximum frequency in signal
    :type fstep: float
    :param fmin: frequency sample distance
    """
    coords = get_geometry(coords, coordsys)
    if isinstance(slim, float):
        sxmin = -slim
        sxmax = slim
        symin = -slim
        symax = slim
    elif isinstance(slim, tuple):
        if len(slim) == 4:
            sxmin = slim[0]
            sxmax = slim[1]
            symin = slim[2]
            symax = slim[3]
    else:
        raise TypeError('slim must either be a float or a tuple of length 4')

    nsx = int(np.ceil((sxmax + sstep / 10. - sxmin) / sstep))
    nsy = int(np.ceil((symax + sstep / 10. - symin) / sstep))
    nf = int(np.ceil((fmax + fstep / 10. - fmin) / fstep))

    transff = np.empty((nsx, nsy))
    buff = np.zeros(nf)

    for i, sx in enumerate(np.arange(sxmin, sxmax + sstep / 10., sstep)):
        for j, sy in enumerate(np.arange(symin, symax + sstep / 10., sstep)):
            for k, f in enumerate(np.arange(fmin, fmax + fstep / 10., fstep)):
                _sum = 0j
                for l in np.arange(len(coords)):  # NOQA
                    _sum += np.exp(
                        complex(0., (coords[l, 0] * sx + coords[l, 1] * sy) *
                                2 * np.pi * f))
                buff[k] = abs(_sum) ** 2 
            ### STORENGY MODIF
            if len(buff) > 1:
                transff[i, j] = cumtrapz(buff, dx=fstep)[-1] # original line
            else:
                transff[i, j] = buff * fstep
            ### END OF STORENGY MODIF

    transff /= transff.max()
    return transff


def dump(pow_map, apow_map, i):
    """
    Example function to use with `store` kwarg in
    :func:`~obspy.signal.array_analysis.array_processing`.
    """
    np.savez('pow_map_%d.npz' % i, pow_map)
    np.savez('apow_map_%d.npz' % i, apow_map)


def array_processing(
        stream,
        win_len,
        win_frac,
        sll_x,
        slm_x,
        sll_y,
        slm_y,
        sl_s,
        semb_thres,
        vel_thres,
        frqlow,
        frqhigh,
        stime,
        etime,
        prewhiten,
        coordsys='lonlat',
        timestamp='mlabday',
        method=0,
        store=None,
        save_arf=False,
        sll_x_arf=None,
        slm_x_arf=None,
        sll_y_arf=None,
        slm_y_arf=None
):
    """
    Method for Seismic-Array-Beamforming/FK-Analysis/Capon

    :param stream: Stream object, the trace.stats dict like class must
        contain an :class:`~obspy.core.util.attribdict.AttribDict` with
        'latitude', 'longitude' (in degrees) and 'elevation' (in km), or 'x',
        'y', 'elevation' (in km) items/attributes. See param ``coordsys``.
    :type win_len: float
    :param win_len: Sliding window length in seconds
    :type win_frac: float
    :param win_frac: Fraction of sliding window to use for step
    :type sll_x: float
    :param sll_x: slowness x min (lower)
    :type slm_x: float
    :param slm_x: slowness x max
    :type sll_y: float
    :param sll_y: slowness y min (lower)
    :type slm_y: float
    :param slm_y: slowness y max
    :type sl_s: float
    :param sl_s: slowness step
    :type semb_thres: float
    :param semb_thres: Threshold for semblance
    :type vel_thres: float
    :param vel_thres: Threshold for velocity
    :type frqlow: float
    :param frqlow: lower frequency for fk/capon
    :type frqhigh: float
    :param frqhigh: higher frequency for fk/capon
    :type stime: :class:`~obspy.core.utcdatetime.UTCDateTime`
    :param stime: Start time of interest
    :type etime: :class:`~obspy.core.utcdatetime.UTCDateTime`
    :param etime: End time of interest
    :type prewhiten: int
    :param prewhiten: Do prewhitening, values: 1 or 0
    :param coordsys: valid values: 'lonlat' and 'xy', choose which stream
        attributes to use for coordinates
    :type timestamp: str
    :param timestamp: valid values: 'julsec' and 'mlabday'; 'julsec' returns
        the timestamp in seconds since 1970-01-01T00:00:00, 'mlabday'
        returns the timestamp in days (decimals represent hours, minutes
        and seconds) since the start of the used matplotlib time epoch
    :type method: int
    :param method: the method to use 0 == bf, 1 == capon
    :type store: callable
    :param store: A custom function which gets called on each iteration. It is
        called with the relative power map and the time offset as first and
        second arguments and the iteration number as third argument. Useful for
        storing or plotting the map for each iteration. For this purpose the
        dump function of this module can be used.
    :return: :class:`numpy.ndarray` of timestamp, relative relpow, absolute
        relpow, backazimuth, slowness
    """
    res: List[np.typing.ArrayLike] = []
    eotr = True

    # check that sampling rates do not vary
    fs = stream[0].stats.sampling_rate
    if len(stream) != len(stream.select(sampling_rate=fs)):
        msg = 'in sonic sampling rates of traces in stream are not equal'
        raise ValueError(msg)

    grdpts_x = int(((slm_x - sll_x) / sl_s + 0.5) + 1)
    grdpts_y = int(((slm_y - sll_y) / sl_s + 0.5) + 1)

    geometry = get_geometry(stream, coordsys=coordsys, verbose=False)

    time_shift_table = get_timeshift(geometry, sll_x, sll_y,
                                     sl_s, grdpts_x, grdpts_y)
    # offset of arrays
    spoint, _epoint = get_spoint(stream, stime, etime)
    #
    # loop with a sliding window over the dat trace array and apply bbfk
    #
    nstat = len(stream)  # Nombre de traces dans stream, nombre de stations
    fs = stream[0].stats.sampling_rate
    nsamp = int(win_len * fs)
    nstep = int(nsamp * win_frac)

    # generate plan for rfftr
    nfft = next_pow_2(nsamp)
    deltaf = fs / float(nfft)
    nlow = int(frqlow / float(deltaf) + 0.5)
    nhigh = int(frqhigh / float(deltaf) + 0.5)
    nlow = max(1, nlow)  # avoid using the offset
    nhigh = min(nfft // 2 - 1, nhigh)  # avoid using nyquist
    nf = nhigh - nlow + 1  # include upper and lower frequency
    # to speed up the routine a bit we estimate all steering vectors in advance
    steer = np.empty((nf, grdpts_x, grdpts_y, nstat), dtype=np.complex128)
    clibsignal.calcSteer(nstat, grdpts_x, grdpts_y, nf, nlow,
                         deltaf, time_shift_table, steer)
    _r = np.empty((nf, nstat, nstat), dtype=np.complex128)  # matrice interspectrale
    ft = np.empty((nstat, nf), dtype=np.complex128)
    ### ADDED CKH/ AKA 18/05/22 ###
    f_axis = np.linspace(0, fs/2, int(nfft/2) + 1)
    ft_full = np.empty((nstat, int(nfft/2) + 1), dtype=np.complex128)
    ###
    newstart = stime
    # 0.22 matches 0.2 of historical C bbfk.c
    tap = cosine_taper(nsamp, p=0.22)
    offset = 0
    relpow_map = np.empty((grdpts_x, grdpts_y), dtype=np.float64)
    abspow_map = np.empty((grdpts_x, grdpts_y), dtype=np.float64)
    while eotr:
        try:
            for i, tr in enumerate(stream):
                dat = tr.data[spoint[i] + offset:
                              spoint[i] + offset + nsamp]
                dat = (dat - dat.mean()) * tap
                ### ORIGINAL
                # ft[i, :] = np.fft.rfft(dat, nfft)[nlow:nlow + nf]
                ### ADDED CKH/ AKA 18/05/22 ###
                ft_full[i, :] = np.fft.rfft(dat, nfft)
                ###
        except IndexError:
            break

        ### ADDED CKH/ AKA 18/05/22 ###`
        i_good_stations, i_st_on = statistical_reject(np.abs(ft_full), f_axis)
        ft[:, :] = 0
        ft[i_st_on[i_good_stations], :] = ft_full[i_st_on[i_good_stations], nlow:nlow + nf]
        ###

        ### AKA 19/07/2023 ###
        arf = calculate_arf(deltaf, frqhigh, frqlow, geometry, i_good_stations, i_st_on, save_arf, sl_s, sll_x_arf,
                            sll_y_arf, slm_x_arf, slm_y_arf)
        ######################

        relpow_map.fill(0.)
        abspow_map.fill(0.)
        dpow, ft = Compute_covariances(_r, ft, method, nf, nstat)

        errcode = clibsignal.generalizedBeamformer(
            relpow_map, abspow_map, steer, _r, nstat, prewhiten,
            grdpts_x, grdpts_y, nf, dpow, method)
        if errcode != 0:
            msg = 'generalizedBeamforming exited with error %d'
            raise Exception(msg % errcode)
        ix, iy = np.unravel_index(relpow_map.argmax(), relpow_map.shape)
        relpow, abspow = relpow_map[ix, iy], abspow_map[ix, iy]
        ### AKA 19/07/2023 added arf as extra input to "store" method
        if store is not None:
            store(relpow_map, abspow_map, offset, arf=arf)
        ##########################################
        # here we compute baz, slow
        slow_x = sll_x + ix * sl_s
        slow_y = sll_y + iy * sl_s

        slow = np.sqrt(slow_x ** 2 + slow_y ** 2)
        if slow < 1e-8:
            slow = 1e-8
        azimut = 180 * math.atan2(slow_x, slow_y) / math.pi
        baz = azimut % -360 + 180
        if relpow > semb_thres and 1. / slow > vel_thres:
            res.append(np.array([newstart.timestamp, relpow, abspow, baz,
                                 slow]))
        if (newstart + (nsamp + nstep) / fs) > etime:
            eotr = False
        offset += nstep

        newstart += nstep / fs
    stacked_res: np.typing.ArrayLike = np.array(res)
    if timestamp == 'julsec':
        pass
    elif timestamp == 'mlabday':
        stacked_res[:, 0] = stacked_res[:, 0] / (24. * 3600) + datestr2num('1970-01-01')
    else:
        msg = "Option timestamp must be one of 'julsec', or 'mlabday'"
        raise ValueError(msg)
    return np.array(stacked_res)


def Compute_covariances(_r, ft, method, nf, nstat):
    # computing the covariances of the signal at different receivers
    ft = np.ascontiguousarray(ft, np.complex128)
    dpow = 0.
    for i in range(nstat):
        for j in range(i, nstat):
            _r[:, i, j] = ft[i, :] * ft[j, :].conj()
            if method == 1:
                _r[:, i, j] /= np.abs(_r[:, i, j].sum())
            if i != j:
                _r[:, j, i] = _r[:, i, j].conjugate()
            else:
                dpow += np.abs(_r[:, i, j].sum())
    dpow *= nstat
    if method == 1:
        # P(f) = 1/(e.H R(f)^-1 e)
        for n in range(nf):
            _r[n, :, :] = np.linalg.pinv(_r[n, :, :], rcond=1e-6)
    return dpow, ft


def calculate_arf(
        deltaf,
        frqhigh,
        frqlow,
        geometry,
        i_good_stations,
        i_st_on,
        save_arf,
        sl_s,
        sll_x_arf,
        sll_y_arf,
        slm_x_arf,
        slm_y_arf
):
    if not save_arf:
        return None

    geometry_updated = geometry[i_st_on[i_good_stations], :]
    return array_transff_freqslowness(
        geometry_updated,
        (sll_x_arf, slm_x_arf, sll_y_arf, slm_y_arf),
        sl_s,
        frqlow,
        frqhigh,
        deltaf,
        coordsys="xy"
    )
