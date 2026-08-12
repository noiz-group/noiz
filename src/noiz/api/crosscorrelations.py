# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import more_itertools

from loguru import logger
from obspy.signal.cross_correlation import correlate
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import subqueryload, Query
from sqlalchemy.sql import Insert
from typing import List, Union, Optional, Collection, Dict, Generator, Tuple, Any, FrozenSet, Set

from noiz.api.component import fetch_components_by_id
from noiz.api.component_pair import (
    fetch_componentpairs_cartesian,
    fetch_componentpairs_cartesian_by_id,
    fetch_componentpairs_cylindrical,
)
from noiz.api.helpers import (
    extract_object_ids,
    _run_calculate_and_upsert_on_dask,
    _run_calculate_and_upsert_sequentially,
    extract_object_ids_keep_objects,
    create_process_dask_client,
    recreate_process_dask_client,
)
from noiz.api.processing_config import (
    fetch_crosscorrelation_cartesian_params_by_id,
    fetch_crosscorrelation_cylindrical_params_by_id,
)
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import InconsistentDataException, CorruptedDataException
from noiz.models import (
    ComponentPairCartesian,
    CrosscorrelationCartesianFile,
    CrosscorrelationCartesian,
    Datachunk,
    ProcessedDatachunk,
    CrosscorrelationCartesianParams,
    Timespan,
    CrosscorrelationCylindrical,
    ComponentPairCylindrical,
    CrosscorrelationCylindricalFile,
    CrosscorrelationCylindricalParams,
)
from noiz.models.type_aliases import CrosscorrelationCartesianRunnerInputs, CrosscorrelationCylindricalRunnerInputs
from noiz.processing.crosscorrelations import (
    validate_component_code_pairs,
    group_chunks_by_timespanid_componentid,
    load_data_for_chunks,
    extract_component_ids_from_component_pairs_cartesian,
    assembly_ccf_cartesian_dataframe,
    group_xcrorrcartesian_by_timespanid_componentids,
    _fetch_R_T_xcoor,
    _computation_cylindrical_correlation_R_T,
    _fetch_RT_Z_xcoor,
    _computation_cylindrical_correlation_RT_Z,
    _fetch_Z_TR_xcoor,
    _computation_cylindrical_correlation_Z_TR,
)
from noiz.processing.io import write_ccfs_to_npz
from noiz.processing.path_helpers import (
    assembly_filepath,
    increment_filename_counter,
    parent_directory_exists_or_create,
)
from noiz.validation_helpers import validate_to_tuple


def _filter_component_pairs_for_component_batches(
    component_pairs: Collection[ComponentPairCartesian],
    batch_i_component_ids: Collection[int],
    batch_j_component_ids: Collection[int],
) -> List[ComponentPairCartesian]:
    """
    Select component pairs that belong exactly to the provided component-batch combination.

    For self-batches, only pairs with both components inside the same batch are selected.
    For cross-batches, only pairs that span between the two batches are selected.

    This avoids re-processing intra-batch pairs in every cross-batch combination.
    """

    batch_i = set(batch_i_component_ids)
    batch_j = set(batch_j_component_ids)

    if batch_i == batch_j:
        return [pair for pair in component_pairs if pair.component_a_id in batch_i and pair.component_b_id in batch_i]

    return [
        pair
        for pair in component_pairs
        if (
            (pair.component_a_id in batch_i and pair.component_b_id in batch_j)
            or (pair.component_a_id in batch_j and pair.component_b_id in batch_i)
        )
    ]


def _resolve_ccf_dask_submission_batch_size(
    pairs_per_task: int,
    dask_n_workers: Optional[int],
) -> int:
    if dask_n_workers is None:
        return pairs_per_task

    submission_batch_size = min(pairs_per_task, max(16, dask_n_workers * 4))
    if submission_batch_size < pairs_per_task:
        logger.info(
            f"Limiting Dask submission batch size to {submission_batch_size} task input(s) "
            f"for scheduler stability while keeping pairs_per_task={pairs_per_task}."
        )

    return submission_batch_size


def fetch_crosscorrelation_cartesian(
    crosscorrelation_cartesian_params_id: Optional[int] = None,
    componentpair_id: Optional[Collection[int]] = None,
    timespan_id: Optional[Collection[int]] = None,
    load_componentpair: bool = False,
    load_timespan: bool = False,
    load_crosscorrelation_cartesian_params: bool = False,
) -> List[CrosscorrelationCartesian]:
    """filldocs"""

    query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_cartesian_params=load_crosscorrelation_cartesian_params,
    )

    return query.all()


def count_crosscorrelation_cartesian(
    crosscorrelation_cartesian_params_id: Optional[int] = None,
    componentpair_id: Optional[Collection[int]] = None,
    timespan_id: Optional[Collection[int]] = None,
    load_componentpair: bool = False,
    load_timespan: bool = False,
    load_crosscorrelation_cartesian_params: bool = False,
) -> int:
    """filldocs"""
    query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        componentpair_id=componentpair_id,
        timespan_id=timespan_id,
        load_componentpair=load_componentpair,
        load_timespan=load_timespan,
        load_crosscorrelation_cartesian_params=load_crosscorrelation_cartesian_params,
    )

    return query.count()


def _query_crosscorrelation_cartesian(
    crosscorrelation_cartesian_params_id: Optional[int] = None,
    componentpair_id: Optional[Collection[int]] = None,
    timespan_id: Optional[Collection[int]] = None,
    load_componentpair: bool = False,
    load_timespan: bool = False,
    load_crosscorrelation_cartesian_params: bool = False,
) -> Query:
    """filldocs"""
    filters = []

    if crosscorrelation_cartesian_params_id is not None:
        filters.append(
            CrosscorrelationCartesian.crosscorrelation_cartesian_params_id == crosscorrelation_cartesian_params_id
        )
    if componentpair_id is not None:
        filters.append(CrosscorrelationCartesian.componentpair_id.in_(componentpair_id))
    if timespan_id is not None:
        filters.append(CrosscorrelationCartesian.timespan_id.in_(timespan_id))
    if len(filters) == 0:
        filters.append(True)

    opts = []
    if load_timespan:
        opts.append(subqueryload(CrosscorrelationCartesian.timespan))
    if load_componentpair:
        opts.append(subqueryload(CrosscorrelationCartesian.componentpair_cartesian))
    if load_crosscorrelation_cartesian_params:
        opts.append(subqueryload(CrosscorrelationCartesian.crosscorrelation_cartesian_params))

    return db.session.query(CrosscorrelationCartesian).filter(*filters).options(opts)


def _prepare_upsert_command_crosscorrelation_cartesian(xcorr: CrosscorrelationCartesian) -> Insert:
    # First, ensure the file record exists in the database
    # The file may have been rolled back if the bulk insert failed
    file_id = None
    if xcorr.file is not None:
        filepath = xcorr.file.filepath
        logger.debug(f"DEBUG_UPSERT: Checking if file exists in DB: {filepath[:80]}...")
        # Check if file already exists in DB by filepath
        existing_file = (
            db.session.query(CrosscorrelationCartesianFile)
            .filter(CrosscorrelationCartesianFile.filepath == filepath)
            .first()
        )

        if existing_file is not None:
            file_id = existing_file.id
            logger.debug(f"DEBUG_UPSERT: Found existing file with id={file_id}")
        else:
            # Insert the file record first
            logger.debug("DEBUG_UPSERT: File not found, inserting new file record...")
            new_file = CrosscorrelationCartesianFile(filepath=filepath)
            db.session.add(new_file)
            db.session.flush()  # Get the ID without committing
            file_id = new_file.id
            logger.debug(f"DEBUG_UPSERT: Inserted new file with id={file_id}")
    elif xcorr.crosscorrelation_cartesian_file_id is not None:
        file_id = xcorr.crosscorrelation_cartesian_file_id
        logger.debug(f"DEBUG_UPSERT: Using existing file_id from xcorr: {file_id}")

    insert_command = (
        insert(CrosscorrelationCartesian)
        .values(
            crosscorrelation_cartesian_params_id=xcorr.crosscorrelation_cartesian_params_id,
            componentpair_id=xcorr.componentpair_id,
            timespan_id=xcorr.timespan_id,
            crosscorrelation_cartesian_file_id=file_id,
        )
        .on_conflict_do_update(
            constraint="unique_ccfn_per_timespan_per_componentpair_per_config",  # Note: ccfN not ccf
            set_={"crosscorrelation_cartesian_file_id": file_id},
        )
    )
    return insert_command


from dataclasses import dataclass
from typing import NamedTuple


@dataclass
class MemoryEstimation:
    """Result of memory estimation for crosscorrelation processing."""

    timespans_at_once: int
    components_per_batch: Optional[int]  # None means all components, int means sub-timespan chunking needed
    samples_per_chunk: int
    ccf_samples: int
    mem_per_timespan_full: float  # Memory for full timespan (all components)
    mem_per_component: float  # Memory per component per timespan
    usable_ram: float
    needs_component_chunking: bool


def _estimate_timespans_at_once(
    crosscorrelation_cartesian_params_id: int,
    num_timespans: int,
    num_component_pairs: int,
    num_unique_components: int,
    parallel: bool = True,
    ram_safety_factor: float = 0.5,
    max_timespans: int = 100,
) -> MemoryEstimation:
    """
    Estimate optimal number of timespans to process at once based on available RAM.

    When even a single timespan doesn't fit in memory (too many components/pairs),
    this function calculates how to split by component subsets ("sub-timespan chunking").

    Memory estimation factors:
    - ProcessedDatachunk data: ~samples_per_chunk * 8 bytes (float64) per component per timespan
    - CCF output: ~(2 * correlation_max_lag_samples + 1) * 8 bytes per pair per timespan
    - SQLAlchemy overhead: ~1KB per object
    - Dask overhead: ~2x data size for task serialization/deserialization

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams
    :param num_timespans: Total number of timespans to process
    :param num_component_pairs: Number of component pairs
    :param num_unique_components: Number of unique components (for datachunk loading)
    :param parallel: Whether using Dask parallel processing
    :param ram_safety_factor: Fraction of available RAM to use (0.0-1.0), default 0.5 (50%)
    :param max_timespans: Maximum timespans at once regardless of RAM
    :return: MemoryEstimation with recommended settings
    """
    import psutil

    # Get available system RAM
    mem = psutil.virtual_memory()
    available_ram_bytes = mem.available
    total_ram_bytes = mem.total

    logger.info(
        f"RAM estimation: Total={total_ram_bytes / 1024**3:.2f}GB, Available={available_ram_bytes / 1024**3:.2f}GB"
    )

    # Fetch params to get chunk size info
    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)

    # Get sampling rate from DatachunkParams
    from noiz.api.processing_config import fetch_processed_datachunk_params_by_id, fetch_datachunkparams_by_id

    processed_datachunk_params = fetch_processed_datachunk_params_by_id(id=params.processed_datachunk_params_id)
    datachunk_params = fetch_datachunkparams_by_id(id=processed_datachunk_params.datachunk_params_id)
    sampling_rate = datachunk_params.sampling_rate

    # Get chunk duration from a Timespan (Timespan defines the window length)
    from noiz.models import Timespan

    sample_timespan = db.session.query(Timespan).first()
    if sample_timespan is None:
        logger.warning("No timespans found in database. Using default chunk duration of 3600s")
        chunk_duration_seconds = 3600.0
    else:
        chunk_duration_seconds = (sample_timespan.endtime - sample_timespan.starttime).total_seconds()

    samples_per_chunk = int(chunk_duration_seconds * sampling_rate)

    logger.info(
        f"RAM estimation: DatachunkParams id={datachunk_params.id}, "
        f"chunk_duration={chunk_duration_seconds}s, sampling_rate={sampling_rate}Hz, "
        f"samples_per_chunk={samples_per_chunk:,}"
    )

    # CCF output size (2 * max_lag + 1 samples)
    ccf_samples = 2 * params.correlation_max_lag_samples + 1

    # Memory per timespan estimation (in bytes)
    bytes_per_float = 8  # float64

    # 1. Datachunk data: num_components * samples_per_chunk * 8 bytes
    datachunk_mem_per_timespan = num_unique_components * samples_per_chunk * bytes_per_float

    # 2. CCF output: num_pairs * ccf_samples * 8 bytes
    ccf_mem_per_timespan = num_component_pairs * ccf_samples * bytes_per_float

    # 3. SQLAlchemy object overhead: ~2KB per CCF object + ~1KB per datachunk
    sqlalchemy_overhead_per_timespan = (num_component_pairs * 2048) + (num_unique_components * 1024)

    # 4. Dask serialization overhead (if parallel): ~2x for task data
    dask_multiplier = 2.5 if parallel else 1.0

    # Total memory per timespan (with all components)
    mem_per_timespan = (
        datachunk_mem_per_timespan + ccf_mem_per_timespan + sqlalchemy_overhead_per_timespan
    ) * dask_multiplier

    # Memory per component (for sub-timespan chunking calculation)
    # When we have N components, pairs grow as N*(N-1)/2, so mem scales ~quadratically
    # Per component: datachunk data + its share of pairs
    mem_per_component_datachunk = samples_per_chunk * bytes_per_float * dask_multiplier
    # Pairs per component (average): each component participates in ~(N-1) pairs
    # So per component CCF contribution: ~(N-1) * ccf_samples * 8 / 2 (divided by 2 because pair is shared)
    avg_pairs_per_component = (num_unique_components - 1) if num_unique_components > 1 else 1
    mem_per_component_ccf = avg_pairs_per_component * ccf_samples * bytes_per_float * dask_multiplier / 2
    mem_per_component = mem_per_component_datachunk + mem_per_component_ccf + 1536  # +1.5KB overhead

    # Add base Python/process overhead (~500MB)
    base_overhead = 500 * 1024**2

    # Calculate usable RAM
    usable_ram = (available_ram_bytes - base_overhead) * ram_safety_factor

    if usable_ram <= 0:
        logger.warning(f"Very low available RAM ({available_ram_bytes / 1024**3:.2f}GB).")
        usable_ram = 100 * 1024**2  # Minimum 100MB

    # Calculate optimal timespans_at_once
    optimal_timespans_raw = usable_ram / mem_per_timespan

    # Check if we need sub-timespan chunking (when even 1 timespan doesn't fit)
    needs_component_chunking = optimal_timespans_raw < 1.0
    components_per_batch = None

    if needs_component_chunking:
        # Calculate how many components we can process at once
        # Memory for N components in a single timespan:
        # = N * datachunk_mem + N*(N-1)/2 * ccf_mem + overhead
        # Solve for N given usable_ram

        # Simplified: assume linear scaling for conservative estimate
        # components_per_batch = usable_ram / mem_per_component
        components_per_batch_raw = usable_ram / mem_per_component
        components_per_batch = max(10, int(components_per_batch_raw))  # Minimum 10 components

        # Cap at total components (no chunking needed if we can fit all)
        if components_per_batch >= num_unique_components:
            components_per_batch = None
            needs_component_chunking = False
            optimal_timespans = 1
        else:
            optimal_timespans = 1  # Process 1 timespan at a time with component chunking

        logger.warning(
            f"RAM insufficient for full timespan processing!\n"
            f"  - Memory needed per full timespan: {mem_per_timespan / 1024**3:.2f}GB\n"
            f"  - Usable RAM: {usable_ram / 1024**3:.2f}GB\n"
            f"  - Enabling sub-timespan chunking with ~{components_per_batch} components per batch\n"
            f"  - This will require multiple passes per timespan to cover all component pairs"
        )
    else:
        # Normal case: can fit at least 1 full timespan
        optimal_timespans = int(optimal_timespans_raw)
        optimal_timespans = max(1, min(optimal_timespans, max_timespans, num_timespans))

    # Log estimation details
    logger.info(
        f"RAM estimation breakdown:\n"
        f"  - Samples per chunk: {samples_per_chunk:,}\n"
        f"  - CCF samples: {ccf_samples:,}\n"
        f"  - Unique components: {num_unique_components}\n"
        f"  - Component pairs: {num_component_pairs:,}\n"
        f"  - Memory per full timespan: {mem_per_timespan / 1024**2:.1f}MB\n"
        f"    - Datachunks: {datachunk_mem_per_timespan / 1024**2:.1f}MB\n"
        f"    - CCF outputs: {ccf_mem_per_timespan / 1024**2:.1f}MB\n"
        f"    - SQLAlchemy overhead: {sqlalchemy_overhead_per_timespan / 1024**2:.1f}MB\n"
        f"    - Dask multiplier: {dask_multiplier}x\n"
        f"  - Usable RAM: {usable_ram / 1024**3:.2f}GB (safety factor: {ram_safety_factor})\n"
        f"  - Recommended timespans_at_once: {optimal_timespans}\n"
        f"  - Sub-timespan chunking: {'YES - ' + str(components_per_batch) + ' components/batch' if needs_component_chunking else 'NO'}"
    )

    return MemoryEstimation(
        timespans_at_once=optimal_timespans,
        components_per_batch=components_per_batch,
        samples_per_chunk=samples_per_chunk,
        ccf_samples=ccf_samples,
        mem_per_timespan_full=mem_per_timespan,
        mem_per_component=mem_per_component,
        usable_ram=usable_ram,
        needs_component_chunking=needs_component_chunking,
    )


def perform_crosscorrelations_cartesian(
    crosscorrelation_cartesian_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    component_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    component_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
    include_autocorrelation: Optional[bool] = False,
    include_intracorrelation: Optional[bool] = False,
    only_autocorrelation: Optional[bool] = False,
    only_intracorrelation: Optional[bool] = False,
    raise_errors: bool = False,
    batch_size: int = 5000,
    parallel: bool = True,
    verbose_ccf_logging: bool = False,
    overwrite: bool = True,
    timespans_at_once: Optional[int] = None,
    ram_safety_factor: float = 0.5,
    restart_dask_every_n_timespans: Optional[int] = None,
) -> None:
    """
    Performs crosscorrelations_cartesian according to provided set of selectors.
    Uses Dask.distributed for parallelism.
    All the calculations are divided into batches in order to speed up queries that gather all the inputs.

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams object to be used as config
    :type crosscorrelation_cartesian_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param network_codes_a: Selector for network code of A station in the pair
    :type network_codes_a: Optional[Union[Collection[str], str]]
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]]
    :param component_codes_a: Selector for component code of A station in the pair
    :type component_codes_a: Optional[Union[Collection[str], str]]
    :param network_codes_b: Selector for network code of B station in the pair
    :type network_codes_b: Optional[Union[Collection[str], str]]
    :param station_codes_b: Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]]
    :param component_codes_b: Selector for component code of B station in the pair
    :type component_codes_b: Optional[Union[Collection[str], str]]
    :param accepted_component_code_pairs: Collection of component code pairs that should be fetched
    :type accepted_component_code_pairs: Optional[Union[Collection[str], str]]
    :param include_autocorrelation: If autocorrelation pairs should be also included
    :type include_autocorrelation: Optional[bool]
    :param include_intracorrelation: If intracorrelation pairs should be also included
    :type include_intracorrelation: Optional[bool]
    :param only_autocorrelation: If only autocorrelation pairs should be selected
    :type only_autocorrelation: Optional[bool]
    :param only_intracorrelation: If only intracorrelation pairs should be selected
    :type only_intracorrelation: Optional[bool]
    :param raise_errors: If errors should be raised or just logged
    :type raise_errors: bool
    :param batch_size: Number of component pairs per Dask task (controls parallelization granularity)
    :type batch_size: int
    :param parallel: If the calculations should be done in parallel
    :type parallel: bool
    :param verbose_ccf_logging: If True, log every individual CCF file write. If False (default in parallel), only log batch progress.
    :type verbose_ccf_logging: bool
    :param overwrite: If True, delete existing DB records before insert. If False, skip records that already exist in DB.
    :type overwrite: bool
    :param timespans_at_once: Number of timespans to process in each batch. If None, automatically estimated based on available RAM.
    :type timespans_at_once: Optional[int]
    :param ram_safety_factor: Fraction of available RAM to use for estimation (0.0-1.0). Default 0.5 (50%).
    :type ram_safety_factor: float
    :param restart_dask_every_n_timespans: Restart Dask client every N timespans to fully release worker memory. If None, defaults to 10 in sub-timespan mode. Set to 0 to disable.
    :type restart_dask_every_n_timespans: Optional[int]
    :return: None
    :rtype: NoneType
    """

    import gc  # DEBUG_MEMORY
    import psutil  # DEBUG_MEMORY
    import time as time_module  # DEBUG_TIMING

    # Track memory across iterations for leak detection
    _memory_baseline: Optional[float] = None
    _memory_previous: Optional[float] = None
    _group_start_time: Optional[float] = None  # DEBUG_TIMING

    def _clear_scipy_caches() -> None:
        """Clear scipy internal caches to prevent memory accumulation."""
        try:
            # Clear scipy.fft cache (FFTW plans)
            from scipy import fft

            if hasattr(fft, "_pocketfft") and hasattr(fft._pocketfft, "pypocketfft"):
                pass  # pocketfft doesn't have explicit cache clearing
            # Clear scipy.fftpack if used
            try:
                from scipy import fftpack

                if hasattr(fftpack, "fftpack") and hasattr(fftpack.fftpack, "_fftpack"):
                    pass  # fftpack caches are per-size
            except ImportError:
                pass
        except Exception as e:
            logger.debug(f"Could not clear scipy caches: {e}")

        try:
            # Clear numpy FFT cache
            import numpy as np

            if hasattr(np.fft, "_pocketfft") and hasattr(np.fft._pocketfft, "pypocketfft"):
                pass
        except Exception as e:
            logger.debug(f"Could not clear numpy FFT caches: {e}")

        try:
            # Clear any linecache (used by traceback/logging)
            import linecache

            linecache.clearcache()
        except Exception as e:
            logger.debug(f"Could not clear linecache: {e}")

    def _aggressive_memory_cleanup() -> None:
        """Perform aggressive memory cleanup including Python caches."""
        # 1. Clear SQLAlchemy session completely
        db.session.expire_all()
        db.session.expunge_all()
        # Also remove from local session cache if using scoped session
        try:
            db.session.remove()  # For scoped_session
        except Exception:
            pass

        # 2. Clear scipy/numpy caches
        _clear_scipy_caches()

        # 3. Force multiple GC passes
        gc.collect(0)  # Young generation
        gc.collect(1)  # Middle generation
        gc.collect(2)  # Old generation
        gc.collect()  # Full collection

        # 4. Try to return memory to OS (Linux-specific)
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim(0)
            logger.debug("Called malloc_trim(0) to return memory to OS")
        except Exception as e:
            logger.debug(f"Could not call malloc_trim: {e}")

    def _debug_log_memory(label: str, detailed: bool = False) -> None:  # DEBUG_MEMORY
        """DEBUG_MEMORY: Log current memory usage with optional leak detection"""
        nonlocal _memory_baseline, _memory_previous

        process = psutil.Process()
        mem_info = process.memory_info()
        rss_gb = mem_info.rss / 1024**3
        vms_gb = mem_info.vms / 1024**3

        # Track baseline and deltas
        if _memory_baseline is None:
            _memory_baseline = rss_gb

        delta_from_baseline = rss_gb - _memory_baseline
        delta_from_previous = rss_gb - _memory_previous if _memory_previous is not None else 0.0
        _memory_previous = rss_gb

        # Basic log
        logger.warning(
            f"DEBUG_MEMORY [{label}]: RSS={rss_gb:.2f}GB, VMS={vms_gb:.2f}GB | "
            f"Δbaseline={delta_from_baseline:+.3f}GB, Δprev={delta_from_previous:+.3f}GB"
        )

        # Detailed diagnostics (SQLAlchemy identity map, etc.)
        if detailed:
            identity_map_size = len(db.session.identity_map)
            logger.warning(f"DEBUG_MEMORY [{label}] DETAIL: SQLAlchemy identity_map size={identity_map_size}")

    timespan_list = fetch_timespans_between_dates(starttime, endtime)

    # Pre-fetch component pairs to estimate RAM requirements
    if accepted_component_code_pairs is not None:
        validated_pairs = validate_component_code_pairs(
            component_pairs_cartesian=validate_to_tuple(accepted_component_code_pairs, str)
        )
    else:
        validated_pairs = None

    prefetch_component_pairs = fetch_componentpairs_cartesian(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=validated_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )
    num_component_pairs = len(prefetch_component_pairs)
    num_unique_components = len(extract_component_ids_from_component_pairs_cartesian(prefetch_component_pairs))

    # Extract unique component IDs for potential sub-timespan chunking
    all_component_ids = list(extract_component_ids_from_component_pairs_cartesian(prefetch_component_pairs))

    # Estimate or use provided timespans_at_once
    components_per_batch: Optional[int] = None
    if timespans_at_once is None:
        memory_estimation = _estimate_timespans_at_once(
            crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
            num_timespans=len(timespan_list),
            num_component_pairs=num_component_pairs,
            num_unique_components=num_unique_components,
            parallel=parallel,
            ram_safety_factor=ram_safety_factor,
        )
        timespans_at_once = memory_estimation.timespans_at_once
        components_per_batch = memory_estimation.components_per_batch
    else:
        logger.info(f"Using user-provided timespans_at_once={timespans_at_once}")

    # Check if we need sub-timespan chunking (component batching)
    if components_per_batch is not None:
        # SUB-TIMESPAN CHUNKING MODE
        # Process 1 timespan at a time, but split components into batches
        # to reduce memory footprint

        # Set default for restart interval in sub-timespan mode (every 10 timespans)
        if restart_dask_every_n_timespans is None:
            restart_dask_every_n_timespans = 10

        logger.warning(
            f"=== SUB-TIMESPAN CHUNKING ENABLED ===\n"
            f"Processing {len(timespan_list)} timespans with {num_unique_components} components\n"
            f"Components will be split into batches of ~{components_per_batch} for memory efficiency\n"
            f"Each timespan will require multiple passes to cover all component pairs\n"
            f"Dask client will restart every {restart_dask_every_n_timespans} timespan(s) for memory safety"
        )

        # Create component batches
        component_batches = list(more_itertools.chunked(all_component_ids, components_per_batch))
        num_component_batches = len(component_batches)

        # For cross-batch pairs, we need to process pairs between different batches too
        # Strategy: for each pair of component batches (including self-pairs), process that combination
        # This ensures all pairs are covered: batch1×batch1, batch1×batch2, batch2×batch2, etc.

        _debug_log_memory("before_subtimespan_loop")  # DEBUG_MEMORY

        total_iterations = len(timespan_list) * (num_component_batches * (num_component_batches + 1) // 2)
        iteration_counter = 0

        # Create persistent Dask client for sub-timespan mode (reused across sub-batches)
        dask_client = None
        dask_cluster = None
        dask_n_workers = None
        if parallel:
            dask_client, dask_cluster, dask_n_workers = create_process_dask_client()
            logger.info(f"Dask client started for sub-timespan mode. Dashboard: {dask_client.dashboard_link}")

        for ts_idx, timespan in enumerate(timespan_list):
            logger.info(f"Processing timespan {ts_idx + 1}/{len(timespan_list)}: {timespan}")

            # Periodic Dask restart for memory safety
            if parallel and dask_client is not None and restart_dask_every_n_timespans > 0:
                if ts_idx > 0 and ts_idx % restart_dask_every_n_timespans == 0:
                    logger.warning(
                        f"=== PERIODIC DASK RESTART at timespan {ts_idx} (every {restart_dask_every_n_timespans}) ==="
                    )
                    _debug_log_memory(f"before_dask_restart_ts_{ts_idx}")

                    dask_client, dask_cluster, _ = recreate_process_dask_client(
                        client=dask_client,
                        cluster=dask_cluster,
                        n_workers=dask_n_workers,
                    )
                    gc.collect()

                    _debug_log_memory(f"after_dask_restart_ts_{ts_idx}")
                    logger.warning("=== DASK RESTART COMPLETE ===")

            # Process all component batch combinations for this timespan
            for batch_i_idx in range(num_component_batches):
                for batch_j_idx in range(batch_i_idx, num_component_batches):
                    iteration_counter += 1
                    batch_i = set(component_batches[batch_i_idx])
                    batch_j = set(component_batches[batch_j_idx])
                    combined_components = batch_i | batch_j

                    filtered_pairs = _filter_component_pairs_for_component_batches(
                        component_pairs=prefetch_component_pairs,
                        batch_i_component_ids=batch_i,
                        batch_j_component_ids=batch_j,
                    )

                    if not filtered_pairs:
                        continue

                    logger.info(
                        f"  Sub-batch {iteration_counter}/{total_iterations}: "
                        f"component batches ({batch_i_idx + 1},{batch_j_idx + 1}) with {len(filtered_pairs)} pairs, "
                        f"{len(combined_components)} components"
                    )

                    _debug_log_memory(f"subtimespan_iter_{iteration_counter}")  # DEBUG_MEMORY

                    # Create filtered component IDs for this sub-batch
                    filtered_component_ids = list(combined_components)

                    calculation_inputs = _prepare_inputs_for_crosscorrelations_cartesian_filtered(
                        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
                        timespan=timespan,
                        component_pairs=filtered_pairs,
                        component_ids=filtered_component_ids,
                        pairs_per_task=batch_size,
                        verbose_ccf_logging=verbose_ccf_logging,
                        overwrite=overwrite,
                    )

                    if parallel:
                        dask_submission_batch_size = _resolve_ccf_dask_submission_batch_size(
                            pairs_per_task=batch_size,
                            dask_n_workers=dask_n_workers,
                        )
                        _run_calculate_and_upsert_on_dask(
                            batch_size=dask_submission_batch_size,
                            inputs=calculation_inputs,
                            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
                            upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
                            raise_errors=raise_errors,
                            overwrite=overwrite,
                            existing_client=dask_client,  # Reuse persistent client
                        )
                    else:
                        _run_calculate_and_upsert_sequentially(
                            batch_size=batch_size,
                            inputs=calculation_inputs,
                            calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
                            upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
                            raise_errors=raise_errors,
                            with_file=True,
                            overwrite=overwrite,
                        )

                    # Cleanup after each sub-batch
                    del calculation_inputs
                    db.session.expire_all()
                    db.session.expunge_all()
                    gc.collect()

            logger.info(f"=== COMPLETED timespan {ts_idx + 1}/{len(timespan_list)} ===")
            _debug_log_memory(f"after_timespan_{ts_idx}", detailed=True)  # DEBUG_MEMORY with details

        # Close persistent Dask client
        if dask_client is not None:
            logger.info("Closing Dask client after sub-timespan processing complete")
            dask_client.close()
            if dask_cluster is not None:
                dask_cluster.close()
            dask_client = None
            dask_cluster = None

        # Final cleanup
        del timespan_list
        del prefetch_component_pairs
        del all_component_ids
        del component_batches
        db.session.expire_all()
        db.session.expunge_all()
        gc.collect()

        return

    # NORMAL MODE: Process multiple timespans at once (original behavior)
    # In normal mode, restart_dask_every_n_timespans applies to timespan GROUPS, not individual timespans
    # If restart_dask_every_n_timespans is set, we'll restart between groups
    timespan_groups = list(more_itertools.chunked(timespan_list, timespans_at_once))
    logger.info(
        f"Will process {len(timespan_list)} timespans in {len(timespan_groups)} groups of up to {timespans_at_once} timespans each"
    )

    if restart_dask_every_n_timespans is not None and restart_dask_every_n_timespans > 0:
        logger.info(
            f"Dask client will restart every {restart_dask_every_n_timespans} timespan group(s) for memory safety"
        )

    _debug_log_memory("before_loop")  # DEBUG_MEMORY

    # Create persistent Dask client for normal mode if restart is configured
    dask_client = None
    dask_cluster = None
    dask_n_workers = None
    if parallel and restart_dask_every_n_timespans is not None and restart_dask_every_n_timespans > 0:
        dask_client, dask_cluster, dask_n_workers = create_process_dask_client()
        logger.info(
            f"Dask client started for normal mode with periodic restart. Dashboard: {dask_client.dashboard_link}"
        )

    for group_idx, timespans_i in enumerate(timespan_groups):
        _group_start_time = time_module.time()  # DEBUG_TIMING

        logger.info(
            f"Processing timespan group {group_idx + 1}/{len(timespan_groups)} with {len(timespans_i)} timespan(s)"
        )

        # Periodic Dask restart for memory safety (in normal mode, applies to groups)
        if (
            parallel
            and dask_client is not None
            and restart_dask_every_n_timespans is not None
            and restart_dask_every_n_timespans > 0
        ):
            if group_idx > 0 and group_idx % restart_dask_every_n_timespans == 0:
                logger.warning(
                    f"=== PERIODIC DASK RESTART at group {group_idx} (every {restart_dask_every_n_timespans} groups) ==="
                )
                _debug_log_memory(f"before_dask_restart_group_{group_idx}")

                dask_client, dask_cluster, _ = recreate_process_dask_client(
                    client=dask_client,
                    cluster=dask_cluster,
                    n_workers=dask_n_workers,
                )
                _aggressive_memory_cleanup()  # Use aggressive cleanup after Dask restart

                _debug_log_memory(f"after_dask_restart_group_{group_idx}")
                logger.warning("=== DASK RESTART COMPLETE ===")

        _debug_log_memory(f"loop_start_group_{group_idx}")  # DEBUG_MEMORY

        calculation_inputs = _prepare_inputs_for_crosscorrelations_cartesian(
            crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
            timespans=timespans_i,
            network_codes_a=network_codes_a,
            station_codes_a=station_codes_a,
            component_codes_a=component_codes_a,
            network_codes_b=network_codes_b,
            station_codes_b=station_codes_b,
            component_codes_b=component_codes_b,
            accepted_component_code_pairs=accepted_component_code_pairs,
            include_autocorrelation=include_autocorrelation,
            include_intracorrelation=include_intracorrelation,
            only_autocorrelation=only_autocorrelation,
            only_intracorrelation=only_intracorrelation,
            pairs_per_task=batch_size,
            verbose_ccf_logging=verbose_ccf_logging,
            overwrite=overwrite,
        )

        if parallel:
            dask_submission_batch_size = _resolve_ccf_dask_submission_batch_size(
                pairs_per_task=batch_size,
                dask_n_workers=dask_n_workers,
            )
            _run_calculate_and_upsert_on_dask(
                batch_size=dask_submission_batch_size,
                inputs=calculation_inputs,
                calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
                upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
                raise_errors=raise_errors,
                overwrite=overwrite,
                existing_client=dask_client,  # Reuse persistent client if configured
            )
        else:
            _run_calculate_and_upsert_sequentially(
                batch_size=batch_size,
                inputs=calculation_inputs,
                calculation_task=_crosscorrelate_for_timespan_wrapper,  # type: ignore
                upserter_callable=_prepare_upsert_command_crosscorrelation_cartesian,
                raise_errors=raise_errors,
                with_file=True,
                overwrite=overwrite,
            )

        # Ensure all DB operations are complete before moving to next group
        group_elapsed = time_module.time() - _group_start_time  # DEBUG_TIMING
        logger.info(
            f"=== COMPLETED timespan group {group_idx + 1}/{len(timespan_groups)} in {group_elapsed:.1f}s ({group_elapsed / 60:.1f}min) ==="
        )

        # DEBUG_MEMORY: Explicit cleanup after each timespan group
        _debug_log_memory(f"after_processing_group_{group_idx}")  # DEBUG_MEMORY
        del calculation_inputs  # DEBUG_MEMORY

        # MEMORY_FIX: Use aggressive cleanup to release memory more effectively
        _aggressive_memory_cleanup()
        logger.info(f"Completed aggressive memory cleanup after group {group_idx + 1}")

        _debug_log_memory(f"after_gc_group_{group_idx}")  # DEBUG_MEMORY

    # Close persistent Dask client if we created one
    if dask_client is not None:
        logger.info("Closing Dask client after normal mode processing complete")
        dask_client.close()
        if dask_cluster is not None:
            dask_cluster.close()
        dask_client = None
        dask_cluster = None

    # MEMORY_FIX: Final cleanup of objects fetched at function start
    del timespan_list
    del timespan_groups
    del prefetch_component_pairs
    del all_component_ids
    _aggressive_memory_cleanup()  # Use aggressive cleanup at end

    return


def _prepare_inputs_for_crosscorrelations_cartesian_filtered(
    crosscorrelation_cartesian_params_id: int,
    timespan: Timespan,
    component_pairs: List[ComponentPairCartesian],
    component_ids: List[int],
    pairs_per_task: Optional[int] = None,
    verbose_ccf_logging: bool = False,
    overwrite: bool = True,
) -> Generator[CrosscorrelationCartesianRunnerInputs, None, None]:
    """
    Prepare inputs for crosscorrelation processing with pre-filtered components.

    This is used for sub-timespan chunking when memory is limited. It only loads
    ProcessedDatachunk data for the specified component_ids, reducing memory footprint.

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams
    :param timespan: Single Timespan to process
    :param component_pairs: Pre-filtered list of component pairs to process
    :param component_ids: List of component IDs to load data for
    :param pairs_per_task: Number of pairs per yielded item
    :param verbose_ccf_logging: If True, enable verbose logging
    :param overwrite: If False, skip existing CCF records
    :return: Generator of CrosscorrelationCartesianRunnerInputs
    """
    from noiz.processing.crosscorrelations import group_chunks_by_timespanid_componentid

    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)

    # When overwrite=False, filter out pairs that already have CCF records
    if not overwrite:
        existing_query = db.session.query(CrosscorrelationCartesian.componentpair_id).filter(
            CrosscorrelationCartesian.timespan_id == timespan.id,
            CrosscorrelationCartesian.crosscorrelation_cartesian_params_id == crosscorrelation_cartesian_params_id,
        )
        existing_pair_ids = frozenset(row.componentpair_id for row in existing_query.all())
        original_count = len(component_pairs)
        component_pairs = [p for p in component_pairs if p.id not in existing_pair_ids]
        if len(component_pairs) < original_count:
            logger.info(
                f"overwrite=False: Filtered {original_count - len(component_pairs)} existing pairs, {len(component_pairs)} remaining"
            )
        if not component_pairs:
            return

    # Fetch ProcessedDatachunks ONLY for the specified component_ids (memory optimization)
    fetched_processed_datachunks = (
        db.session.query(Timespan, ProcessedDatachunk)
        .join(Datachunk, Timespan.id == Datachunk.timespan_id)
        .join(ProcessedDatachunk, Datachunk.id == ProcessedDatachunk.datachunk_id)
        .filter(
            Timespan.id == timespan.id,
            ProcessedDatachunk.processed_datachunk_params_id == params.processed_datachunk_params_id,
            Datachunk.component_id.in_(component_ids),  # Only specified components!
        )
        .options(subqueryload(ProcessedDatachunk.datachunk))
        .all()
    )

    logger.debug(
        f"Fetched {len(fetched_processed_datachunks)} ProcessedDatachunks for timespan {timespan.id} with {len(component_ids)} component IDs"
    )

    if not fetched_processed_datachunks:
        logger.warning(
            f"No ProcessedDatachunks found for timespan {timespan.id} with specified components (component_ids sample: {component_ids[:5]}...)"
        )
        return

    # Group by timespan - but use timespan.id as key to avoid object identity issues
    # The timespan object passed in may be different from the one returned by the query
    grouped_processed_chunks: Dict[int, ProcessedDatachunk] = {}
    for _, chunk in fetched_processed_datachunks:
        grouped_processed_chunks[chunk.datachunk.component_id] = chunk

    if not grouped_processed_chunks:
        logger.warning(f"No ProcessedDatachunks could be grouped for timespan {timespan.id}")
        return

    logger.debug(
        f"Grouped {len(grouped_processed_chunks)} ProcessedDatachunks by component_id for timespan {timespan.id}"
    )

    # Chunk component pairs for Dask tasks
    if pairs_per_task is None or pairs_per_task >= len(component_pairs):
        component_pair_chunks = [tuple(component_pairs)]
    else:
        component_pair_chunks = [tuple(chunk) for chunk in more_itertools.chunked(component_pairs, pairs_per_task)]

    total_items = len(component_pair_chunks)

    for chunk_idx, pair_chunk in enumerate(component_pair_chunks):
        db.session.expunge_all()
        yield CrosscorrelationCartesianRunnerInputs(
            timespan=timespan,
            crosscorrelation_cartesian_params=params,
            grouped_processed_chunks=grouped_processed_chunks,
            component_pairs_cartesian=pair_chunk,
            verbose_ccf_logging=verbose_ccf_logging,
            batch_index=chunk_idx + 1,
            total_batches=total_items,
        )


def _prepare_inputs_for_crosscorrelations_cartesian(
    crosscorrelation_cartesian_params_id: int,
    timespans: List[Timespan],
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    component_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    component_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
    include_autocorrelation: Optional[bool] = False,
    include_intracorrelation: Optional[bool] = False,
    only_autocorrelation: Optional[bool] = False,
    only_intracorrelation: Optional[bool] = False,
    pairs_per_task: Optional[int] = None,
    verbose_ccf_logging: bool = False,
    overwrite: bool = True,
) -> Generator[CrosscorrelationCartesianRunnerInputs, None, None]:
    """
    Performs all the database queries to prepare all the data required for running crosscorrelations_cartesian.
    Returns a tuple of inputs specific for the further calculations.

    :param crosscorrelation_cartesian_params_id: ID of CrosscorrelationCartesianParams object to use
    :type crosscorrelation_cartesian_params_id: int
    :param timespans: List of Timespan objects to process
    :type timespans: List[Timespan]
    :param network_codes_a: Selector for network code of A station in the pair
    :type network_codes_a: Optional[Union[Collection[str], str]]
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]]
    :param component_codes_a: Selector for component code of A station in the pair
    :type component_codes_a: Optional[Union[Collection[str], str]]
    :param network_codes_b: Selector for network code of B station in the pair
    :type network_codes_b: Optional[Union[Collection[str], str]]
    :param station_codes_b: Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]]
    :param component_codes_b: Selector for component code of B station in the pair
    :type component_codes_b: Optional[Union[Collection[str], str]]
    :param include_autocorrelation: If autocorrelation pairs should be also included
    :type include_autocorrelation: Optional[bool]
    :param include_intracorrelation: If intracorrelation pairs should be also included
    :type include_intracorrelation: Optional[bool]
    :param only_autocorrelation: If only autocorrelation pairs should be selected
    :type only_autocorrelation: Optional[bool]
    :param only_intracorrelation: If only intracorrelation pairs should be selected
    :type only_intracorrelation: Optional[bool]
    :param pairs_per_task: Number of component pairs per yielded item (for parallelization). If None, all pairs in one item.
    :type pairs_per_task: Optional[int]
    :param overwrite: If False, filter out component pairs that already have CCF records for each timespan.
    :type overwrite: bool
    :return:
    :rtype:
    """

    # Use the timespans passed directly instead of re-querying
    fetched_timespans = timespans
    fetched_timespans_ids = extract_object_ids(fetched_timespans)
    logger.info(f"There are {len(fetched_timespans_ids)} timespan(s) to process: {fetched_timespans_ids}")

    if accepted_component_code_pairs is not None:
        accepted_component_code_pairs = validate_component_code_pairs(
            component_pairs_cartesian=validate_to_tuple(accepted_component_code_pairs, str)
        )

    fetched_component_pairs: List[ComponentPairCartesian] = fetch_componentpairs_cartesian(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )
    logger.info(f"There are {len(fetched_component_pairs)} component pairs to process.")

    single_component_ids = extract_component_ids_from_component_pairs_cartesian(fetched_component_pairs)
    logger.info(f"There are in total {len(single_component_ids)} unique components to be fetched from db.")

    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)
    logger.info(f"Fetched correlation_params object {params}")

    # When overwrite=False, query existing CCF records to skip already-processed pairs
    existing_ccf_keys: Dict[int, FrozenSet[int]] = {}  # timespan_id -> frozenset of componentpair_ids
    if not overwrite:
        logger.info("overwrite=False: Querying existing CCF records to skip already-processed pairs...")
        existing_query = db.session.query(
            CrosscorrelationCartesian.timespan_id,
            CrosscorrelationCartesian.componentpair_id,
        ).filter(
            CrosscorrelationCartesian.timespan_id.in_(fetched_timespans_ids),
            CrosscorrelationCartesian.crosscorrelation_cartesian_params_id == crosscorrelation_cartesian_params_id,
        )
        # Build with mutable sets first, then convert to frozensets
        _existing_ccf_keys_building: Dict[int, Set[int]] = {}
        for row in existing_query.all():
            if row.timespan_id not in _existing_ccf_keys_building:
                _existing_ccf_keys_building[row.timespan_id] = set()
            _existing_ccf_keys_building[row.timespan_id].add(row.componentpair_id)
        # Convert to frozensets for immutability
        existing_ccf_keys = {k: frozenset(v) for k, v in _existing_ccf_keys_building.items()}
        total_existing = sum(len(v) for v in existing_ccf_keys.values())
        logger.info(f"Found {total_existing} existing CCF records across {len(existing_ccf_keys)} timespans")

    fetched_processed_datachunks = (
        db.session.query(Timespan, ProcessedDatachunk)
        .join(Datachunk, Timespan.id == Datachunk.timespan_id)
        .join(ProcessedDatachunk, Datachunk.id == ProcessedDatachunk.datachunk_id)
        .filter(
            Timespan.id.in_(fetched_timespans_ids),  # type: ignore
            ProcessedDatachunk.processed_datachunk_params_id == params.processed_datachunk_params_id,
            Datachunk.component_id.in_(single_component_ids),
        )
        .options(subqueryload(ProcessedDatachunk.datachunk))
        .all()
    )
    grouped_datachunks = group_chunks_by_timespanid_componentid(processed_datachunks=fetched_processed_datachunks)

    # Determine chunking strategy for component pairs
    if pairs_per_task is None or pairs_per_task >= len(fetched_component_pairs):
        # Original behavior: all pairs in one task per timespan
        component_pair_chunks = [tuple(fetched_component_pairs)]
        logger.warning(
            f"DEBUG_BATCH: pairs_per_task={pairs_per_task}, using all {len(fetched_component_pairs)} pairs per task (original behavior)"
        )
    else:
        # New behavior: split pairs into chunks for better parallelization
        component_pair_chunks_temp = list(more_itertools.chunked(fetched_component_pairs, pairs_per_task))
        component_pair_chunks = [tuple(chunk) for chunk in component_pair_chunks_temp]
        logger.info(
            f"Splitting {len(fetched_component_pairs)} component pairs into {len(component_pair_chunks)} chunks of ~{pairs_per_task} pairs each"
        )

    # Calculate total items to yield (estimate - may be less if filtering existing)
    total_items = len(grouped_datachunks) * len(component_pair_chunks)
    logger.warning(
        f"DEBUG_BATCH: Will yield up to {total_items} items total ({len(grouped_datachunks)} timespans x {len(component_pair_chunks)} pair chunks)"
    )
    logger.warning(
        f"DEBUG_BATCH: Each Dask task will process ~{pairs_per_task if pairs_per_task else len(fetched_component_pairs)} component pairs"
    )

    item_counter = 0
    skipped_pairs_total = 0
    for timespan, grouped_processed_chunks in grouped_datachunks.items():
        # When overwrite=False, filter out component pairs that already have CCF records for this timespan
        if not overwrite and timespan.id in existing_ccf_keys:
            existing_pair_ids = existing_ccf_keys[timespan.id]
            filtered_pairs = [p for p in fetched_component_pairs if p.id not in existing_pair_ids]
            skipped_count = len(fetched_component_pairs) - len(filtered_pairs)
            skipped_pairs_total += skipped_count
            if skipped_count > 0:
                logger.info(
                    f"overwrite=False: Skipping {skipped_count} already-processed pairs for timespan {timespan.id}, {len(filtered_pairs)} pairs remaining"
                )
            if not filtered_pairs:
                logger.info(
                    f"overwrite=False: All pairs already processed for timespan {timespan.id}, skipping entirely"
                )
                continue
            # Re-chunk the filtered pairs
            if pairs_per_task is None or pairs_per_task >= len(filtered_pairs):
                timespan_pair_chunks = [tuple(filtered_pairs)]
            else:
                timespan_pair_chunks = [
                    tuple(chunk) for chunk in more_itertools.chunked(filtered_pairs, pairs_per_task)
                ]
        else:
            timespan_pair_chunks = component_pair_chunks

        for chunk_idx, pair_chunk in enumerate(timespan_pair_chunks):
            db.session.expunge_all()
            item_counter += 1
            logger.debug(
                f"DEBUG_BATCH: Yielding item {item_counter} for timespan {timespan.id}, chunk {chunk_idx + 1}/{len(timespan_pair_chunks)} with {len(pair_chunk)} pairs"
            )
            yield CrosscorrelationCartesianRunnerInputs(
                timespan=timespan,
                crosscorrelation_cartesian_params=params,
                grouped_processed_chunks=grouped_processed_chunks,
                component_pairs_cartesian=pair_chunk,
                verbose_ccf_logging=verbose_ccf_logging,
                batch_index=item_counter,
                total_batches=total_items,  # Note: this is an estimate
            )

    if not overwrite and skipped_pairs_total > 0:
        logger.info(
            f"overwrite=False: Skipped {skipped_pairs_total} already-processed (timespan, pair) combinations in total"
        )
    return


def _crosscorrelate_for_timespan_wrapper(
    inputs: CrosscorrelationCartesianRunnerInputs,
) -> Tuple[CrosscorrelationCartesian, ...]:
    """
    Thin wrapper around :py:meth:`noiz.api.crosscorrelations._crosscorrelate_for_timespan` translating
    single input TypedDict to standard keyword arguments and converting output to a Tuple.

    :param inputs: Input dictionary
    :type inputs: ~noiz.api.type_aliases.CrosscorrelationCartesianRunnerInputs
    :return: Finished CrosscorrelationCartesians in form of tuple
    :rtype: Tuple[~noiz.models.crosscorrelation.CrosscorrelationCartesian, ...]
    """
    return tuple(
        _crosscorrelate_for_timespan(
            timespan=inputs["timespan"],
            params=inputs["crosscorrelation_cartesian_params"],
            grouped_processed_chunks=inputs["grouped_processed_chunks"],
            component_pairs_cartesian=inputs["component_pairs_cartesian"],
            verbose_ccf_logging=inputs.get("verbose_ccf_logging", False),
            batch_index=inputs.get("batch_index", 1),
            total_batches=inputs.get("total_batches", 1),
        )
    )


def assembly_ccf_filename(component_pair_cartesian: ComponentPairCartesian, timespan: Timespan, count: int = 0) -> str:
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    filename = ".".join(
        [
            component_pair_cartesian.component_a.network,
            component_pair_cartesian.component_a.station,
            component_pair_cartesian.component_a.component,
            component_pair_cartesian.component_b.network,
            component_pair_cartesian.component_b.station,
            component_pair_cartesian.component_b.component,
            year,
            doy_time,
            str(count),
            "npy",
        ]
    )

    return filename


def assembly_ccf_dir(component_pair_cartesian: ComponentPairCartesian, timespan: Timespan) -> Path:
    """
    Assembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component_pair_cartesian: Component object containing information about used channel
    :type component_pair_cartesian: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Path object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(str(timespan.starttime.month))
        .joinpath(component_pair_cartesian.component_code_pair)
        .joinpath(
            f"{component_pair_cartesian.component_a.network}.{component_pair_cartesian.component_a.station}-"
            f"{component_pair_cartesian.component_b.network}.{component_pair_cartesian.component_b.station}"
        )
    )


def _crosscorrelate_for_timespan(
    timespan: Timespan,
    params: CrosscorrelationCartesianParams,
    grouped_processed_chunks: Dict[int, ProcessedDatachunk],
    component_pairs_cartesian: Tuple[ComponentPairCartesian, ...],
    verbose_ccf_logging: bool = False,
    batch_index: int = 1,
    total_batches: int = 1,
) -> List[CrosscorrelationCartesian]:
    """filldocs"""
    from noiz.globals import PROCESSED_DATA_DIR
    from noiz.processing.path_helpers import (
        assembly_filepath,
        increment_filename_counter,
        parent_directory_exists_or_create,
    )

    import numpy as np

    logger.info(
        f"Processing batch n°{batch_index} out of {total_batches}: crosscorrelation_cartesian for {timespan} with {len(component_pairs_cartesian)} pairs"
    )

    if verbose_ccf_logging:
        logger.debug(f"Loading data for timespan {timespan}")
    try:
        streams = load_data_for_chunks(chunks=grouped_processed_chunks)
    except CorruptedDataException as e:
        logger.error(e)
        raise CorruptedDataException(e) from e
    xcorrs = []
    for pair in component_pairs_cartesian:
        cmp_a_id = pair.component_a_id
        cmp_b_id = pair.component_b_id

        if cmp_a_id not in grouped_processed_chunks.keys() or cmp_b_id not in grouped_processed_chunks.keys():
            if verbose_ccf_logging:
                logger.debug(f"No data for pair {pair}")
            continue

        if verbose_ccf_logging:
            logger.debug(f"Processed chunks for {pair} are present. Starting processing.")

        if streams[cmp_a_id].data.shape != streams[cmp_b_id].data.shape:
            msg = (
                f"The shapes of data arrays for {cmp_a_id} and {cmp_b_id} are different. "
                f"Shapes: {cmp_a_id} is {streams[cmp_a_id].data.shape} "
                f"{cmp_b_id} is {streams[cmp_b_id].data.shape} "
            )
            logger.error(msg)
            raise InconsistentDataException(msg)

        ccf_data = correlate(
            a=streams[cmp_a_id],
            b=streams[cmp_b_id],
            shift=params.correlation_max_lag_samples,
        )

        filepath = assembly_filepath(
            PROCESSED_DATA_DIR,  # type: ignore
            "ccf",
            assembly_ccf_dir(component_pair_cartesian=pair, timespan=timespan).joinpath(
                assembly_ccf_filename(component_pair_cartesian=pair, timespan=timespan, count=0)
            ),
        )

        if filepath.exists():
            if verbose_ccf_logging:
                logger.debug(f"Filepath {filepath} exists. Trying to find next free one.")
            filepath = increment_filename_counter(filepath=filepath, extension=True)
            if verbose_ccf_logging:
                logger.debug(f"Free filepath found. CCF will be saved to {filepath}")

        if verbose_ccf_logging:
            logger.info(f"CCF will be written to {str(filepath)}")
        parent_directory_exists_or_create(filepath, verbose=verbose_ccf_logging)

        ccf_file = CrosscorrelationCartesianFile(filepath=str(filepath))

        np.save(file=ccf_file.filepath, arr=ccf_data)

        xcorr = CrosscorrelationCartesian(
            crosscorrelation_cartesian_params_id=params.id,
            componentpair_id=pair.id,
            timespan_id=timespan.id,
            file=ccf_file,
        )

        xcorrs.append(xcorr)
    return xcorrs


def fetch_crosscorrelations_cartesian_and_save(
    crosscorrelation_cartesian_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    dirpath: Path,
    overwrite: bool = False,
    network_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    component_codes_a: Optional[Union[Collection[str], str]] = None,
    network_codes_b: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    component_codes_b: Optional[Union[Collection[str], str]] = None,
    accepted_component_code_pairs: Optional[Union[Collection[str], str]] = None,
    include_autocorrelation: Optional[bool] = False,
    include_intracorrelation: Optional[bool] = False,
    only_autocorrelation: Optional[bool] = False,
    only_intracorrelation: Optional[bool] = False,
):
    component_pairs_cartesian = fetch_componentpairs_cartesian(
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )

    logger.info(f"Found {len(component_pairs_cartesian)} for provided parameters.")

    dirpath = dirpath.absolute()
    if dirpath.exists() and dirpath.is_file():
        raise FileExistsError("Provided dirpath should be a directory. It is a file.")

    dirpath.mkdir(exist_ok=True, parents=True)

    for pair in component_pairs_cartesian:
        logger.info(f"Fetching data for pair {pair}")
        filepath = dirpath.joinpath(f"raw_ccfs_{pair}.npz")

        try:
            final_path = fetch_crosscorrelations_cartesian_single_pair_and_save(
                crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
                starttime=starttime,
                endtime=endtime,
                filepath=filepath,
                component_pair_cartesian=pair,
                overwrite=overwrite,
            )
        except FileExistsError:
            logger.error(
                f"File {filepath} with data for pair {pair} exists. "
                f"It will not be overwritten. "
                f"If you want to overwrite it, pass overwrite=True."
                f"Skipping to the next pair. "
            )
            continue
        if final_path is None:
            continue

        logger.info(f"File {final_path} with data for pair {pair} was successfully written.")
    return


def fetch_crosscorrelations_cartesian_single_pair_and_save(
    crosscorrelation_cartesian_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    filepath: Path,
    component_pair_cartesian: Optional[ComponentPairCartesian] = None,
    component_pair_cartesian_id: Optional[int] = None,
    overwrite: bool = False,
) -> Optional[Path]:
    if component_pair_cartesian_id is not None and component_pair_cartesian is not None:
        raise ValueError("You cannot provide both component_pair_cartesian and component_pair_cartesian_id")
    if component_pair_cartesian_id is None and component_pair_cartesian is None:
        raise ValueError("You have to provide one of component_pair_cartesian or component_pair_cartesian_id")
    if component_pair_cartesian_id is not None:
        pairs = fetch_componentpairs_cartesian_by_id(component_pair_cartesian_id=component_pair_cartesian_id)
        if len(pairs) != 1:
            raise ValueError(f"Expected only one component pair. Got {len(pairs)}.")
        else:
            pair = pairs[0]
    if component_pair_cartesian is not None:
        pair = component_pair_cartesian

    params = fetch_crosscorrelation_cartesian_params_by_id(id=crosscorrelation_cartesian_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    # noinspection PyUnboundLocalVariable
    fetched_ccfs = fetch_crosscorrelation_cartesian(
        componentpair_id=(pair.id,), load_timespan=True, timespan_id=extract_object_ids(timespans)
    )

    if len(fetched_ccfs) == 0:
        logger.info(f"There are no crosscorrelations_cartesian for pair {component_pair_cartesian}")
        return None

    df = assembly_ccf_cartesian_dataframe(fetched_ccfs, params)

    metadata = prepare_metadata_for_saving_raw_ccf_cartesian_file(pair, starttime, endtime, params)

    write_ccfs_to_npz(
        df=df,
        filepath=filepath,
        overwrite=overwrite,
        metadata_keys=metadata.keys(),
        metadata_values=metadata.values(),
    )
    return filepath


def prepare_metadata_for_saving_raw_ccf_cartesian_file(pair: ComponentPairCartesian, starttime, endtime, config):
    from noiz import __version__

    processing_parameters_dict = get_parent_configs_as_dict(config=config)

    metadata = {
        "noiz_version": __version__,
        "type": "raw_crosscorrelations_cartesian_without_qc2_selection",
        "starttime": str(starttime),
        "endtime": str(endtime),
        "component_pair_cartesian": str(pair),
    }
    metadata.update(processing_parameters_dict)
    return metadata


def get_parent_configs_as_dict(config) -> Dict[str, Any]:
    return {}


def _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan):
    """
    Creating a file containing the cylindrical crosscorrelation previously computed

    :param component_pair_cylindrical: component_pair_cylindrical
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param xcorr_cylindrical: cylindrical crosscorrelation previously computed
    :type xcorr_cylindrical: array
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return: file
    :rtype:
    """

    import numpy as np
    from noiz.globals import PROCESSED_DATA_DIR

    filepath = assembly_filepath(
        PROCESSED_DATA_DIR,  # type: ignore
        "ccf_cylindrical",
        assembly_ccf_cylindrical_dir(
            component_pair_cylindrical=component_pair_cylindrical, timespan=timespan
        ).joinpath(
            assembly_ccf_cylindrical_filename(
                component_pair_cylindrical=component_pair_cylindrical, timespan=timespan, count=0
            )
        ),
    )

    if filepath.exists():
        logger.debug(f"Filepath {filepath} exists. Trying to find next free one.")
        filepath = increment_filename_counter(filepath=filepath, extension=True)
        logger.debug(f"Free filepath found. CCF will be saved to {filepath}")

    logger.info(f"CCF will be written to {str(filepath)}")
    parent_directory_exists_or_create(filepath)

    ccf_file = CrosscorrelationCylindricalFile(filepath=str(filepath))

    np.save(file=ccf_file.filepath, arr=xcorr_cylindrical)

    return ccf_file


def assembly_ccf_cylindrical_dir(
    component_pair_cylindrical: ComponentPairCylindrical,
    timespan: Timespan,
) -> Path:
    """
    Assembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component_pair_cylindrical: Component object containing information about used channel
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return: Path object containing SDS-like directory hierarchy.
    :rtype: Path
    """

    if (component_pair_cylindrical.component_aE is None) & (component_pair_cylindrical.component_aN is None):
        a_network = component_pair_cylindrical.component_aZ.network
        a_station = component_pair_cylindrical.component_aZ.station
    else:
        a_network = component_pair_cylindrical.component_aE.network
        a_station = component_pair_cylindrical.component_aE.station

    if (component_pair_cylindrical.component_bE is None) & (component_pair_cylindrical.component_bN is None):
        b_network = component_pair_cylindrical.component_bZ.network
        b_station = component_pair_cylindrical.component_bZ.station
    else:
        b_network = component_pair_cylindrical.component_bE.network
        b_station = component_pair_cylindrical.component_bE.station

    return (
        Path(str(timespan.starttime.year))
        .joinpath(str(timespan.starttime.month))
        .joinpath(component_pair_cylindrical.component_cylindrical_code_pair)
        .joinpath(f"{a_network}.{a_station}-{b_network}.{b_station}")
    )


def assembly_ccf_cylindrical_filename(
    component_pair_cylindrical: ComponentPairCylindrical, timespan: Timespan, count: int = 0
) -> str:
    """
    Creating the filename for saving cylindrical crosscorrelation file

    :param component_pair_cylindrical: Component object containing information about used channel
    :type component_pair_cylindrical: ComponentPairCylindrical
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :param count: counter for increasing if filename exits, defaults to 0
    :type count: int, optional
    :return: filename to save cylindrical crosscorrelation
    :rtype: str
    """

    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    if (component_pair_cylindrical.component_aE is None) & (component_pair_cylindrical.component_aN is None):
        a_network = component_pair_cylindrical.component_aZ.network
        a_station = component_pair_cylindrical.component_aZ.station
    else:
        a_network = component_pair_cylindrical.component_aE.network
        a_station = component_pair_cylindrical.component_aE.station

    if (component_pair_cylindrical.component_bE is None) & (component_pair_cylindrical.component_bN is None):
        b_network = component_pair_cylindrical.component_bZ.network
        b_station = component_pair_cylindrical.component_bZ.station
    else:
        b_network = component_pair_cylindrical.component_bE.network
        b_station = component_pair_cylindrical.component_bE.station

    filename = ".".join(
        [
            a_network,
            a_station,
            component_pair_cylindrical.component_cylindrical_code_pair[0],
            b_network,
            b_station,
            component_pair_cylindrical.component_cylindrical_code_pair[1],
            year,
            doy_time,
            str(count),
            "npy",
        ]
    )

    return filename


def _crosscorrelate_cylindrical_for_timespan_wrapper(
    inputs: CrosscorrelationCylindricalRunnerInputs,
) -> Tuple[CrosscorrelationCylindrical, ...]:
    results = _crosscorrelate_cylindrical_for_timespan(
        timespan=inputs["timespan"],
        crosscorrelation_cylindrical_params=inputs["crosscorrelation_cylindrical_params"],
        grouped_processed_xcorrcartisian=inputs["grouped_processed_xcorrcartisian"],
        component_pairs_cylindrical=inputs["component_pairs_cylindrical"],
    )
    # Filter out None results (when cartesian CCFs don't exist for a pair)
    return tuple(r for r in results if r is not None)


def _crosscorrelate_cylindrical_for_timespan(
    timespan: Timespan,
    crosscorrelation_cylindrical_params: CrosscorrelationCylindricalParams,
    grouped_processed_xcorrcartisian,
    component_pairs_cylindrical: Tuple[ComponentPairCylindrical, ...],
) -> List[CrosscorrelationCylindrical]:
    from noiz.globals import PROCESSED_DATA_DIR
    from noiz.processing.path_helpers import (
        assembly_filepath,
        increment_filename_counter,
        parent_directory_exists_or_create,
    )

    import numpy as np

    logger.info(f"Running crosscorrelation_cylindrical for {timespan}")

    logger.debug(f"Loading data for timespan {timespan}")
    xcorrs_cylindrical = []

    for cp in component_pairs_cylindrical:
        xcorr_cylindrical_all = cylindrical_correlation_computation(
            cp, grouped_processed_xcorrcartisian, timespan, crosscorrelation_cylindrical_params
        )
        xcorrs_cylindrical.append(xcorr_cylindrical_all)

    return xcorrs_cylindrical


def _prepare_upsert_command_crosscorrelation_cylindrical(xcorr: CrosscorrelationCylindrical) -> Insert:
    # First, ensure the file record exists in the database
    file_id = None
    if xcorr.file is not None:
        filepath = xcorr.file.filepath
        logger.debug(f"DEBUG_UPSERT_CYL: Checking if file exists in DB: {filepath[:80]}...")
        # Check if file already exists in DB by filepath
        existing_file = (
            db.session.query(CrosscorrelationCylindricalFile)
            .filter(CrosscorrelationCylindricalFile.filepath == filepath)
            .first()
        )

        if existing_file is not None:
            file_id = existing_file.id
            logger.debug(f"DEBUG_UPSERT_CYL: Found existing file with id={file_id}")
        else:
            # Insert the file record first
            logger.debug("DEBUG_UPSERT_CYL: File not found, inserting new file record...")
            new_file = CrosscorrelationCylindricalFile(filepath=filepath)
            db.session.add(new_file)
            db.session.flush()  # Get the ID without committing
            file_id = new_file.id
            logger.debug(f"DEBUG_UPSERT_CYL: Inserted new file with id={file_id}")
    elif xcorr.crosscorrelation_cylindrical_file_id is not None:
        file_id = xcorr.crosscorrelation_cylindrical_file_id
        logger.debug(f"DEBUG_UPSERT_CYL: Using existing file_id from xcorr: {file_id}")

    insert_command = (
        insert(CrosscorrelationCylindrical)
        .values(
            crosscorrelation_cylindrical_params_id=xcorr.crosscorrelation_cylindrical_params_id,
            componentpair_cylindrical_id=xcorr.componentpair_cylindrical_id,
            timespan_id=xcorr.timespan_id,
            crosscorrelation_cartesian_1_id=xcorr.crosscorrelation_cartesian_1_id,
            crosscorrelation_cartesian_1_code_pair=xcorr.crosscorrelation_cartesian_1_code_pair,
            crosscorrelation_cartesian_2_id=xcorr.crosscorrelation_cartesian_2_id,
            crosscorrelation_cartesian_2_code_pair=xcorr.crosscorrelation_cartesian_2_code_pair,
            crosscorrelation_cartesian_3_id=xcorr.crosscorrelation_cartesian_3_id,
            crosscorrelation_cartesian_3_code_pair=xcorr.crosscorrelation_cartesian_3_code_pair,
            crosscorrelation_cartesian_4_id=xcorr.crosscorrelation_cartesian_4_id,
            crosscorrelation_cartesian_4_code_pair=xcorr.crosscorrelation_cartesian_4_code_pair,
            crosscorrelation_cylindrical_file_id=file_id,
        )
        .on_conflict_do_update(
            constraint="unique_ccfcylindrical_per_timespan_cylindrical_per_config",
            set_={"crosscorrelation_cylindrical_file_id": file_id},
        )
    )
    return insert_command


def _fetch_cp_cartesian_associated_to_cp_cylindrical(
    component_pairs_cylindrical: List[ComponentPairCylindrical],
) -> List[ComponentPairCartesian]:
    """Fetch the cartesian componentpairs associated to the cylindrical componentpairs to process

    :param component_pairs_cylindrical: List of component_pairs_cylindrical to process
    :type component_pairs_cylindrical: ComponentPairCylindrical
    :return: List of component_pairs_cartesian
    :rtype: List[ComponentPairCartesian]
    """

    component_a_id = tuple(
        [
            cp.component_aE_id if cp.component_aE_id is not None else cp.component_aZ_id
            for cp in component_pairs_cylindrical
        ]
    )
    component_b_id = tuple(
        [
            cp.component_bE_id if cp.component_bE_id is not None else cp.component_bZ_id
            for cp in component_pairs_cylindrical
        ]
    )

    stationa = tuple([cp.station for cp in fetch_components_by_id(component_a_id)])
    stationb = tuple([cp.station for cp in fetch_components_by_id(component_b_id)])

    component_pairs_cartesian = fetch_componentpairs_cartesian(station_codes_a=stationa, station_codes_b=stationb)
    return list(component_pairs_cartesian)


def cylindrical_correlation_computation(
    component_pair_cylindrical: ComponentPairCylindrical,
    grouped_processed_xcorrcartisian: Dict[FrozenSet[int], CrosscorrelationCartesian],
    timespan: Timespan,
    params: CrosscorrelationCylindricalParams,
):
    """_summary_

    :param component_pair_cylindrical: _description_
    :type component_pair_cylindrical: _type_
    :param grouped_processed_xcorrcartisian: _description_
    :type grouped_processed_xcorrcartisian: _type_
    :param timespan: _description_
    :type timespan: _type_
    :param params: _description_
    :type params: _type_
    :return: _description_
    :rtype: _type_
    """

    code = component_pair_cylindrical.component_cylindrical_code_pair
    back_az = component_pair_cylindrical.backazimuth
    try:
        if (code == "RR") | (code == "TT") | (code == "RT") | (code == "TR"):
            xcorr_aN_bN, xcorr_aE_bE, xcorr_aN_bE, xcorr_aE_bN = _fetch_R_T_xcoor(
                grouped_processed_xcorrcartisian, component_pair_cylindrical
            )
            xcorr_cylindrical = _computation_cylindrical_correlation_R_T(
                code, xcorr_aN_bN, xcorr_aE_bE, xcorr_aN_bE, xcorr_aE_bN, back_az
            )
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            logger.info(f"ccf_file is {str(ccf_file)}")
            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aN_bN.id,
                crosscorrelation_cartesian_1_code_pair="NN",
                crosscorrelation_cartesian_2_id=xcorr_aE_bE.id,
                crosscorrelation_cartesian_2_code_pair="EE",
                crosscorrelation_cartesian_3_id=xcorr_aN_bE.id,
                crosscorrelation_cartesian_3_code_pair="NE",
                crosscorrelation_cartesian_4_id=xcorr_aE_bN.id,
                crosscorrelation_cartesian_4_code_pair="EN",
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )

        elif (code == "RZ") | (code == "TZ"):
            xcorr_aE_bZ, xcorr_aN_bZ = _fetch_RT_Z_xcoor(grouped_processed_xcorrcartisian, component_pair_cylindrical)
            xcorr_cylindrical = _computation_cylindrical_correlation_RT_Z(code, xcorr_aE_bZ, xcorr_aN_bZ, back_az)
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aE_bZ.id,
                crosscorrelation_cartesian_1_code_pair="EZ",
                crosscorrelation_cartesian_2_id=xcorr_aN_bZ.id,
                crosscorrelation_cartesian_2_code_pair="NZ",
                crosscorrelation_cartesian_3_id=None,  # type: ignore
                crosscorrelation_cartesian_3_code_pair=None,  # type: ignore
                crosscorrelation_cartesian_4_id=None,  # type: ignore
                crosscorrelation_cartesian_4_code_pair=None,  # type: ignore
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )

        elif (code == "ZR") | (code == "ZT"):
            xcorr_aZ_bE, xcorr_aZ_bN = _fetch_Z_TR_xcoor(grouped_processed_xcorrcartisian, component_pair_cylindrical)
            xcorr_cylindrical = _computation_cylindrical_correlation_Z_TR(code, xcorr_aZ_bE, xcorr_aZ_bN, back_az)
            ccf_file = _create_cylindrical_correlation_file(component_pair_cylindrical, xcorr_cylindrical, timespan)

            xcorr = CrosscorrelationCylindrical(
                componentpair_cylindrical_id=component_pair_cylindrical.id,
                timespan_id=timespan.id,
                crosscorrelation_cartesian_1_id=xcorr_aZ_bE.id,
                crosscorrelation_cartesian_1_code_pair="ZE",
                crosscorrelation_cartesian_2_id=xcorr_aZ_bN.id,
                crosscorrelation_cartesian_2_code_pair="ZN",
                crosscorrelation_cartesian_3_id=None,  # type: ignore
                crosscorrelation_cartesian_3_code_pair=None,  # type: ignore
                crosscorrelation_cartesian_4_id=None,  # type: ignore
                crosscorrelation_cartesian_4_code_pair=None,  # type: ignore
                crosscorrelation_cylindrical_params_id=params.id,
                crosscorrelation_cylindrical_file_id=ccf_file.id,
                file=ccf_file,
            )
        return xcorr
    except Exception:
        logger.error(f"No cartesian correlation for pair {component_pair_cylindrical} ")
        return None


def _prepare_inputs_for_crosscorrelations_cylindrical(
    crosscorrelation_cylindrical_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    batch_size: int = 100,
) -> Generator[CrosscorrelationCylindricalRunnerInputs, None, None]:
    """Performs all the database queries to prepare all the data required for running crosscorrelations_cylindrical.
    Returns a tuple of inputs specific for the further calculations.

    :param crosscorrelation_cylindrical_params_id: ID of CrosscorrelationCylindricalParams object to use
    :type crosscorrelation_cylindrical_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param accepted_component_code_pairs_cylindrical: Code pair accepted for cylindrical componentpair
    :type accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]], optional
    :param station_codes_a: Selector for station code of A station in the pair
    :type station_codes_a: Optional[Union[Collection[str], str]], optional
    :param station_codes_b:Selector for station code of B station in the pair
    :type station_codes_b: Optional[Union[Collection[str], str]], optional
    :param batch_size: Batch size for which inputs are fetched.
    It changes count of timespans that are pulled from db and prepared for processing at the same time.
    If you have a lot of stations, you might want to reduce that value.
    :type batch_size: int
    :return:
    :rtype: Generator[CrosscorrelationCylindricalRunnerInputs, None, None]
    """

    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.info(f"There are {len(fetched_timespans)} timespan to process")
    if accepted_component_code_pairs_cylindrical is not None:
        accepted_component_code_pairs_cylindrical = validate_component_code_pairs(
            component_pairs_cartesian=validate_to_tuple(accepted_component_code_pairs_cylindrical, str)
        )
    component_pairs_cylindrical = fetch_componentpairs_cylindrical(
        station_codes_a=station_codes_a,
        station_codes_b=station_codes_b,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        starttime=starttime,
        endtime=endtime,
    )
    component_pairs_cartesian_ids = extract_object_ids(
        _fetch_cp_cartesian_associated_to_cp_cylindrical(component_pairs_cylindrical)
    )
    params = fetch_crosscorrelation_cylindrical_params_by_id(id=crosscorrelation_cylindrical_params_id)

    for timespan_batch in more_itertools.chunked(fetched_timespans, batch_size):
        batch_t_tid = extract_object_ids_keep_objects(timespan_batch)
        logger.info(f"batch_t_id length  is {len(batch_t_tid)}")
        fetched_xcorrelation_cartesian = (
            db.session.query(CrosscorrelationCartesian.timespan_id, CrosscorrelationCartesian)
            .filter(
                CrosscorrelationCartesian.timespan_id.in_(batch_t_tid.keys()),  # type: ignore
                CrosscorrelationCartesian.crosscorrelation_cartesian_params_id
                == params.crosscorrelation_cartesian_params_id,
                CrosscorrelationCartesian.componentpair_id.in_(component_pairs_cartesian_ids),
            )
            .order_by(CrosscorrelationCartesian.timespan_id)
            .all()
        )

        grouped_xcorr = group_xcrorrcartesian_by_timespanid_componentids(
            processed_xcorrcartesian=fetched_xcorrelation_cartesian
        )

        comp_cart_id = list(
            {
                xc.CrosscorrelationCartesian.componentpair_cartesian.component_a_id
                for xc in fetched_xcorrelation_cartesian
            }.union(
                {
                    xc.CrosscorrelationCartesian.componentpair_cartesian.component_b_id
                    for xc in fetched_xcorrelation_cartesian
                }
            )
        )

        component_pairs_cylindrical_select = []
        for xc in component_pairs_cylindrical:
            if any(comp.id in comp_cart_id for comp in xc.get_all_components() if comp is not None):
                component_pairs_cylindrical_select.append(xc)

        for timespan_id, grouped_processed_xcorr in grouped_xcorr.items():
            db.session.expunge_all()
            yield CrosscorrelationCylindricalRunnerInputs(
                timespan=batch_t_tid[timespan_id],
                crosscorrelation_cylindrical_params=params,
                grouped_processed_xcorrcartisian=grouped_processed_xcorr,
                component_pairs_cylindrical=tuple(component_pairs_cylindrical_select),
            )
    return


def perform_crosscorrelations_cylindrical(
    crosscorrelation_cylindrical_params_id: int,
    starttime: Union[datetime.date, datetime.datetime],
    endtime: Union[datetime.date, datetime.datetime],
    accepted_component_code_pairs_cylindrical: Optional[Union[Collection[str], str]] = None,
    station_codes_a: Optional[Union[Collection[str], str]] = None,
    station_codes_b: Optional[Union[Collection[str], str]] = None,
    raise_errors: bool = False,
    batch_size: int = 100,
    parallel: bool = True,
    overwrite: bool = True,
) -> None:
    calculation_inputs = _prepare_inputs_for_crosscorrelations_cylindrical(
        crosscorrelation_cylindrical_params_id=crosscorrelation_cylindrical_params_id,
        starttime=starttime,
        endtime=endtime,
        accepted_component_code_pairs_cylindrical=accepted_component_code_pairs_cylindrical,
        station_codes_a=station_codes_a,
        station_codes_b=station_codes_b,
        batch_size=batch_size,
    )
    logger.info("calculation_inputs ok")
    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_cylindrical_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation_cylindrical,
            raise_errors=raise_errors,
            overwrite=overwrite,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=_crosscorrelate_cylindrical_for_timespan_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_crosscorrelation_cylindrical,
            raise_errors=raise_errors,
            with_file=True,
            overwrite=overwrite,
        )
    return
