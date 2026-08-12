# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from sqlalchemy.sql import Insert
from typing import Union, TypedDict, Collection, Callable, Optional, List, Tuple, Dict, FrozenSet

from noiz.models import (
    CCFStack,
    CCFStackCylindrical,
    DatachunkStats,
    ProcessedDatachunk,
    QCOneResults,
    QCTwoResults,
    Datachunk,
    DatachunkFile,
    QCOneConfig,
    QCTwoConfig,
    AveragedSohGps,
    ComponentPairCartesian,
    StackingSchema,
    StackingTimespan,
    Component,
    Timespan,
    Tsindex,
    DatachunkParams,
    ProcessedDatachunkParams,
    CrosscorrelationCartesianParams,
    ProcessedDatachunkFile,
    BeamformingFile,
    BeamformingResult,
    PPSDParams,
    CrosscorrelationCartesian,
    CrosscorrelationCartesianFile,
    PPSDFile,
    PPSDResult,
    BeamformingParams,
    EventDetectionParams,
    EventDetectionResult,
    EventDetectionFile,
    EventConfirmationParams,
    EventConfirmationResult,
    EventConfirmationFile,
    EventConfirmationRun,
    ComponentPairCylindrical,
    CrosscorrelationCylindrical,
    CrosscorrelationCylindricalFile,
    CrosscorrelationCylindricalParams,
    CrosscorrelationCylindricalParamsHolder,
)
from noiz.models.beamforming import (
    BeamformingPeakAverageAbspower,
    BeamformingPeakAverageRelpower,
    BeamformingPeakAllAbspower,
    BeamformingPeakAllRelpower,
)

BulkAddableObjects = Union[
    Datachunk,
    CrosscorrelationCartesian,
    CrosscorrelationCylindrical,
    BeamformingResult,
    PPSDResult,
    CCFStack,
    CCFStackCylindrical,
    DatachunkStats,
    ProcessedDatachunk,
    QCOneResults,
    QCTwoResults,
    DatachunkFile,
    CrosscorrelationCartesianFile,
    CrosscorrelationCylindricalFile,
    ProcessedDatachunkFile,
    EventDetectionResult,
    EventConfirmationResult,
    EventConfirmationRun,
]

BulkAddableFileObjects = Union[
    DatachunkFile,
    CrosscorrelationCartesianFile,
    CrosscorrelationCylindricalFile,
    ProcessedDatachunkFile,
    BeamformingFile,
    PPSDFile,
    BeamformingPeakAverageAbspower,
    BeamformingPeakAverageRelpower,
    BeamformingPeakAllAbspower,
    BeamformingPeakAllRelpower,
    EventDetectionFile,
    EventConfirmationFile,
]


class CalculateDatachunkStatsInputs(TypedDict):
    """
    TypedDict class that describes inputs required for :py:func:`noiz.processing.datachunk.calculate_datachunk_stats`
    """

    datachunk: Datachunk
    datachunk_file: Optional[DatachunkFile]


class ProcessDatachunksInputs(TypedDict):
    """
    TypedDict class that describes inputs required for :py:func:`noiz.processing.datachunk.calculate_datachunk_stats`
    """

    datachunk: Datachunk
    datachunk_file: Optional[DatachunkFile]
    params: ProcessedDatachunkParams


class RunDatachunkPreparationInputs(TypedDict):
    component: Component
    timespans: Collection[Timespan]
    time_series: Tsindex
    processing_params: DatachunkParams


class QCOneRunnerInputs(TypedDict):
    datachunk: Datachunk
    qcone_config: QCOneConfig
    stats: Optional[DatachunkStats]
    avg_soh_gps: Optional[AveragedSohGps]


class QCTwoRunnerInputs(TypedDict):
    crosscorrelation_cartesian: CrosscorrelationCartesian
    qctwo_config: QCTwoConfig


class QCTwoParallelInputs(TypedDict):
    qctwo_config_id: int
    crosscorrelation_cartesian_ids: Tuple[int, ...]


class BeamformingRunnerInputs(TypedDict):
    beamforming_params: Collection[BeamformingParams]
    timespan: Timespan
    datachunks: Tuple[Datachunk, ...]
    raise_errors: bool


class PPSDRunnerInputs(TypedDict):
    ppsd_params: PPSDParams
    timespan: Timespan
    datachunk: Datachunk
    component: Component


class CrosscorrelationCartesianRunnerInputs(TypedDict):
    timespan: Timespan
    crosscorrelation_cartesian_params: CrosscorrelationCartesianParams
    grouped_processed_chunks: Dict[int, ProcessedDatachunk]
    component_pairs_cartesian: Tuple[ComponentPairCartesian, ...]
    verbose_ccf_logging: bool
    batch_index: int
    total_batches: int


class CrosscorrelationCylindricalRunnerInputs(TypedDict):
    timespan: Timespan
    crosscorrelation_cylindrical_params: CrosscorrelationCylindricalParams
    grouped_processed_xcorrcartisian: Dict[FrozenSet[int], CrosscorrelationCartesian]
    component_pairs_cylindrical: Tuple[ComponentPairCylindrical, ...]


class StackingInputs(TypedDict):
    qctwo_ccfs_container: List[Tuple[QCTwoResults, CrosscorrelationCartesian]]
    componentpair_cartesian: ComponentPairCartesian
    stacking_schema: StackingSchema
    stacking_timespan: StackingTimespan


class StackingParallelInputs(TypedDict):
    qctwo_config_id: int
    componentpair_cartesian_id: int
    stacking_schema_id: int
    stacking_timespan_id: int


class StackingCylindricalInputs(TypedDict):
    """TypedDict for cylindrical CCF stacking inputs."""

    ccfs_cylindrical: List[CrosscorrelationCylindrical]
    componentpair_cylindrical: ComponentPairCylindrical
    stacking_schema: StackingSchema
    stacking_timespan: StackingTimespan


class EventDetectionRunnerInputs(TypedDict):
    event_detection_params: EventDetectionParams
    timespan: Timespan
    datachunk: Datachunk
    component: Component
    event_detection_run_id: int
    plot_figures: bool


class EventConfirmationRunnerInputs(TypedDict):
    event_confirmation_params: EventConfirmationParams
    timespan: Timespan
    event_detection_results: Collection[EventDetectionResult]
    event_confirmation_run: EventConfirmationRun


InputsForMassCalculations = Union[
    CalculateDatachunkStatsInputs,
    RunDatachunkPreparationInputs,
    BeamformingRunnerInputs,
    PPSDRunnerInputs,
    QCOneRunnerInputs,
    QCTwoRunnerInputs,
    QCTwoParallelInputs,
    ProcessDatachunksInputs,
    CrosscorrelationCartesianRunnerInputs,
    CrosscorrelationCylindricalRunnerInputs,
    StackingInputs,
    StackingParallelInputs,
    StackingCylindricalInputs,
    EventDetectionRunnerInputs,
    EventConfirmationRunnerInputs,
]


class BulkAddOrUpsertObjectsInputs(TypedDict):
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]]
    upserter_callable: Callable[[BulkAddableObjects], Insert]
    bulk_insert: bool
