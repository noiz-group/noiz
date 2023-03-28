from sqlalchemy.sql import Insert
from typing import Union, TypedDict, Collection, Callable, Optional, List, Tuple, Dict

from noiz.models import CrosscorrelationOld, CCFStack, DatachunkStats, ProcessedDatachunk, QCOneResults, QCTwoResults, \
    Datachunk, DatachunkFile, QCOneConfig, AveragedSohGps, ComponentPairCartesian, StackingSchema, StackingTimespan, Component, \
    Timespan, Tsindex, DatachunkParams, ProcessedDatachunkParams, CrosscorrelationParams, ProcessedDatachunkFile, \
    BeamformingFile, BeamformingResult, PPSDParams, Crosscorrelation, CrosscorrelationFile, PPSDFile, PPSDResult, \
    BeamformingParams, EventDetectionParams, EventDetectionResult, EventDetectionFile, EventConfirmationParams, EventConfirmationResult, \
    EventConfirmationFile, EventConfirmationRun
from noiz.models.beamforming import BeamformingPeakAverageAbspower, BeamformingPeakAverageRelpower, \
    BeamformingPeakAllAbspower, BeamformingPeakAllRelpower

BulkAddableObjects = Union[
    Datachunk,
    CrosscorrelationOld,
    Crosscorrelation,
    BeamformingResult,
    PPSDResult,
    CCFStack,
    DatachunkStats,
    ProcessedDatachunk,
    QCOneResults,
    QCTwoResults,
    DatachunkFile,
    CrosscorrelationFile,
    ProcessedDatachunkFile,
    EventDetectionResult,
    EventConfirmationResult,
    EventConfirmationRun
    ]

BulkAddableFileObjects = Union[
    DatachunkFile,
    CrosscorrelationFile,
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


class BeamformingRunnerInputs(TypedDict):
    beamforming_params: Collection[BeamformingParams]
    timespan: Timespan
    datachunks: Tuple[Datachunk, ...]


class PPSDRunnerInputs(TypedDict):
    ppsd_params: PPSDParams
    timespan: Timespan
    datachunk: Datachunk
    component: Component


class CrosscorrelationRunnerInputs(TypedDict):
    timespan: Timespan
    crosscorrelation_params: CrosscorrelationParams
    grouped_processed_chunks: Dict[int, ProcessedDatachunk]
    component_pairs: Tuple[ComponentPairCartesian, ...]


class StackingInputs(TypedDict):
    qctwo_ccfs_container: List[Tuple[QCTwoResults, Crosscorrelation]]
    componentpair_cartesian: ComponentPairCartesian
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
    ProcessDatachunksInputs,
    CrosscorrelationRunnerInputs,
    StackingInputs,
    EventDetectionRunnerInputs,
    EventConfirmationRunnerInputs,
]


class BulkAddOrUpsertObjectsInputs(TypedDict):
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]]
    upserter_callable: Callable[[BulkAddableObjects], Insert]
    bulk_insert: bool
