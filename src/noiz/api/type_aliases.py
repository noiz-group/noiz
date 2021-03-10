from sqlalchemy.sql import Insert
from typing import Union, TypedDict, Collection, Callable, Optional, List, Tuple, Dict

from noiz.models import CrosscorrelationOld, CCFStack, DatachunkStats, ProcessedDatachunk, QCOneResults, QCTwoResults, \
    Datachunk, DatachunkFile, QCOneConfig, AveragedSohGps, ComponentPair, StackingSchema, StackingTimespan, Component, \
    Timespan, Tsindex, DatachunkParams, ProcessedDatachunkParams, CrosscorrelationParams, ProcessedDatachunkFile
from noiz.models.beamforming import BeamformingResult
from noiz.models.crosscorrelation import Crosscorrelation, CrosscorrelationFile
from noiz.models.processing_params import BeamformingParams

BulkAddableObjects = Union[
    Datachunk,
    CrosscorrelationOld,
    Crosscorrelation,
    BeamformingResult,
    CCFStack,
    DatachunkStats,
    ProcessedDatachunk,
    QCOneResults,
    QCTwoResults,
    DatachunkFile,
    CrosscorrelationFile,
    ProcessedDatachunkFile,
    ]

BulkAddableFileObjects = Union[
    DatachunkFile,
    CrosscorrelationFile,
    ProcessedDatachunkFile,
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
    beamforming_params: BeamformingParams
    timespan: Timespan
    datachunks: Tuple[Datachunk, ...]


class CrosscorrelationRunnerInputs(TypedDict):
    timespan: Timespan
    crosscorrelation_params: CrosscorrelationParams
    grouped_processed_chunks: Dict[int, ProcessedDatachunk]
    component_pairs: Tuple[ComponentPair, ...]


class StackingInputs(TypedDict):
    qctwo_ccfs_container: List[Tuple[QCTwoResults, Crosscorrelation]]
    componentpair: ComponentPair
    stacking_schema: StackingSchema
    stacking_timespan: StackingTimespan


InputsForMassCalculations = Union[
    CalculateDatachunkStatsInputs,
    RunDatachunkPreparationInputs,
    BeamformingRunnerInputs,
    QCOneRunnerInputs,
    ProcessDatachunksInputs,
    CrosscorrelationRunnerInputs,
    StackingInputs,
]


class BulkAddOrUpsertObjectsInputs(TypedDict):
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]]
    upserter_callable: Callable[[BulkAddableObjects], Insert]
    bulk_insert: bool
