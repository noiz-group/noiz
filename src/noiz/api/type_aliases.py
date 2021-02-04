from sqlalchemy.sql import Insert
from typing import Union, TypedDict, Collection, Callable, Optional, List, Tuple, DefaultDict, Dict

from noiz.models import Crosscorrelation, CCFStack, DatachunkStats, ProcessedDatachunk, QCOneResults, QCTwoResults, \
    Datachunk, DatachunkFile, QCOneConfig, AveragedSohGps, ComponentPair, StackingSchema, StackingTimespan, Component, \
    Timespan, Tsindex, DatachunkParams, ProcessedDatachunkParams, CrosscorrelationParams

BulkAddableObjects = Union[
        Datachunk,
        Crosscorrelation,
        CCFStack,
        DatachunkStats,
        ProcessedDatachunk,
        QCOneResults,
        QCTwoResults,
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


class CrosscorrelationRunnerInputs(TypedDict):
    timespan_id: int
    crosscorrelation_params: CrosscorrelationParams
    groupped_processed_chunks: DefaultDict[int, Dict[int, ProcessedDatachunk]]
    component_pairs: ComponentPair


InputsForMassCalculations = Union[
    CalculateDatachunkStatsInputs,
    RunDatachunkPreparationInputs,
    QCOneRunnerInputs,
    ProcessDatachunksInputs,
]


class BulkAddOrUpsertObjectsInputs(TypedDict):
    objects_to_add: Union[BulkAddableObjects, Collection[BulkAddableObjects]]
    upserter_callable: Callable[[BulkAddableObjects], Insert]
    bulk_insert: bool


class StackingInputs(TypedDict):
    qctwo_ccfs_container: List[Tuple[QCTwoResults, Crosscorrelation]]
    componentpair: ComponentPair
    stacking_schema: StackingSchema
    stacking_timespan: StackingTimespan
