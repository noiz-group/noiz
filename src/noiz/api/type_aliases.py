from sqlalchemy.sql import Insert
from typing import Union, TypedDict, Collection, Callable, Optional, List, Tuple

from noiz.models import Crosscorrelation, CCFStack, DatachunkStats, ProcessedDatachunk, QCOneResults, QCTwoResults, \
    Datachunk, DatachunkFile, QCOneConfig, AveragedSohGps, ComponentPair, StackingSchema, StackingTimespan, Component, \
    Timespan, Tsindex, DatachunkParams

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


InputsForMassCalculations = Union[
    CalculateDatachunkStatsInputs,
    RunDatachunkPreparationInputs,
    QCOneRunnerInputs,
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
