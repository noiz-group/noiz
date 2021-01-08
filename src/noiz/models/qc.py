from functools import cached_property
from typing import List, Union, Optional, Tuple, Collection

import datetime
from pydantic.dataclasses import dataclass

from noiz.database import db
from noiz.globals import ExtendedEnum


class NullTreatmentPolicy(ExtendedEnum):
    FAIL = "fail"
    PASS = "pass"


class QCOneRejectedTime(db.Model):
    __tablename__ = "qcone_rejected_time_periods"
    id = db.Column("id", db.Integer, primary_key=True)

    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"))
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    qcone_config = db.relationship(
        "QCOneConfig",
        uselist=False,
        back_populates="time_periods_rejected",
        foreign_keys=[qcone_config_id]
    )
    component = db.relationship("Component", foreign_keys=[component_id])


class QCOneConfig(db.Model):
    __tablename__ = "qcone_config"

    id = db.Column("id", db.Integer, primary_key=True)

    null_policy = db.Column("null_policy", db.UnicodeText, default=NullTreatmentPolicy.PASS.value, nullable=False)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Float, nullable=True)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Float, nullable=True)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Float, nullable=True)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Float, nullable=True)
    signal_energy_min = db.Column("signal_energy_min", db.Float, nullable=True)
    signal_energy_max = db.Column("signal_energy_max", db.Float, nullable=True)
    signal_min_value_min = db.Column("signal_min_value_min", db.Float, nullable=True)
    signal_min_value_max = db.Column("signal_min_value_max", db.Float, nullable=True)
    signal_max_value_min = db.Column("signal_max_value_min", db.Float, nullable=True)
    signal_max_value_max = db.Column("signal_max_value_max", db.Float, nullable=True)
    signal_mean_value_min = db.Column("signal_mean_value_min", db.Float, nullable=True)
    signal_mean_value_max = db.Column("signal_mean_value_max", db.Float, nullable=True)
    signal_variance_min = db.Column("signal_variance_min", db.Float, nullable=True)
    signal_variance_max = db.Column("signal_variance_max", db.Float, nullable=True)
    signal_skewness_min = db.Column("signal_skewness_min", db.Float, nullable=True)
    signal_skewness_max = db.Column("signal_skewness_max", db.Float, nullable=True)
    signal_kurtosis_min = db.Column("signal_kurtosis_min", db.Float, nullable=True)
    signal_kurtosis_max = db.Column("signal_kurtosis_max", db.Float, nullable=True)

    time_periods_rejected: List[QCOneRejectedTime] = db.relationship(
        "QCOneRejectedTime",
        uselist=True,
        back_populates="qcone_config",
        lazy="joined"
    )

    processed_datachunk_params = db.relationship(
        "ProcessedDatachunkParams",
        uselist=True,
        back_populates="qcone_config",
    )

    def uses_gps(self) -> bool:
        """
        Checks if any of the GPS checks is defined.

        :return: If any of GPS checks is defines
        :rtype: bool
        """
        res = any([x is not None for x in (
            self.avg_gps_time_error_min,
            self.avg_gps_time_error_max,
            self.avg_gps_time_uncertainty_max,
            self.avg_gps_time_uncertainty_min
        )])
        return res

    # py38 only. If you want to go below, use just standard property
    @cached_property
    def null_value(self) -> bool:
        if self.null_policy is NullTreatmentPolicy.PASS or self.null_policy == NullTreatmentPolicy.PASS.value:
            return True
        elif self.null_policy is NullTreatmentPolicy.FAIL or self.null_policy == NullTreatmentPolicy.FAIL.value:
            return False
        else:
            raise NotImplementedError(f"I did not expect this value of null_policy {self.null_policy}")

    # py38 only. If you want to go below, use just standard property
    @cached_property
    def component_ids_rejected_times(self) -> Tuple[int, ...]:
        return tuple([x.component_id for x in self.time_periods_rejected])


class QCOneResults(db.Model):
    __tablename__ = "qcone_results"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id", "qcone_config_id", name="unique_qcone_results_per_config_per_datachunk"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)

    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"))
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"))

    starttime = db.Column("starttime", db.Boolean, nullable=False)
    endtime = db.Column("endtime", db.Boolean, nullable=False)
    accepted_time = db.Column("accepted_time", db.Boolean, nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Boolean, nullable=False)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Boolean, nullable=False)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Boolean, nullable=False)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Boolean, nullable=False)
    signal_energy_min = db.Column("signal_energy_min", db.Boolean, nullable=False)
    signal_energy_max = db.Column("signal_energy_max", db.Boolean, nullable=False)
    signal_min_value_min = db.Column("signal_min_value_min", db.Boolean, nullable=False)
    signal_min_value_max = db.Column("signal_min_value_max", db.Boolean, nullable=False)
    signal_max_value_min = db.Column("signal_max_value_min", db.Boolean, nullable=False)
    signal_max_value_max = db.Column("signal_max_value_max", db.Boolean, nullable=False)
    signal_mean_value_min = db.Column("signal_mean_value_min", db.Boolean, nullable=False)
    signal_mean_value_max = db.Column("signal_mean_value_max", db.Boolean, nullable=False)
    signal_variance_min = db.Column("signal_variance_min", db.Boolean, nullable=False)
    signal_variance_max = db.Column("signal_variance_max", db.Boolean, nullable=False)
    signal_skewness_min = db.Column("signal_skewness_min", db.Boolean, nullable=False)
    signal_skewness_max = db.Column("signal_skewness_max", db.Boolean, nullable=False)
    signal_kurtosis_min = db.Column("signal_kurtosis_min", db.Boolean, nullable=False)
    signal_kurtosis_max = db.Column("signal_kurtosis_max", db.Boolean, nullable=False)

    qcone_config = db.relationship("QCOneConfig", foreign_keys=[qcone_config_id])
    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id])

    def is_passing(self) -> bool:
        """
        Checks if all values of QCOne are True.

        :return: If QCOne is passing for that chunk
        :rtype: bool
        """
        ret = all((
            self.starttime,
            self.endtime,
            self.accepted_time,
            self.avg_gps_time_error_min,
            self.avg_gps_time_error_max,
            self.avg_gps_time_uncertainty_min,
            self.avg_gps_time_uncertainty_max,
            self.signal_energy_min,
            self.signal_energy_max,
            self.signal_min_value_min,
            self.signal_min_value_max,
            self.signal_max_value_min,
            self.signal_max_value_max,
            self.signal_mean_value_min,
            self.signal_mean_value_max,
            self.signal_variance_min,
            self.signal_variance_max,
            self.signal_skewness_min,
            self.signal_skewness_max,
            self.signal_kurtosis_min,
            self.signal_kurtosis_max,
        ))
        return ret


@dataclass
class QCOneConfigRejectedTimeHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.QCOneRejectedTime` values loaded
        from the TOML file
    """
    network: str
    station: str
    component: str
    starttime: Union[datetime.datetime, datetime.date]
    endtime: Union[datetime.datetime, datetime.date]


@dataclass
class QCOneConfigHolder:
    """
    This simple dataclass is just helping to validate :class:`~noiz.models.QCOneConfig` values loaded from the TOML file
    """

    null_treatment_policy: NullTreatmentPolicy = NullTreatmentPolicy.PASS
    starttime: Union[datetime.datetime, datetime.date] = datetime.date(2010, 1, 1)
    endtime: Union[datetime.datetime, datetime.date] = datetime.date(2030, 1, 1)
    avg_gps_time_error_min: Optional[float] = None
    avg_gps_time_error_max: Optional[float] = None
    avg_gps_time_uncertainty_min: Optional[float] = None
    avg_gps_time_uncertainty_max: Optional[float] = None
    rejected_times: Union[Tuple[QCOneConfigRejectedTimeHolder, ...], List[QCOneConfigRejectedTimeHolder]] = tuple()
    signal_energy_min: Optional[float] = None
    signal_energy_max: Optional[float] = None
    signal_min_value_min: Optional[float] = None
    signal_min_value_max: Optional[float] = None
    signal_max_value_min: Optional[float] = None
    signal_max_value_max: Optional[float] = None
    signal_mean_value_min: Optional[float] = None
    signal_mean_value_max: Optional[float] = None
    signal_variance_min: Optional[float] = None
    signal_variance_max: Optional[float] = None
    signal_skewness_min: Optional[float] = None
    signal_skewness_max: Optional[float] = None
    signal_kurtosis_min: Optional[float] = None
    signal_kurtosis_max: Optional[float] = None
