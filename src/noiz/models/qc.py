from typing import List

import datetime
from pydantic.dataclasses import dataclass

from noiz.database import db
from noiz.globals import ExtendedEnum


class NullTreatmentPolicy(ExtendedEnum):
    FAIL = "fail"
    PASS = "pass"


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

    time_periods_rejected = db.relationship("QCOneRejectedTime", back_populates="qcone_config", lazy="joined")


class QCOneRejectedTime(db.Model):
    __tablename__ = "qcone_rejected_time_periods"
    id = db.Column("id", db.Integer, primary_key=True)

    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"))
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    qcone_config = db.relationship("QCOneConfig", back_populates="time_periods_rejected",
                                    foreign_keys=[qcone_config_id])
    component = db.relationship("Component", foreign_keys=[component_id])


class QCOneResults(db.Model):
    __tablename__ = "qcone_results"

    id = db.Column("id", db.BigInteger, primary_key=True)

    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"))
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"))

    starttime = db.Column("starttime", db.Boolean, nullable=False)
    endtime = db.Column("endtime", db.Boolean, nullable=False)
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


@dataclass
class QCOneRejectedTimeHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.QCOneRejectedTime` values loaded
        from the TOML file
    """
    network: str
    station: str
    component: str
    starttime: datetime.datetime
    endtime: datetime.datetime


@dataclass
class QCOneHolder:
    """
    This simple dataclass is just helping to validate :class:`~noiz.models.QCOne` values loaded from the TOML file
    """

    null_treatment_policy: NullTreatmentPolicy
    starttime: datetime.datetime
    endtime: datetime.datetime
    avg_gps_time_error_min: float
    avg_gps_time_error_max: float
    avg_gps_time_uncertainty_min: float
    avg_gps_time_uncertainty_max: float
    rejected_times: List[QCOneRejectedTimeHolder]
    signal_energy_min: float
    signal_energy_max: float
    signal_min_value_min: float
    signal_min_value_max: float
    signal_max_value_min: float
    signal_max_value_max: float
    signal_mean_value_min: float
    signal_mean_value_max: float
    signal_variance_min: float
    signal_variance_max: float
    signal_skewness_min: float
    signal_skewness_max: float
    signal_kurtosis_min: float
    signal_kurtosis_max: float
