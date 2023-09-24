# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

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

    datachunk_params_id = db.Column("datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"))
    null_policy = db.Column("null_policy", db.UnicodeText, default=NullTreatmentPolicy.PASS.value, nullable=False)
    strict_gps = db.Column("strict_gps", db.Boolean, default=False, nullable=False)
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

    datachunk_params = db.relationship(
        "DatachunkParams",
        uselist=True,
        lazy="joined"
    )

    processed_datachunk_params = db.relationship(
        "ProcessedDatachunkParams",
        uselist=True,
        back_populates="qcone_config",
    )

    beamforming_params = db.relationship(
        "BeamformingParams",
        uselist=True,
        back_populates="qcone_config",
    )

    def uses_gps(self) -> bool:
        """
        Checks if any of the GPS checks is defined.

        :return: If any of GPS checks is defined
        :rtype: bool
        """
        res = any([x is not None for x in (
            self.avg_gps_time_error_min,
            self.avg_gps_time_error_max,
            self.avg_gps_time_uncertainty_max,
            self.avg_gps_time_uncertainty_min
        )])
        return res

    @cached_property
    def uses_stats(self) -> bool:
        """
        Checks if any of the Stats checks is defined.

        :return: Returns True if any of the Stats is defined
        :rtype: bool
        """
        res = any([x is not None for x in (
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

    datachunk_params_id: int
    null_treatment_policy: NullTreatmentPolicy = NullTreatmentPolicy.PASS
    strict_gps: bool = False
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


class QCTwoRejectedTime(db.Model):
    __tablename__ = "qctwo_rejected_time_periods"
    id = db.Column("id", db.Integer, primary_key=True)

    qctwo_config_id = db.Column("qctwo_config_id", db.Integer, db.ForeignKey("qctwo_config.id"))
    componentpair_id = db.Column("componentpair_id", db.Integer, db.ForeignKey("componentpair_cartesian.id"))
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    qctwo_config = db.relationship(
        "QCTwoConfig",
        uselist=False,
        back_populates="time_periods_rejected",
        foreign_keys=[qctwo_config_id]
    )
    component_pair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id])


class QCTwoConfig(db.Model):
    __tablename__ = "qctwo_config"

    id = db.Column("id", db.Integer, primary_key=True)

    crosscorrelation_cartesian_params_id = db.Column(
        "crosscorrelation_cartesian_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian_params.id"))
    null_policy = db.Column("null_policy", db.UnicodeText, default=NullTreatmentPolicy.PASS.value, nullable=False)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    time_periods_rejected: List[QCTwoRejectedTime] = db.relationship(
        "QCTwoRejectedTime",
        uselist=True,
        back_populates="qctwo_config",
        lazy="joined"
    )

    crosscorrelation_cartesian_params = db.relationship(
        "CrosscorrelationCartesianParams",
        uselist=True,
        lazy="joined"
    )

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
    def componentpair_ids_rejected_times(self) -> Tuple[int, ...]:
        return tuple([x.componentpair_id for x in self.time_periods_rejected])


class QCTwoResults(db.Model):
    __tablename__ = "qctwo_results"
    __table_args__ = (
        db.UniqueConstraint(
            "crosscorrelation_cartesian_id", "qctwo_config_id", name="unique_qctwo_results_per_config_per_ccf"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)

    qctwo_config_id = db.Column("qctwo_config_id", db.Integer, db.ForeignKey("qctwo_config.id"))
    crosscorrelation_cartesian_id = db.Column("crosscorrelation_cartesian_id", db.Integer, db.ForeignKey("crosscorrelation_cartesiannew.id"))

    starttime = db.Column("starttime", db.Boolean, nullable=False)
    endtime = db.Column("endtime", db.Boolean, nullable=False)
    accepted_time = db.Column("accepted_time", db.Boolean, nullable=False)

    qctwo_config = db.relationship("QCTwoConfig", foreign_keys=[qctwo_config_id])
    crosscorrelation_cartesian = db.relationship("CrosscorrelationCartesian", foreign_keys=[crosscorrelation_cartesian_id])

    def is_passing(self) -> bool:
        """
        Checks if all values of QCTwo are True.

        :return: If QCTwo is passing for that chunk
        :rtype: bool
        """
        ret = all((
            self.starttime,
            self.endtime,
            self.accepted_time,
        ))
        return ret


@dataclass
class QCTwoConfigRejectedTimeHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.QCOneRejectedTime` values loaded
        from the TOML file
    """
    network_a: str
    station_a: str
    component_a: str
    network_b: str
    station_b: str
    component_b: str
    starttime: Union[datetime.datetime, datetime.date]
    endtime: Union[datetime.datetime, datetime.date]


@dataclass
class QCTwoConfigHolder:
    """
    This simple dataclass is just helping to validate :class:`~noiz.models.QCOneConfig` values loaded from the TOML file
    """

    crosscorrelation_cartesian_params_id: int
    null_treatment_policy: NullTreatmentPolicy = NullTreatmentPolicy.PASS
    starttime: Union[datetime.datetime, datetime.date] = datetime.date(2010, 1, 1)
    endtime: Union[datetime.datetime, datetime.date] = datetime.date(2030, 1, 1)
    rejected_times: Union[Tuple[QCTwoConfigRejectedTimeHolder, ...], List[QCTwoConfigRejectedTimeHolder]] = tuple()
