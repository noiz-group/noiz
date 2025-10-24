# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from noiz.database import db
from noiz.models.mixins import ULIDMixin
from sqlalchemy.ext.associationproxy import association_proxy


association_table_soh_instr = db.Table(
    "soh_instrument_association",
    db.metadata,
    db.Column("component_id", db.String(26), db.ForeignKey("component.id")),
    db.Column("soh_instrument_id", db.String(26), db.ForeignKey("soh_instrument.id")),
    db.UniqueConstraint("component_id", "soh_instrument_id"),
)


class SohInstrument(ULIDMixin, db.Model):
    __tablename__ = "soh_instrument"
    __table_args__ = (db.UniqueConstraint("datetime", "z_component_id"),)  # formerly: unique_soh_instrument

    # id field from ULIDMixin (ULID primary key)
    z_component_id = db.Column("z_component_id", db.String(26), db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    voltage = db.Column("voltage", db.Float, nullable=True)
    current = db.Column("current", db.Float, nullable=True)
    temperature = db.Column("temperature", db.Float, nullable=True)
    device_id = db.Column("device_id", db.String(26), db.ForeignKey("device.id"), nullable=True)

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    z_component = db.relationship("Component", foreign_keys=[z_component_id])
    components = db.relationship("Component", secondary=association_table_soh_instr)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "voltage": self.voltage,
            "current": self.current,
            "temperature": self.temperature,
        }


association_table_soh_gps = db.Table(
    "soh_gps_association",
    db.metadata,
    db.Column("component_id", db.String(26), db.ForeignKey("component.id")),
    db.Column("soh_gps_id", db.String(26), db.ForeignKey("soh_gps.id")),
    db.UniqueConstraint("component_id", "soh_gps_id"),
)


class SohGps(ULIDMixin, db.Model):
    __tablename__ = "soh_gps"
    __table_args__ = (db.UniqueConstraint("datetime", "z_component_id"),)  # formerly: unique_soh_gps

    # id field from ULIDMixin (ULID primary key)
    z_component_id = db.Column("z_component_id", db.String(26), db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    time_error = db.Column("time_error", db.Float, nullable=True)
    time_uncertainty = db.Column("time_uncertainty", db.Float, nullable=True)
    device_id = db.Column("device_id", db.String(26), db.ForeignKey("device.id"), nullable=True)

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    z_component = db.relationship("Component", foreign_keys=[z_component_id])
    components = db.relationship("Component", secondary=association_table_soh_gps)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "time_error": self.time_error,
            "time_uncertainty": self.time_uncertainty,
        }


association_table_averaged_soh_gps_components = db.Table(
    "averaged_soh_gps_association_components",
    db.metadata,
    db.Column("component_id", db.String(26), db.ForeignKey("component.id")),
    db.Column("averaged_soh_gps_id", db.String(26), db.ForeignKey("averaged_soh_gps.id")),
    db.UniqueConstraint("component_id", "averaged_soh_gps_id"),
)


class AveragedSohGps(ULIDMixin, db.Model):
    __tablename__ = "averaged_soh_gps"
    __table_args__ = (db.UniqueConstraint("timespan_id", "z_component_id"),)  # formerly: unique_averaged_soh_gps

    # id field from ULIDMixin (ULID primary key)
    timespan_id = db.Column("timespan_id", db.String(26), db.ForeignKey("timespan.id"), nullable=False)
    z_component_id = db.Column("z_component_id", db.String(26), db.ForeignKey("component.id"))
    time_error = db.Column("time_error", db.Float, nullable=True)
    time_uncertainty = db.Column("time_uncertainty", db.Float, nullable=True)
    device_id = db.Column("device_id", db.String(26), db.ForeignKey("device.id"), nullable=True)

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    z_component = db.relationship("Component", foreign_keys=[z_component_id])
    components = db.relationship("Component", secondary=lambda: association_table_averaged_soh_gps_components)

    component_ids = association_proxy("components", "id")
