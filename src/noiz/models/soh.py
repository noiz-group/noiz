from noiz.database import db


association_table_soh_instr = db.Table(
    "soh_instrument_association",
    db.metadata,
    db.Column(
        "component_id", db.BigInteger, db.ForeignKey("component.id")
    ),
    db.Column("soh_instrument_id", db.BigInteger, db.ForeignKey("soh_instrument.id")),
    db.UniqueConstraint("component_id", "soh_instrument_id"),
)


class SohInstrument(db.Model):
    __tablename__ = "soh_instrument"
    __table_args__ = (
        db.UniqueConstraint(
            "datetime", "z_component_id", name="unique_timestamp_per_station_in_sohinstrument"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    z_component_id = db.Column("z_component_id", db.Integer, db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    voltage = db.Column("voltage", db.Float, nullable=True)
    current = db.Column("current", db.Float, nullable=True)
    temperature = db.Column("temperature", db.Float, nullable=True)

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
    db.Column(
        "component_id", db.BigInteger, db.ForeignKey("component.id")
    ),
    db.Column("soh_gps_id", db.BigInteger, db.ForeignKey("soh_gps.id")),
    db.UniqueConstraint("component_id", "soh_gps_id"),
)


class SohGps(db.Model):
    __tablename__ = "soh_gps"
    __table_args__ = (
        db.UniqueConstraint(
            "datetime", "z_component_id", name="unique_timestamp_per_station_in_sohgps"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    z_component_id = db.Column("z_component_id", db.Integer, db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    time_error = db.Column("time_error", db.Float, nullable=True)
    time_uncertainty = db.Column("time_uncertainty", db.Float, nullable=True)

    z_component = db.relationship("Component", foreign_keys=[z_component_id])
    components = db.relationship("Component", secondary=association_table_soh_gps)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "time_error": self.time_error,
            "time_uncertainty": self.time_uncertainty,
        }
