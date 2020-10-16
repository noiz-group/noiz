from noiz.database import db


class SohEnvironment(db.Model):
    __tablename__ = "soh_environment"
    __table_args__ = (
        db.UniqueConstraint(
            "datetime", "component_id", name="unique_timestamp_per_station"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    voltage = db.Column("voltage", db.Float, nullable=True)
    current = db.Column("current", db.Float, nullable=True)
    temperature = db.Column("temperature", db.Float, nullable=True)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "voltage": self.voltage,
            "current": self.current,
            "temperature": self.temperature,
        }


class SohGps(db.Model):
    __tablename__ = "soh_gps"
    __table_args__ = (
        db.UniqueConstraint(
            "datetime", "component_id", name="unique_timestamp_per_station"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    time_error = db.Column("voltage", db.Integer, nullable=True)
    time_uncertainty = db.Column("voltage", db.Integer, nullable=True)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "time_error": self.time_error,
            "time_uncertainty": self.time_uncertainty,
        }
