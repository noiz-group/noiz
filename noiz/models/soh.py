from noiz.database import db


class Soh(db.Model):
    __tablename__ = "soh"
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
