from noiz.models.timespan import TimespanModel
from noiz.database import db


class StackingSchema(db.Model):
    __tablename__ = "stacking_schema"

    id = db.Column("id", db.Integer, primary_key=True)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    stacking_length = db.Column("stacking_length", db.INTERVAL, nullable=False)
    stacking_step = db.Column("stacking_step", db.INTERVAL, nullable=False)


class StackingTimespan(TimespanModel):
    __tablename__ = "stacking_timespan"
    __table_args__ = (
        db.UniqueConstraint("starttime", name="unique_starttime"),
        db.UniqueConstraint("midtime", name="unique_midtime"),
        db.UniqueConstraint("endtime", name="unique_endtime"),
        db.UniqueConstraint("starttime", "midtime", "endtime", name="unique_times"),
    )
