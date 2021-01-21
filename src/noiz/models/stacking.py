import datetime
import pandas as pd
from typing import Union, Optional
from pydantic.dataclasses import dataclass
from sqlalchemy.dialects.postgresql import ARRAY

from noiz.models.timespan import TimespanModel
from noiz.database import db


class StackingTimespan(TimespanModel):
    __tablename__ = "stacking_timespan"
    __table_args__ = (
        db.UniqueConstraint("stacking_schema_id", "starttime", name="unique_stack_starttime_per_config"),
        db.UniqueConstraint("stacking_schema_id", "midtime", name="unique_stack_midtime_per_config"),
        db.UniqueConstraint("stacking_schema_id", "endtime", name="unique_stack_endtime_per_config"),
        db.UniqueConstraint(
            "stacking_schema_id", "starttime", "midtime", "endtime", name="unique_stack_times_per_config"
        ),
    )
    stacking_schema_id = db.Column(
        "stacking_schema_id",
        db.Integer,
        db.ForeignKey("stacking_schema.id"),
        nullable=False,
    )

    stacking_schema = db.relationship(
        "StackingSchema", foreign_keys=[stacking_schema_id]
    )


@dataclass
class StackingSchemaHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.StackingSchema` values loaded
        from the TOML file
    """
    qctwo_config_id: int
    starttime: Union[datetime.datetime, datetime.date]
    endtime: Union[datetime.datetime, datetime.date]
    stacking_length: Union[pd.Timedelta, datetime.timedelta, str]
    stacking_step: Optional[Union[pd.Timedelta, datetime.timedelta, str]] = None
    stacking_overlap: Optional[Union[pd.Timedelta, datetime.timedelta, str]] = None


def _validate_timedelta(timedelta: Union[pd.Timedelta, datetime.timedelta, str]):
    if timedelta is None:
        return None
    if isinstance(timedelta, pd.Timedelta):
        return timedelta.to_pytimedelta()
    if isinstance(timedelta, datetime.timedelta):
        return timedelta
    if isinstance(timedelta, str):
        return pd.Timedelta(timedelta).to_pytimedelta()


class StackingSchema(db.Model):
    __tablename__ = "stacking_schema"

    id = db.Column("id", db.Integer, primary_key=True)
    qctwo_config_id = db.Column("qctwo_config_id", db.Integer, db.ForeignKey("qctwo_config.id"), nullable=False)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    stacking_length = db.Column("stacking_length", db.Interval, nullable=False)
    stacking_overlap = db.Column("stacking_overlap", db.Interval, nullable=False)

    qctwo_config = db.relationship("QCTwoConfig", foreign_keys=[qctwo_config_id], uselist=False, lazy="joined")

    def __init__(self, **kwargs):
        self.starttime = kwargs.get("starttime", None)
        self.endtime = kwargs.get("endtime", None)
        self.stacking_length = _validate_timedelta(
            kwargs.get("stacking_length", None)
        )
        self.stacking_step = _validate_timedelta(kwargs.get("stacking_step", None))
        self.stacking_overlap = _validate_timedelta(
            kwargs.get("stacking_overlap", None)
        )

        if self.stacking_step is None and self.stacking_overlap is None:
            raise ValueError("You have to provide either stacking_step or stacking_overlap.")

        if self.stacking_step is not None and self.stacking_overlap is not None:
            raise ValueError("You cannot provide stacking_step and stacking overlap at the same time.")

        if self.stacking_step is not None and self.stacking_overlap is None:
            self._calclulate_overlap()

        if self.stacking_step is None and self.stacking_overlap is not None:
            self._calculate_stacking_step()

    def _calclulate_overlap(self):
        self.stacking_overlap = self.stacking_length - self.stacking_step

    def _calculate_stacking_step(self):
        self.stacking_step = self.stacking_length - self.stacking_overlap


ccf_ccfstack_association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column(
        "crosscorrelation_id", db.BigInteger, db.ForeignKey("crosscorrelation.id")
    ),
    db.Column("ccfstack_id", db.BigInteger, db.ForeignKey("ccfstack.id")),
)


class CCFStack(db.Model):
    __tablename__ = "ccfstack"
    __table_args__ = (
        db.UniqueConstraint(
            "stacking_timespan_id", "componentpair_id", name="unique_stack_per_pair"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    stacking_timespan_id = db.Column(
        "stacking_timespan_id",
        db.BigInteger,
        db.ForeignKey("stacking_timespan.id"),
        nullable=False,
    )
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair.id"),
        nullable=False,
    )
    stack = db.Column("stack", ARRAY(db.Float), nullable=False)
    no_ccfs = db.Column("no_ccfs", db.Integer, nullable=False)

    ccfs = db.relationship(
        "Crosscorrelation", secondary=ccf_ccfstack_association_table, back_populates="stacks"
    )
