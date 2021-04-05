import datetime
import pandas as pd
from typing import Union, Optional
from pydantic.dataclasses import dataclass
from sqlalchemy.dialects.postgresql import ARRAY

from noiz.models.timespan import TimespanMixin
from noiz.database import db
from noiz.processing.time_utils import calculate_window_step_or_overlap
from noiz.validation_helpers import validate_as_pytimedelta_or_none


class StackingTimespan(TimespanMixin):
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
    minimum_ccf_count: int
    starttime: Union[datetime.datetime, datetime.date]
    endtime: Union[datetime.datetime, datetime.date]
    stacking_length: Union[pd.Timedelta, datetime.timedelta, str]
    stacking_step: Optional[Union[pd.Timedelta, datetime.timedelta, str]] = None
    stacking_overlap: Optional[Union[pd.Timedelta, datetime.timedelta, str]] = None


class StackingSchema(db.Model):
    __tablename__ = "stacking_schema"

    id = db.Column("id", db.Integer, primary_key=True)
    crosscorrelation_params_id = db.Column(
        "crosscorrelation_params_id", db.Integer, db.ForeignKey("crosscorrelation_params.id"), nullable=False)
    qctwo_config_id = db.Column("qctwo_config_id", db.Integer, db.ForeignKey("qctwo_config.id"), nullable=False)
    minimum_ccf_count = db.Column("minimum_ccf_count", db.Integer, nullable=False)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    stacking_length = db.Column("stacking_length", db.Interval, nullable=False)
    stacking_overlap = db.Column("stacking_overlap", db.Interval, nullable=False)

    crosscorrelation_params = db.relationship("CrosscorrelationParams", foreign_keys=[crosscorrelation_params_id],
                                              uselist=False, lazy="joined")
    qctwo_config = db.relationship("QCTwoConfig", foreign_keys=[qctwo_config_id], uselist=False, lazy="joined")

    def __init__(self, **kwargs):
        for key in (
                "qctwo_config_id",
                "starttime",
                "endtime",
                "stacking_length",
                "minimum_ccf_count",
        ):
            if key not in kwargs.keys():
                raise ValueError(f"Required value of {key} missing. You have to provide it.")

        self.crosscorrelation_params_id = kwargs.get("crosscorrelation_params_id")
        self.qctwo_config_id = kwargs.get("qctwo_config_id")
        self.minimum_ccf_count = kwargs.get("minimum_ccf_count")
        self.starttime = kwargs.get("starttime")
        self.endtime = kwargs.get("endtime")

        self.stacking_length = validate_as_pytimedelta_or_none(kwargs.get("stacking_length", None))
        self.stacking_step = validate_as_pytimedelta_or_none(kwargs.get("stacking_step", None))
        self.stacking_overlap = validate_as_pytimedelta_or_none(kwargs.get("stacking_overlap", None))

        if self.stacking_step is None and self.stacking_overlap is None:
            raise ValueError("You have to provide either stacking_step or stacking_overlap.")

        if self.stacking_step is not None and self.stacking_overlap is not None:
            raise ValueError("You cannot provide stacking_step and stacking overlap at the same time.")

        if self.stacking_step is not None and self.stacking_overlap is None:
            self._calclulate_overlap()

        if self.stacking_step is None and self.stacking_overlap is not None:
            self._calculate_stacking_step()

    def _calclulate_overlap(self):
        self.stacking_overlap = calculate_window_step_or_overlap(self.stacking_length, self.stacking_step)

    def _calculate_stacking_step(self):
        self.stacking_step = calculate_window_step_or_overlap(self.stacking_length, self.stacking_overlap)


ccf_ccfstack_association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column(
        "crosscorrelation_id", db.BigInteger, db.ForeignKey("crosscorrelationnew.id")
    ),
    db.Column("ccfstack_id", db.BigInteger, db.ForeignKey("ccfstack.id")),
)


class CCFStack(db.Model):
    __tablename__ = "ccfstack"
    __table_args__ = (
        db.UniqueConstraint(
            "stacking_timespan_id", "stacking_schema_id", "componentpair_id", name="unique_stack_per_pair_per_config"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    stacking_timespan_id = db.Column(
        "stacking_timespan_id",
        db.BigInteger,
        db.ForeignKey("stacking_timespan.id"),
        nullable=False,
    )
    stacking_schema_id = db.Column(
        "stacking_schema_id",
        db.Integer,
        db.ForeignKey("stacking_schema.id"),
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

    stacking_timespan = db.relationship(
        "StackingTimespan",
        foreign_keys=[stacking_timespan_id],
        uselist=False,
        lazy="joined"
    )

    stacking_schema = db.relationship(
        "StackingSchema",
        foreign_keys=[stacking_schema_id],
        uselist=False,
        lazy="joined"
    )
