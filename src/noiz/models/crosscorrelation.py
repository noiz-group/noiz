from sqlalchemy.dialects.postgresql import ARRAY
from noiz.database import db

from noiz.models.stacking import association_table


class Crosscorrelation(db.Model):
    __tablename__ = "crosscorrelation"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "processing_params_id",
            name="unique_ccf_per_timespan_per_componentpair_per_processing",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair.id"),
        nullable=False,
    )
    timespan_id = db.Column(
        "timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False
    )
    processing_params_id = db.Column(
        "processing_params_id",
        db.Integer,
        db.ForeignKey("processing_params.id"),
        nullable=False,
    )
    ccf = db.Column("ccf", ARRAY(db.Float))

    componentpair = db.relationship("ComponentPair", foreign_keys=[componentpair_id])
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    processing_params = db.relationship(
        "DatachunkPreprocessingConfig", foreign_keys=[processing_params_id]
    )
    stacks = db.relationship(
        "CCFStack", secondary=association_table, back_populates="ccfs"
    )
