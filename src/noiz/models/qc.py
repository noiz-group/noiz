from noiz.database import db


association_qc_one_component_rejected = db.Table(
    "association_qc_one_component_rejected",
    db.metadata,
    db.Column(
        "component_id", db.BigInteger, db.ForeignKey("component.id")
    ),
    db.Column("qc_one_id", db.BigInteger, db.ForeignKey("qc_one.id")),
    db.UniqueConstraint("component_id", "qc_one_id"),
)


class QCOne(db.Model):
    __tablename__ = "qc_one"

    id = db.Column("id", db.Integer, primary_key=True)

    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Float, nullable=False)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Float, nullable=False)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Float, nullable=False)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Float, nullable=False)

    components_rejected = db.relationship("Component", secondary=association_qc_one_component_rejected, lazy="joined")
    time_periods_rejected = db.relationship("QCOneRejectedTime", back_populates="qc_one", lazy="joined")


class QCOneRejectedTime(db.Model):
    __tablename__ = "qc_one_rejected_time_periods"
    id = db.Column("id", db.Integer, primary_key=True)

    qc_one_id = db.Column("qc_one_id", db.Integer, db.ForeignKey("qc_one.id"))
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    qc_one = db.relationship("QCOne", back_populates="time_periods_rejected")


class QCOneResults(db.Model):
    __tablename__ = "qc_one_results"

    id = db.Column("id", db.BigInteger, primary_key=True)

    qc_one_id = db.Column("qc_one_id", db.Integer, db.ForeignKey("qc_one.id"))
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"))

    starttime = db.Column("starttime", db.Boolean, nullable=False)
    endtime = db.Column("endtime", db.Boolean, nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Boolean, nullable=False)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Boolean, nullable=False)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Boolean, nullable=False)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Boolean, nullable=False)
    components_rejected = db.Column("components_rejected", db.Boolean, nullable=False)
    time_periods_rejected = db.Column("time_periods_rejected", db.Boolean, nullable=False)
