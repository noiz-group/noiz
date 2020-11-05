from noiz.database import db


# TODO Add null treatment policy field
class QCOne(db.Model):
    __tablename__ = "qc_one"

    id = db.Column("id", db.Integer, primary_key=True)

    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Float, nullable=False)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Float, nullable=False)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Float, nullable=False)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Float, nullable=False)

    time_periods_rejected = db.relationship("QCOneRejectedTime", back_populates="qc_one", lazy="joined")


class QCOneRejectedTime(db.Model):
    __tablename__ = "qc_one_rejected_time_periods"
    id = db.Column("id", db.Integer, primary_key=True)

    qc_one_id = db.Column("qc_one_id", db.Integer, db.ForeignKey("qc_one.id"))
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    qc_one = db.relationship("QCOne", back_populates="time_periods_rejected", foreign_keys=[qc_one_id])
    component = db.relationship("Component", foreign_keys=[component_id])


class QCOneResults(db.Model):
    __tablename__ = "qc_one_results"

    id = db.Column("id", db.BigInteger, primary_key=True)

    qc_one_id = db.Column("qc_one_id", db.Integer, db.ForeignKey("qc_one.id"))
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"))

    starttime = db.Column("starttime", db.Boolean, nullable=False)
    endtime = db.Column("endtime", db.Boolean, nullable=False)
    avg_gps_time_error_min = db.Column("avg_gps_time_error_min", db.Boolean, nullable=True)
    avg_gps_time_error_max = db.Column("avg_gps_time_error_max", db.Boolean, nullable=True)
    avg_gps_time_uncertainty_min = db.Column("avg_gps_time_uncertainty_min", db.Boolean, nullable=True)
    avg_gps_time_uncertainty_max = db.Column("avg_gps_time_uncertainty_max", db.Boolean, nullable=True)
    time_periods_rejected = db.Column("time_periods_rejected", db.Boolean, nullable=False)

    qc_one = db.relationship("QCOne", foreign_keys=[qc_one_id])
    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id])
