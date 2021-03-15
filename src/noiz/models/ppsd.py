from pathlib import Path

from noiz.database import db
from noiz.exceptions import MissingDataFileException


class PPSDFile(db.Model):
    __tablename__ = "ppsd_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class PPSDResult(db.Model):
    __tablename__ = "ppsd_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id", "ppsd_params_id", name="unique_ppsd_per_config_per_timespan"
        ),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    ppsd_params_id = db.Column("ppsd_params_id", db.Integer, db.ForeignKey("ppsd_params.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)

    ppsd_file_id = db.Column(
        "ppsd_file_id",
        db.BigInteger,
        db.ForeignKey("ppsd_file.id"),
        nullable=True,
    )

    timespan = db.relationship(
        "Timespan",
        foreign_keys=[timespan_id],
        uselist=False,
        lazy="joined",
    )
    datachunk = db.relationship(
        "Datachunk",
        foreign_keys=[datachunk_id],
        uselist=False,
        lazy="joined",
    )

    ppsd_params = db.relationship(
        "PPSDParams",
        foreign_keys=[ppsd_params_id],
        uselist=False,
        lazy="joined",
    )

    ppsd_file = db.relationship(
        "PPSDFile",
        foreign_keys=[ppsd_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self):
        filepath = Path(self.ppsd_file.filepath)
        if filepath.exists:
            raise NotImplementedError("Not yet implemented, use np.load()")
        else:
            raise MissingDataFileException(f"Result file for PPSDResult {self} is missing")
