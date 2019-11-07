from noiz.database import db


class ComponentPair(db.Model):
    __tablename__ = "componentpair"
    __table_args__ = (
        db.UniqueConstraint(
            "component_a_id", "component_b_id", name="single_component_pair"
        ),
    )

    id = db.Column("id", db.Integer, primary_key=True)
    component_a_id = db.Column(
        "component_a_id", db.Integer, db.ForeignKey("component.id"), nullable=False
    )
    component_b_id = db.Column(
        "component_b_id", db.Integer, db.ForeignKey("component.id"), nullable=False
    )
    autocorrelation = db.Column("autocorrelation", db.Boolean, nullable=False)
    intracorrelation = db.Column("intracorrelation", db.Boolean, nullable=False)
    azimuth = db.Column("azimuth", db.Float, nullable=False)
    backazimuth = db.Column("backazimuth", db.Float, nullable=False)
    distance = db.Column("distance", db.Float, nullable=False)
    arcdistance = db.Column("arcdistance", db.Float, nullable=False)

    def _set_same_station(self) -> None:
        self.azimuth = 0
        self.backazimuth = 0
        self.distance = 0
        self.arcdistance = 0
        return

    def set_autocorrelation(self) -> None:
        self.autocorrelation = True
        self.intracorrelation = False
        self._set_same_station()
        return

    def set_intracorrelation(self) -> None:
        self.autocorrelation = False
        self.intracorrelation = True
        self._set_same_station()
        return

    def set_params_from_distaz(self, distaz: dict) -> None:
        self.azimuth = float(distaz["azimuth"])
        self.backazimuth = float(distaz["backazimuth"])
        self.arcdistance = float(distaz["distance"])
        self.distance = float(distaz["distancemeters"])
        self.autocorrelation = False
        self.intracorrelation = False
        if not self._verify_east_west():
            raise ValueError("This pair is not east west in the end.")
        return

    def _verify_east_west(self) -> bool:
        if self.backazimuth >= 180:
            return True
        else:
            return False
