# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.
from typing import Tuple

from noiz.database import db
from noiz.models import Component


class ComponentPairCartesian(db.Model):
    __tablename__ = "componentpair_cartesian"
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
    component_code_pair = db.Column("component_code_pair", db.UnicodeText, nullable=False)
    autocorrelation = db.Column("autocorrelation", db.Boolean, nullable=False)
    intracorrelation = db.Column("intracorrelation", db.Boolean, nullable=False)
    azimuth = db.Column("azimuth", db.Float, nullable=False)
    backazimuth = db.Column("backazimuth", db.Float, nullable=False)
    distance = db.Column("distance", db.Float, nullable=False)
    arcdistance = db.Column("arcdistance", db.Float, nullable=False)

    component_a = db.relationship("Component", foreign_keys=[component_a_id], lazy="joined", )
    component_b = db.relationship("Component", foreign_keys=[component_b_id], lazy="joined", )

    def __str__(self):
        return f"{self.component_a}-{self.component_b}"

    def __repr__(self):
        return f"{self.component_a}-{self.component_b}"

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


class ComponentPairCylindrical(db.Model):
    __tablename__ = "componentpair_cylindrical"

    id = db.Column("id", db.Integer, primary_key=True)
    component_aE_id = db.Column(
        "component_aE_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )
    component_bE_id = db.Column(
        "component_bE_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )
    component_aN_id = db.Column(
        "component_aN_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )
    component_bN_id = db.Column(
        "component_bN_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )
    component_aZ_id = db.Column(
        "component_aZ_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )
    component_bZ_id = db.Column(
        "component_bZ_id", db.Integer, db.ForeignKey("component.id"), nullable=True
    )

    component_cylindrical_code_pair = db.Column("component_cylindrical_code_pair", db.UnicodeText, nullable=False)
    autocorrelation = db.Column("autocorrelation", db.Boolean, nullable=False)
    intracorrelation = db.Column("intracorrelation", db.Boolean, nullable=False)
    azimuth = db.Column("azimuth", db.Float, nullable=False)
    backazimuth = db.Column("backazimuth", db.Float, nullable=False)
    distance = db.Column("distance", db.Float, nullable=False)
    arcdistance = db.Column("arcdistance", db.Float, nullable=False)

    component_aE = db.relationship("Component", foreign_keys=[component_aE_id], lazy="joined", )
    component_bE = db.relationship("Component", foreign_keys=[component_bE_id], lazy="joined", )
    component_aN = db.relationship("Component", foreign_keys=[component_aN_id], lazy="joined", )
    component_bN = db.relationship("Component", foreign_keys=[component_bN_id], lazy="joined", )
    component_aZ = db.relationship("Component", foreign_keys=[component_aZ_id], lazy="joined", )
    component_bZ = db.relationship("Component", foreign_keys=[component_bZ_id], lazy="joined", )

    def get_all_components(self) -> Tuple[Component, ...]:
        """
        Returns a tuple with all components that belong into this ComponentPairCylindrical

        :rtype: Tuple[Component, ...]
        """
        return (
            self.component_aE,
            self.component_bE,
            self.component_aN,
            self.component_bN,
            self.component_aZ,
            self.component_bZ,
        )
