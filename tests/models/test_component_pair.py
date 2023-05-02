# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest

from noiz.models.component_pair import ComponentPairCartesian


class TestComponentPairCartesian:
    @pytest.mark.xfail
    def test_set_same_station(self):
        assert False

    @pytest.mark.xfail
    def test_set_autocorrelation(self):
        assert False

    @pytest.mark.xfail
    def test_set_intracorrelation(self):
        assert False

    @pytest.mark.xfail
    def test_set_params_from_distaz(self):
        assert False

    @pytest.mark.xfail
    def test__verify_east_west(self):
        assert False
