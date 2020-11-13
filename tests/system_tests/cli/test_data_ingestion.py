import pytest
pytestmark = [pytest.mark.system, pytest.mark.cli]


@pytest.mark.system
class TestDataIngestionRoutines:

    @pytest.mark.xfail
    def test_add_seismic_data(self):
        assert False
