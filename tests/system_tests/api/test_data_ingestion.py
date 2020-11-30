import pytest
pytestmark = [pytest.mark.system, pytest.mark.api]

from pathlib import Path
import os


@pytest.mark.system
class TestDataIngestionRoutines:
    def test_existence_of_processed_data_dir(self, noiz_app):
        assert Path(noiz_app.noiz_config['processed_data_dir']).exists()

    @pytest.mark.xfail
    def test_add_seismic_data(self):
        assert False
