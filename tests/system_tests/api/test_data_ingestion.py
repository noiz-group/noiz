import pytest
pytestmark = [pytest.mark.system, pytest.mark.api]

import os
from pathlib import Path
import shutil

from noiz.app import create_app


@pytest.fixture(scope="class")
def workdir_with_content(tmp_path_factory) -> Path:
    original_data_dir: Path = Path(__file__).parent.joinpath("..", "dataset")

    test_workdir = tmp_path_factory.mktemp("workdir")
    shutil.copytree(src=original_data_dir, dst=test_workdir, dirs_exist_ok=True)

    return test_workdir


@pytest.fixture(scope="class")
def noiz_app():
    app = create_app()
    return app


@pytest.mark.system
class TestDataIngestionRoutines:
    def test_existence_of_processed_data_dir(self, noiz_app):
        assert Path(noiz_app.noiz_config['processed_data_dir']).exists()

    @pytest.mark.xfail
    def test_add_seismic_data(self):
        assert False
