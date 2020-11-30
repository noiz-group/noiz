import pytest
pytestmark = [pytest.mark.system, pytest.mark.cli]

from click.testing import CliRunner
from pathlib import Path
import shutil

from noiz.cli import cli
from noiz.app import create_app
from noiz.api.component import fetch_components


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

    def test_add_inventory_data(self, workdir_with_content, noiz_app):

        inventory_path = workdir_with_content.joinpath('STI_station_minimal.xml')

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_inventory", str(inventory_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_components = fetch_components()

        assert len(fetched_components) == 9

    def test_add_soh_data_dir(self, workdir_with_content, noiz_app):
        station = 'SI23'
        station_type = 'taurus'
        soh_type = 'gpstime'
        soh_dir = workdir_with_content.joinpath('soh-data', station)

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_soh_dir",
                                     '--station', station,
                                     '--station_type', station_type,
                                     '--soh_type', soh_type,
                                     str(soh_dir)])

        assert result.exit_code == 0

    @pytest.mark.xfail
    def test_add_seismic_data(self):
        assert False