import pytest
pytestmark = [pytest.mark.system, pytest.mark.cli]

from click.testing import CliRunner
from pathlib import Path

from noiz.cli import cli

from noiz.app import create_app
from noiz.api.component import fetch_components


@pytest.mark.system
class TestDataIngestionRoutines:

    def test_add_inventory_data(self):

        inventory_path = Path(__file__).absolute().parent.joinpath('..', 'dataset', 'STI_station_minimal.xml')

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_inventory", str(inventory_path)])

        assert result.exit_code == 0

        app = create_app()
        with app.app_context():
            fetched_components = fetch_components()

        assert len(fetched_components) == 9

    @pytest.mark.xfail
    def test_add_seismic_data(self):
        assert False

    @pytest.mark.xfail
    def test_add_soh_data(self):
        assert False
