import pytest
from noiz.models.timespan import Timespan

pytestmark = [pytest.mark.system, pytest.mark.cli]

from click.testing import CliRunner
import datetime
from pathlib import Path
import shutil

from noiz.app import create_app
from noiz.api.component import fetch_components
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.cli import cli
from noiz.models.processing_params import DatachunkParams


@pytest.fixture(scope="class")
def workdir_with_content(tmp_path_factory) -> Path:
    original_data_dir: Path = Path(__file__).parent.joinpath("..", "dataset")

    test_workdir = tmp_path_factory.mktemp("workdir")
    shutil.copytree(src=original_data_dir, dst=test_workdir, dirs_exist_ok=True)

    return test_workdir


@pytest.fixture(scope="class")
def noiz_app():
    app = create_app(logging_level="CRITICAL")
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

    def test_add_seismic_data(self, workdir_with_content, noiz_app):
        basedir = workdir_with_content.joinpath('seismic-data')

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_seismic_data",
                                     "--filename_pattern", "*.???.resampled",
                                     str(basedir)])

        assert result.exit_code == 0
        from noiz.models.time_series_index import Tsindex
        from noiz.database import db

        with noiz_app.app_context():
            found_in_db = db.session.query(Tsindex).all()
        assert len(found_in_db) == 18
        assert isinstance(found_in_db[0], Tsindex)

    def test_insert_datachunk_params(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('datachunk_params.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_datachunk_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_config = fetch_processing_config_by_id(id=1)

        assert isinstance(fetched_config, DatachunkParams)
        # TODO Add checking for count of datachunk params

    def test_insert_timespans(self, workdir_with_content, noiz_app):

        startdate = datetime.datetime(2019, 9, 30)
        enddate = datetime.datetime(2019, 10, 3)
        delta = datetime.timedelta(days=3)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "data", "add_timespans",
            "-sd", startdate.strftime("%Y-%m-%d"),
            "-ed", enddate.strftime("%Y-%m-%d"),
            "-wl", "1800",
            "--add_to_db"
        ])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_timespans = fetch_timespans_between_dates(
                starttime=startdate-delta,
                endtime=enddate+delta,
            )
        fetched_timespans = list(fetched_timespans)

        assert isinstance(fetched_timespans[0], Timespan)
        assert len(fetched_timespans) == 145
