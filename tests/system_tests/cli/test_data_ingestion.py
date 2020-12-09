import pytest
pytestmark = [pytest.mark.system, pytest.mark.cli]

from click.testing import CliRunner
import datetime
from pathlib import Path
import shutil

from noiz.api.component import fetch_components
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.app import create_app
from noiz.cli import cli
from noiz.database import db
from noiz.models.processing_params import DatachunkParams
from noiz.models.datachunk import Datachunk, DatachunkStats
from noiz.models.timespan import Timespan
from noiz.models.soh import SohGps, SohInstrument


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
    def test_existence_of_processed_data_dir(self, noiz_app):
        assert Path(noiz_app.noiz_config['processed_data_dir']).exists()

    def test_add_inventory_data(self, workdir_with_content, noiz_app):

        inventory_path = workdir_with_content.joinpath('STI_station_minimal.xml')

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_inventory", str(inventory_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_components = fetch_components()

        assert len(fetched_components) == 9

    @pytest.mark.xfail
    def test_add_soh_files(self, noiz_app):
        assert False

    def test_add_soh_files_miniseed_instrument(self, workdir_with_content, noiz_app):
        station = 'SI11'
        station_type = 'centaur'
        soh_type = 'miniseed_gpstime'
        soh_dir = workdir_with_content.joinpath('soh-data', "SI03-all-fields-miniseed")

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_soh_dir",
                                     '--station', station,
                                     '--station_type', station_type,
                                     '--soh_type', soh_type,
                                     str(soh_dir)])

        assert result.exit_code == 0
        with noiz_app.app_context():
            found_in_db = db.session.query(SohGps) \
                .filter(
                SohGps.datetime > datetime.datetime(2017, 11, 24),
                SohGps.datetime < datetime.datetime(2017, 11, 26),
            ).all()
        assert len(found_in_db) == 48

    def test_add_soh_files_miniseed_gpstime(self, workdir_with_content, noiz_app):
        station = 'SI23'
        station_type = 'centaur'
        soh_type = 'miniseed_instrument'
        soh_dir = workdir_with_content.joinpath('soh-data', "SI09-lacking-fields-minised")

        runner = CliRunner()
        result = runner.invoke(cli, ["data", "add_soh_dir",
                                     '--station', station,
                                     '--station_type', station_type,
                                     '--soh_type', soh_type,
                                     str(soh_dir)])

        assert result.exit_code == 0
        with noiz_app.app_context():
            found_in_db = db.session.query(SohInstrument) \
                .filter(
                SohInstrument.datetime > datetime.datetime(2018, 6, 15),
                SohInstrument.datetime < datetime.datetime(2018, 6, 17),
            ).all()
        assert len(found_in_db) == 48

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
        from noiz.models.timeseries import Tsindex
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
            all_configs = DatachunkParams.query.all()

        assert isinstance(fetched_config, DatachunkParams)
        assert len(all_configs) == 1

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

    def test_run_datachunk_creation(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "prepare_datachunks", "-sd", "2019-09-30", "-ed", "2019-10-03"])
        assert result.exit_code == 0

        from noiz.api.datachunk import fetch_datachunks
        with noiz_app.app_context():
            fetched_datachunks = fetch_datachunks()

        assert len(fetched_datachunks) == 855
        assert isinstance(fetched_datachunks[0], Datachunk)

    @pytest.mark.xfail
    def test_plot_datachunk_availability(self, noiz_app):
        assert False

    @pytest.mark.xfail
    def test_plot_raw_gps_soh(self, noiz_app):
        assert False

    @pytest.mark.xfail
    def test_average_soh_gps(self, noiz_app):
        assert False

    @pytest.mark.xfail
    def test_plot_averaged_gps_soh(self, noiz_app):
        assert False

    def test_calc_datachunk_stats(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "calc_datachunk_stats", "-sd", "2019-09-30", "-ed", "2019-10-03"])
        assert result.exit_code == 0

        from noiz.api.datachunk import fetch_datachunks_without_stats

        with noiz_app.app_context():
            datachunks_without_stats = fetch_datachunks_without_stats()
            stats = db.session.query(DatachunkStats).all()
            datachunks = db.session.query(Datachunk).all()
        assert len(datachunks_without_stats) == 0
        assert len(datachunks) == len(stats)
