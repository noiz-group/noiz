import pytest

from noiz.api.ppsd import fetch_ppsd_params_by_id

pytestmark = [pytest.mark.system, pytest.mark.cli]

from click.testing import CliRunner
import datetime
import numpy as np
from pathlib import Path
import shutil

from noiz.api.component import fetch_components
from noiz.api.component_pair import fetch_componentpairs
from noiz.api.processing_config import fetch_datachunkparams_by_id, fetch_processed_datachunk_params_by_id, \
    fetch_crosscorrelation_params_by_id, fetch_stacking_schema_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.app import create_app
from noiz.cli import cli
from noiz.database import db
from noiz.models import StackingSchema, QCOneResults, QCTwoResults, DatachunkParams, \
    ProcessedDatachunkParams, CrosscorrelationParams, Datachunk, DatachunkStats, ProcessedDatachunk, \
    SohGps, SohInstrument, Timespan, CCFStack, Crosscorrelation
from noiz.models.component import ComponentFile
from noiz.models.beamforming import BeamformingResult, BeamformingFile, BeamformingPeakAverageAbspower
from noiz.models.ppsd import PPSDResult


@pytest.fixture(scope="class")
def workdir_with_content(tmp_path_factory) -> Path:
    original_data_dir: Path = Path(__file__).parent.joinpath("..", "dataset")

    test_workdir = tmp_path_factory.mktemp("workdir")
    shutil.copytree(src=original_data_dir, dst=test_workdir, dirs_exist_ok=True)

    return test_workdir


@pytest.fixture(scope="function")
def empty_workdir(tmp_path_factory) -> Path:
    test_workdir = tmp_path_factory.mktemp("workdir")
    return test_workdir


@pytest.fixture(scope="class")
def noiz_app():
    app = create_app(verbosity=5)
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
            fetched_component_files = db.session.query(ComponentFile).all()
            fetched_componentpairs_normal = fetch_componentpairs()

        assert len(fetched_components) == 9
        assert len(fetched_component_files) == 9
        assert len(fetched_componentpairs_normal) == 27

    def test_fetch_componentpairs(self, noiz_app):
        # TODO remove that test once it's running through API system tests
        kwargs = dict(
            network_codes_a=None,
            station_codes_a=None,
            component_codes_a=None,
            network_codes_b=None,
            station_codes_b=None,
            component_codes_b=None,
            accepted_component_code_pairs=None,
            include_autocorrelation=False,
            include_intracorrelation=False,
            only_autocorrelation=False,
            only_intracorrelation=False,
        )

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            assert len(fetch_componentpairs(**kwargs_mod)) == 27

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['include_autocorrelation'] = True
            kwargs_mod['include_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 54

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['only_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 9

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['only_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 18

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            assert len(fetch_componentpairs(**kwargs_mod)) == 9

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['only_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 6

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11",)
            kwargs_mod['only_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 3

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11",)
            kwargs_mod['only_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 6

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['only_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 12

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11",)
            kwargs_mod['include_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 3

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11",)
            kwargs_mod['include_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 6

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11",)
            kwargs_mod['include_autocorrelation'] = True
            kwargs_mod['include_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 9

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['include_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 15

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['include_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 21

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['include_autocorrelation'] = True
            kwargs_mod['include_intracorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 27

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
            fetched_config = fetch_datachunkparams_by_id(id=1)
            all_configs = DatachunkParams.query.all()

        assert isinstance(fetched_config, DatachunkParams)
        assert len(all_configs) == 1

    def test_add_qcone_config(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('QCOneConfig.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_qcone_config", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.qc import fetch_qcone_config_single
        from noiz.models.qc import QCOneConfig

        with noiz_app.app_context():
            fetched_config = fetch_qcone_config_single(id=1)
            all_configs = QCOneConfig.query.all()

        assert isinstance(fetched_config, QCOneConfig)
        assert len(all_configs) == 1

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['QCOne'].items():
            if key in ("rejected_times", "null_treatment_policy"):
                continue
            # elif key == "null_treatment_policy":
            #     from noiz.models.qc import NullTreatmentPolicy
            #     check.is_true(isinstance(fetched_config.__getattribute__("null_policy"), NullTreatmentPolicy))
            elif key == "strict_gps":
                check.is_false(fetched_config.__getattribute__(key), value)
            elif key in ("starttime", "endtime"):
                check.equal(fetched_config.__getattribute__(key).date(), value)
            else:
                check.almost_equal(fetched_config.__getattribute__(key), value)

        check.equal(len(original_config['QCOne']['rejected_times']), len(fetched_config.time_periods_rejected))

    def test_add_qcone_config_no_rejected_times(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('QCOneConfig_without_rejected_times.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_qcone_config", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.qc import fetch_qcone_config_single
        from noiz.models.qc import QCOneConfig

        with noiz_app.app_context():
            fetched_config = fetch_qcone_config_single(id=2)
            all_configs = QCOneConfig.query.all()

        assert isinstance(fetched_config, QCOneConfig)
        assert len(all_configs) == 2

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['QCOne'].items():
            if key in ("rejected_times", "null_treatment_policy"):
                continue
            # elif key == "null_treatment_policy":
            #     from noiz.models.qc import NullTreatmentPolicy
            #     check.is_true(isinstance(fetched_config.__getattribute__("null_policy"), NullTreatmentPolicy))
            elif key == "strict_gps":
                check.is_false(fetched_config.__getattribute__(key), value)
            elif key in ("starttime", "endtime"):
                check.equal(fetched_config.__getattribute__(key).date(), value)
            else:
                check.almost_equal(fetched_config.__getattribute__(key), value)

        assert len(fetched_config.time_periods_rejected) == 0

    def test_insert_processed_datachunk_params(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('processed_datachunk_params.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_processed_datachunk_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_config = fetch_processed_datachunk_params_by_id(id=1)
            all_configs = ProcessedDatachunkParams.query.all()

        assert isinstance(fetched_config, ProcessedDatachunkParams)
        assert len(all_configs) == 1

    def test_insert_crosscorrelation_params(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('crosscorrelation_params.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_crosscorrelation_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_config = fetch_crosscorrelation_params_by_id(id=1)
            all_configs = CrosscorrelationParams.query.all()

        assert isinstance(fetched_config, CrosscorrelationParams)
        assert len(all_configs) == 1
        assert fetched_config.sampling_rate == 24
        assert fetched_config.correlation_max_lag == 20

    def test_add_qctwo_config(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('QCTwoConfig.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_qctwo_config", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.qc import fetch_qctwo_config_single
        from noiz.models.qc import QCTwoConfig

        with noiz_app.app_context():
            fetched_config = fetch_qctwo_config_single(id=1)
            all_configs = QCTwoConfig.query.all()

        assert isinstance(fetched_config, QCTwoConfig)
        assert len(all_configs) == 1

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['QCTwo'].items():
            if key in ("null_treatment_policy", "rejected_times"):
                continue
            if key in ("starttime", "endtime"):
                check.equal(fetched_config.__getattribute__(key).date(), value)
                continue
            check.almost_equal(fetched_config.__getattribute__(key), value)

        check.equal(len(original_config['QCTwo']['rejected_times']), len(fetched_config.time_periods_rejected))

    def test_add_beamforming_params(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('beamforming_params.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_beamforming_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.beamforming import fetch_beamforming_params_single
        from noiz.models.processing_params import BeamformingParams

        with noiz_app.app_context():
            fetched_config = fetch_beamforming_params_single(id=1)
            all_configs = BeamformingParams.query.all()

        assert isinstance(fetched_config, BeamformingParams)
        assert len(all_configs) == 1

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['BeamformingParams'].items():
            if key in (
                    "prewhiten",
                    "save_average_beamformer_abspower",
                    "save_all_beamformers_abspower",
                    "save_average_beamformer_relpower",
                    "save_all_beamformers_relpower",
                    "extract_peaks_average_beamformer_abspower",
                    "extract_peaks_all_beamformers_abspower",
                    "extract_peaks_average_beamformer_relpower",
                    "extract_peaks_all_beamformers_relpower",
            ):
                check.equal(str(fetched_config.__getattribute__(key)), value)
                continue
            if key == "method":
                check.equal(fetched_config._method, value)
                continue
            if key in (
                    "used_component_codes",
                    "window_length_minimum_periods",
                    "window_step_fraction",
                    "neighborhood_size_xaxis_fraction"
            ):
                # TODO Add check for that value
                continue
            check.almost_equal(fetched_config.__getattribute__(key), value)

    def test_add_ppsd_params(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('ppsd_params.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_ppsd_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.ppsd import fetch_ppsd_params_by_id
        from noiz.models.processing_params import PPSDParams

        with noiz_app.app_context():
            fetched_config = fetch_ppsd_params_by_id(id=1)
            all_configs = PPSDParams.query.all()

        assert isinstance(fetched_config, PPSDParams)
        assert len(all_configs) == 1

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['PPSDParams'].items():
            if key in ("save_all_windows", "save_compressed", "resample"):
                check.equal(str(fetched_config.__getattribute__(key)), value)
            elif key in (
                    "resampled_frequency_start",
                    "resampled_frequency_stop",
                    "resampled_frequency_step",
            ):
                continue
            else:
                check.almost_equal(fetched_config.__getattribute__(key), value)

    def test_add_ppsd_params_resample(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('ppsd_params_resampled.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_ppsd_params", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        import toml
        import pytest_check as check
        from noiz.api.ppsd import fetch_ppsd_params_by_id
        from noiz.models.processing_params import PPSDParams

        with noiz_app.app_context():
            fetched_config = fetch_ppsd_params_by_id(id=2)

        assert isinstance(fetched_config, PPSDParams)

        with open(config_path, 'r') as f:
            original_config = toml.load(f)

        for key, value in original_config['PPSDParams'].items():
            if key in ("save_all_windows", "save_compressed", "resample"):
                check.equal(str(fetched_config.__getattribute__(key)), value)
            else:
                check.almost_equal(fetched_config.__getattribute__(key), value)

    def test_insert_stacking_schema(self, workdir_with_content, noiz_app):

        config_path = workdir_with_content.joinpath('stacking_schema.toml')

        runner = CliRunner()
        result = runner.invoke(cli, ["configs", "add_stacking_schema", "--add_to_db", "-f", str(config_path)])

        assert result.exit_code == 0

        with noiz_app.app_context():
            fetched_config = fetch_stacking_schema_by_id(id=1)
            all_configs = StackingSchema.query.all()

        assert isinstance(fetched_config, StackingSchema)
        assert len(all_configs) == 1
        assert isinstance(fetched_config.starttime, datetime.datetime)
        assert isinstance(fetched_config.endtime, datetime.datetime)
        assert isinstance(fetched_config.stacking_overlap, datetime.timedelta)
        assert isinstance(fetched_config.stacking_length, datetime.timedelta)

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
        result = runner.invoke(cli, ["processing", "prepare_datachunks",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0

        from noiz.api.datachunk import fetch_datachunks
        with noiz_app.app_context():
            fetched_datachunks = fetch_datachunks()

        assert len(fetched_datachunks) == 855
        assert isinstance(fetched_datachunks[0], Datachunk)

    @pytest.mark.xfail
    def test_plot_datachunk_availability(self, noiz_app):
        assert False

    def test_plot_raw_gps_soh(self, noiz_app, empty_workdir):
        exported_filename = "exported_soh.png"
        exported_filepath = empty_workdir.joinpath(exported_filename).absolute()
        runner = CliRunner()
        result = runner.invoke(cli, ["plot", "raw_gps_soh",
                                     "-sd", "2019-09-01",
                                     "-ed", "2019-11-01",
                                     "--savefig",
                                     "-pp", str(exported_filepath)])
        assert result.exit_code == 0
        assert exported_filepath.exists()

    def test_average_soh_gps(self, noiz_app):
        from noiz.api.soh import fetch_averaged_soh_gps_all
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "average_soh_gps",
                                     "-sd", "2019-09-01",
                                     "-ed", "2019-11-01",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            timespans = fetch_timespans_between_dates(
                starttime=datetime.datetime(2019, 9, 1),
                endtime=datetime.datetime(2019, 11, 1),
            )
            components = fetch_components()
            fetched_soh = fetch_averaged_soh_gps_all(components=components, timespans=timespans)
        assert len(fetched_soh) == 97

    @pytest.mark.xfail
    def test_plot_averaged_gps_soh(self, noiz_app):
        assert False

    def test_export_raw_gps_soh(self, noiz_app, empty_workdir):
        exported_filename = "exported_soh.csv"
        exported_filepath = empty_workdir.joinpath(exported_filename).absolute()
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "raw_gps_soh",
                                     "-sd", "2019-09-01",
                                     "-ed", "2019-11-01",
                                     '-p', str(exported_filepath)
                                     ])
        assert result.exit_code == 0
        assert exported_filepath.exists()

    def test_calc_datachunk_stats(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "calc_datachunk_stats",
                                     "-p", "1",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0

        from noiz.api.datachunk import fetch_datachunks_without_stats

        with noiz_app.app_context():
            datachunks_without_stats = fetch_datachunks_without_stats()
            stats = db.session.query(DatachunkStats).all()
            datachunks = db.session.query(Datachunk).all()
        assert len(datachunks_without_stats) == 0
        assert len(datachunks) == len(stats)

    def test_calc_ppsd(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_ppsd",
                                     "-p", "1",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0

        from noiz.api.ppsd import fetch_ppsd_results

        with noiz_app.app_context():
            all_ppsd = fetch_ppsd_results(ppsd_params_id=1)
            count_res = db.session.query(PPSDResult).filter(PPSDResult.ppsd_params_id == 1).count()
            datachunks = db.session.query(Datachunk).count()

        assert len(all_ppsd) == count_res
        assert datachunks == count_res

    def test_calc_ppsd_resampled(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_ppsd",
                                     "-p", "2",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0

        from noiz.api.ppsd import fetch_ppsd_results

        with noiz_app.app_context():
            params = fetch_ppsd_params_by_id(id=2)
            all_ppsd = fetch_ppsd_results(ppsd_params_id=2)
            count_res = db.session.query(PPSDResult).filter(PPSDResult.ppsd_params_id == 2).count()
            first_res = db.session.query(PPSDResult).filter(PPSDResult.ppsd_params_id == 2).first()
            datachunks = db.session.query(Datachunk).count()

        assert len(all_ppsd) == count_res
        assert datachunks == count_res

        hand_loaded_file = np.load(first_res.file.filepath)
        loaded_file = first_res.load_data()
        assert hand_loaded_file == loaded_file

        mean_fft = loaded_file['fft_mean']
        assert len(mean_fft) == len(params.resampled_frequency_vector)

    def test_plot_average_psd(self, noiz_app, empty_workdir):
        exported_filename = "average_psd.png"
        exported_filepath = empty_workdir.joinpath(exported_filename).absolute()
        runner = CliRunner()
        result = runner.invoke(cli, ["plot", "average_psd",
                                     "-p", "1",
                                     "-sd", "2019-09-01",
                                     "-ed", "2019-11-01",
                                     "--savefig",
                                     "-pp", str(exported_filepath)])
        assert result.exit_code == 0
        assert exported_filepath.exists()

    def test_run_qcone(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_qcone",
                                     "-c", "1",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0

        with noiz_app.app_context():
            datachunk_count = Datachunk.query.count()
            qcone_count = QCOneResults.query.count()

        assert qcone_count == datachunk_count

    @pytest.mark.xfail
    def test_run_beamforming_extract_4_save_4(self, noiz_app):
        assert False

    def test_run_beamforming_extract_avg_abspower_save_avg_abspower(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_beamforming",
                                     "-p", "1",
                                     "-sd", "2019-10-02",
                                     "-ed", "2019-10-04",
                                     "--no_parallel",
                                     "--no_skip_existing",
                                     "--no_raise_errors",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            bf_result_count = BeamformingResult.query.count()
            bf_file_count = BeamformingFile.query.count()
            peak_count = BeamformingPeakAverageAbspower.query.count()

        assert 48 == bf_result_count
        assert bf_file_count == bf_result_count
        assert 272 == peak_count

    @pytest.mark.xfail
    def test_run_beamforming_multiple_configs(self, noiz_app):
        assert False

    def test_plot_beamforming_freq_slowness(self, noiz_app, empty_workdir):
        exported_filename = "beamforming_freq_slow.png"
        exported_filepath = empty_workdir.joinpath(exported_filename).absolute()
        runner = CliRunner()
        result = runner.invoke(cli, ["plot", "beamforming_freq_slowness",
                                     "-sd", "2019-09-01",
                                     "-ed", "2019-11-01",
                                     "--savefig",
                                     "-pp", str(exported_filepath)])
        assert result.exit_code == 0
        assert exported_filepath.exists()

    def test_run_datachunk_processing(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "process_datachunks",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            processed_datachunk_count = ProcessedDatachunk.query.count()
        assert 570 == processed_datachunk_count

    def test_run_crosscorrelations(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_crosscorrelations",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "-c", "ZZ",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            crosscorrelation_count = Crosscorrelation.query.count()
        assert 0 == crosscorrelation_count

        result = runner.invoke(cli, ["processing", "run_crosscorrelations",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "-c", "ZE",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            crosscorrelation_count = Crosscorrelation.query.count()
        assert 124 == crosscorrelation_count

    @pytest.mark.xfail
    def test_exporting_raw_ccfs_to_npz(self):
        assert False

    def test_run_qctwo(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_qctwo",
                                     "-c", "1",
                                     ])
        assert result.exit_code == 0

        with noiz_app.app_context():
            ccf_count = Crosscorrelation.query.count()
            qctwo_count = QCTwoResults.query.count()

        assert qctwo_count == ccf_count

    def test_run_stacking(self, noiz_app):
        runner = CliRunner()
        result = runner.invoke(cli, ["processing", "run_stacking",
                                     "-sd", "2019-09-30",
                                     "-ed", "2019-10-03",
                                     "--no_parallel",
                                     ])
        assert result.exit_code == 0
        with noiz_app.app_context():
            stack_count = CCFStack.query.count()
        assert 30 == stack_count
