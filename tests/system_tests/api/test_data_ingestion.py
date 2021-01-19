import pytest
pytestmark = [pytest.mark.system, pytest.mark.api]

import os
from pathlib import Path
import shutil

from noiz.app import create_app
from noiz.api.component_pair import fetch_componentpairs


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

    @pytest.mark.xfail
    def test_fetch_componentpairs(self, noiz_app):
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
            assert len(fetch_componentpairs(**kwargs_mod)) == 0

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            assert len(fetch_componentpairs(**kwargs_mod)) == 9

        with noiz_app.app_context():
            kwargs_mod = kwargs.copy()
            kwargs_mod['station_codes_a'] = ("SI11", "SI05")
            kwargs_mod['only_autocorrelation'] = True
            assert len(fetch_componentpairs(**kwargs_mod)) == 9

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
