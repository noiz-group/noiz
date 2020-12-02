from pathlib import Path

from noiz.processing.tsindex import run_mseedindex_on_passed_dir
from flask import current_app as app


def add_seismic_data(
        basedir: Path,
        current_dir: Path,
        filename_pattern: str = "*",
):
    mseedindex_executable = app.config['MSEEDINDEX_EXECUTABLE']
    postgres_host = app.config['POSTGRES_HOST']
    postgres_user = app.config['POSTGRES_USER']
    postgres_password = app.config['POSTGRES_PASSWORD']
    postgres_db = app.config['POSTGRES_DB']

    run_mseedindex_on_passed_dir(
        basedir=basedir,
        current_dir=current_dir,
        filename_pattern=filename_pattern,
        mseedindex_executable=mseedindex_executable,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_db=postgres_db,
    )
    return
