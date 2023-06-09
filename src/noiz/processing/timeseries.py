# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Union, Collection, Generator, TypedDict
import subprocess
from loguru import logger
from pathlib import Path


def run_mseedindex_on_passed_dir(
        basedir: Union[Path, Collection[Path]],
        current_dir: Path,
        mseedindex_executable: str,
        postgres_host: str,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
        filename_pattern: str = "*",
        parallel: bool = True,
) -> None:
    """
    Recursively globs a provided directory in search of files that could be passed to Mseedindex and scanned for
    seismic data.

    :param basedir: Directory to rglob for files
    :type basedir: Union[Path, Collection[Path]]
    :param current_dir: Current directory for execution
    :type current_dir:  Path
    :param mseedindex_executable: Path to mseedindex executable
    :type mseedindex_executable:  str
    :param postgres_host: Address of PostgreSQL
    :type postgres_host:  str
    :param postgres_user: Database username
    :type postgres_user:  str
    :param postgres_password: Database password
    :type postgres_password:  str
    :param postgres_db: Name of database in the PostgreSQL
    :type postgres_db:  str
    :param filename_pattern: Patter to rglob with
    :type filename_pattern:  str
    :return: None
    :rtype: NoneType
    """
    # TODO change typing of mseedindex_executable to Path

    inputs_to_process = _mseedindex_input_generator(
        basedir=basedir,
        current_dir=current_dir,
        mseedindex_executable=mseedindex_executable,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_db=postgres_db,
        filename_pattern=filename_pattern,
    )

    if parallel:
        import multiprocessing
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        p.map(_call_mseedindex_to_file_wrapper, inputs_to_process)
    else:
        map(_call_mseedindex_to_file_wrapper, inputs_to_process)


class MseedIndexRunnerInputs(TypedDict):
    filepath: Path
    current_dir: Path
    mseedindex_executable: str
    postgres_host: str
    postgres_user: str
    postgres_password: str
    postgres_db: str


def _mseedindex_input_generator(
        basedir: Union[Path, Collection[Path]],
        current_dir: Path,
        mseedindex_executable: str,
        postgres_host: str,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
        filename_pattern: str = "*",
) -> Generator[MseedIndexRunnerInputs, None, None]:
    if isinstance(basedir, Path):
        filepaths = list(basedir.absolute().rglob(filename_pattern))
    elif isinstance(basedir, str):
        filepaths = list(Path(basedir).absolute().rglob(filename_pattern))
    else:
        filepaths = []
        for dirpath in basedir:
            dirpath = Path(dirpath)
            filepaths.extend(list(dirpath.absolute().rglob(filename_pattern)))

    for filepath in filepaths:
        if not filepath.is_file():
            continue
        yield MseedIndexRunnerInputs(
            filepath=filepath,
            current_dir=current_dir,
            mseedindex_executable=mseedindex_executable,
            postgres_host=postgres_host,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_db=postgres_db,
        )


def _call_mseedindex_to_file_wrapper(
        inputs: MseedIndexRunnerInputs,
):
    _call_mseedindex_to_file(
        filepath=inputs["filepath"],
        current_dir=inputs["current_dir"],
        mseedindex_executable=inputs["mseedindex_executable"],
        postgres_host=inputs["postgres_host"],
        postgres_user=inputs["postgres_user"],
        postgres_password=inputs["postgres_password"],
        postgres_db=inputs["postgres_db"],
    )


def _call_mseedindex_to_file(
        filepath: Path,
        current_dir: Path,
        mseedindex_executable: str,
        postgres_host: str,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
):
    """
    Runs mseedindex with set of provided parameters on a provided file.

    :param filepath: Filepath to a file that will be passed for scan
    :type filepath: Path
    :param current_dir: Current directory for execution
    :type current_dir:  Path
    :param mseedindex_executable: Path to mseedindex executable
    :type mseedindex_executable:  str
    :param postgres_host: Address of PostgreSQL
    :type postgres_host:  str
    :param postgres_user: Database username
    :type postgres_user:  str
    :param postgres_password: Database password
    :type postgres_password:  str
    :param postgres_db: Name of database in the PostgreSQL
    :type postgres_db:  str
    :return: None
    :rtype: NoneType
    """
    try:
        cmd = [mseedindex_executable]
        cmd.extend(["-pghost", postgres_host])
        cmd.extend(["-dbuser", postgres_user])
        cmd.extend(["-dbpass", postgres_password])
        cmd.extend(["-dbname", postgres_db])
        cmd.append("-v")
        cmd.append(str(filepath))
        # boolean options have a value of None
        cmd = [c for c in cmd if c is not None]
        logger.debug(f"Running mseedindex command for file {filepath}")
        proc = subprocess.Popen(cmd,
                                cwd=str(current_dir.absolute()),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                )
        out, err = proc.communicate()

        for stdout_line in out.splitlines():
            logger.info(f"mseedindex STDOUT: {stdout_line.strip().decode()}")
        return (mseedindex_executable, proc.returncode,
                out.strip(), err.strip())
    except Exception as err:
        raise OSError(f"Error running command `{mseedindex_executable}` - {err}")
