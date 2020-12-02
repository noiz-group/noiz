import subprocess
from tqdm import tqdm

from pathlib import Path


def run_mseedindex_on_passed_dir(
        basedir: Path,
        current_dir: Path,
        mseedindex_executable: str,
        postgres_host: str,
        postgres_user: str,
        postgres_password: str,
        postgres_db: str,
        filename_pattern: str = "*",
):
    filepaths = basedir.rglob(filename_pattern)

    for filepath in filepaths:
        _call_mseedindex_to_file(
            filepath=filepath,
            current_dir=current_dir,
            mseedindex_executable=mseedindex_executable,
            postgres_host=postgres_host,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_db=postgres_db,
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
    try:
        cmd = [mseedindex_executable]
        cmd.extend(["-pghost", postgres_host])
        cmd.extend(["-dbuser", postgres_user])
        cmd.extend(["-dbpass", postgres_password])
        cmd.extend(["-dbname", postgres_db])
        cmd.append(str(filepath))
        # boolean options have a value of None
        cmd = [c for c in cmd if c is not None]
        proc = subprocess.Popen(cmd,
                                cwd=str(current_dir.absolute()),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        return (mseedindex_executable, proc.returncode,
                out.strip(), err.strip())
    except Exception as err:
        raise OSError(f"Error running command `{mseedindex_executable}` - {err}")
