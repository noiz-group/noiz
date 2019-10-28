from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import sqlalchemy.exc

import logging
from noiz.database import db

# from noiz.models import File

from flask.logging import logging

logger = logging.getLogger(__name__)


def search_recursively_insert_seismic_files(
    main_path: Path,
    glob_call: str = "*.*",
    filetype: str = "mseed",
    commit_freq: int = 100,
) -> None:
    """
    Recursively globs provided directory with globstring
    and adds to database absolute filepaths
    excluding ones that already exist.

    :param main_path: Directory path to search
    :type main_path: Path
    :param glob_call: Glob string used for rglob
    :type glob_call: str
    :param filetype: Assumed filetype
    :type filetype: str
    :param commit_freq: Frequency of bulk inserts to db.
    :type commit_freq: int
    :return: None
    :rtype: None
    """

    existing_filepaths = _get_existing_filepaths()

    found_files = []

    for i, path in enumerate(main_path.rglob(glob_call)):
        logger.info(f"Processing {path}")

        if path.is_dir():
            logger.info(f"{path} is directory. Skipping")
            continue

        abs_path = str(path.absolute())

        if abs_path in existing_filepaths:
            logger.info(f"Filepath {path} exists, skipping.")
            continue

        found_files.append(
            # File(
            #     add_date=datetime.now(),
            #     filepath=abs_path,
            #     filetype=filetype,
            #     processed=False,
            #     readeable=True,
            # )
        )

        if i % commit_freq == 0:
            logger.info(f"Pushing to db in bulk")
            db.session.bulk_save_objects(found_files)
            db.session.flush()
            found_files = []

    logger.info(f"Pushing rest of files to db in bulk")
    db.session.bulk_save_objects(found_files)
    db.session.commit()
    return


def insert_single_seismic_file(path: Path, filetype: str) -> None:
    """

    :param path:
    :type path: Path
    :param filetype:
    :type filetype: str
    :return: None
    :rtype: None
    """
    logger.info(f"Adding single file {path} to DB.")

    abs_path = str(path.absolute())
    try:
        db.session.add(
            # File(
            #     add_date=datetime.now(),
            #     filepath=abs_path,
            #     filetype=filetype,
            #     processed=False,
            #     readeable=True,
            # )
        )
        db.session.commit()
    except sqlalchemy.exc.IntegrityError as e:
        logger.error("Integrity error on during insert. Probably file exists.")
    return


def _get_existing_filepaths() -> List[str]:
    """
    Gets from database a column of filepaths from table File
    :return: List of absolute paths from File.filepath column
    :rtype: List[str]
    """
    pass
    # return [x[0] for x in File.query.with_entities(File.filepath).all()]


def get_not_processed_files(session):
    pass
    # return session.query(File).filter(File.processed.is_(False)).all()


def search_for_seismic_files(
    paths: Iterable[str], glob: str, filetype: str, commit_frequency: int
) -> None:
    """
    Gets an iterable of file or directory paths and inserts them into database.
    Directories are searched with recursive glob using provided glob string
    Files are added directly

    :param paths: Iterable of paths
    :type paths: Iterable[str],
    :param glob: String to use for globbing procedure in pathlib.Path.rglob
    :type glob: str
    :param filetype:
    :type filetype: str
    :param commit_frequency: Frequency of comminting to db
    :type commit_frequency: int
    :return: None
    :rtype: None
    """

    logger.info(f"Processing provided paths")
    for path in paths:
        logger.info(f"Processing filepath {path}")

        if not isinstance(path, Path) and isinstance(path, str):
            logger.info(
                f"Provided path {path} is not instance of Path. Trying to convert"
            )
            path = Path(path)

        if not path.exists():
            logger.warning(f"Provided path {path} does not exist")
            continue

        if path.is_dir():
            logger.info(f"Searching recursively in directory {path}")
            search_recursively_insert_seismic_files(
                main_path=path,
                glob_call=glob,
                filetype=filetype,
                commit_freq=commit_frequency,
            )

        if path.is_file():
            logger.info(f"Path {path} is a file")
            insert_single_seismic_file(path=path, filetype=filetype)
    return
