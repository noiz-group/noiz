from noiz.models import File
from noiz.database import db
from flask_sqlalchemy import SQLAlchemy

from pathlib import Path
from datetime import datetime

import logging
logger = logging.getLogger('processing')


def insert_seismic_files_recursively(main_path: Path,
                                     glob_call: str = '*.*',
                                     filetype: str = 'mseed',
                                     commit_freq: int = 100):

    if not isinstance(main_path, Path) and isinstance(main_path, str):
        logger.info(f'Provided path {main_path} is not instance of Path. Trying to convert')
        main_path = Path('main_path')

    if not main_path.exists():
        logger.info(f'Provided path {main_path} does not exist.')
        raise ValueError(f'Provided path {main_path} does not exist.')

    if main_path.is_file():
        logger.info(f'Provided path {main_path} is a file. Adding single file to DB.')
        db.session.add(File(
            add_date=datetime.now(),
            filepath=main_path,
            filetype=filetype,
            processed=False,
            readeable=True,
            )
        )
        return

    existing_filepaths = _get_existing_filepaths()

    found_files = []

    for i, path in enumerate(main_path.rglob(glob_call)):
        logger.info(f'Processing {path}')

        abs_path = str(path.absolute())

        if abs_path in existing_filepaths:
            logger.info(f'Filepath {path} exists, skipping.')
            continue

        found_files.append(File(
            add_date=datetime.now(),
            filepath=abs_path,
            filetype=filetype,
            processed=False,
            readeable=True,
        ))

        if i % commit_freq == 0:
            logger.info(f'Pushing to db in bulk')
            db.session.bulk_save_objects(found_files)
            db.session.flush()
            found_files = []

    logger.info(f'Pushing rest of files to db in bulk')
    db.session.bulk_save_objects(found_files)
    db.session.commit()
    return


def _get_existing_filepaths():
    return [x[0] for x in File.query.with_entities(File.filepath).all()]

