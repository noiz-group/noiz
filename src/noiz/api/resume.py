# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""Resume detection for interrupted processing workflows."""

from loguru import logger
from sqlalchemy import select
from typing import List, Set
from ulid import ULID

from noiz.database import db


def detect_completed_work(
    model_class,
    candidate_ulids: List[ULID],
) -> Set[str]:
    """
    Detect which ULIDs already exist in database for resume capability.

    Query database for existing ULIDs to determine which tasks were already
    completed in a previous run. This enables resuming interrupted processing
    without duplicate work.

    :param model_class: SQLAlchemy model class to check (e.g., Datachunk, CrosscorrelationCartesian)
    :param candidate_ulids: List of ULIDs that might be processed
    :return: Set of ULID strings that already exist in database
    """
    if not candidate_ulids:
        return set()

    # Convert ULID objects to strings for query
    ulid_strings = [str(u) for u in candidate_ulids]

    logger.info(f"Checking for existing {model_class.__name__} records (resume detection)")
    logger.debug(f"Checking {len(ulid_strings)} candidate ULIDs")

    # T085: Query database for existing ULIDs
    stmt = select(model_class.ulid).where(model_class.ulid.in_(ulid_strings))
    existing_ulids = set(db.session.execute(stmt).scalars().all())

    completed_count = len(existing_ulids)
    pending_count = len(ulid_strings) - completed_count

    logger.info(f"Resume detection: {completed_count} already complete, {pending_count} remaining to process")

    return existing_ulids


def filter_pending_tasks(all_tasks, task_ulids, existing_ulids):
    """
    Filter task list to only pending (not completed) tasks.

    :param all_tasks: List of all tasks to potentially process
    :param task_ulids: Dict mapping task to its ULID
    :param existing_ulids: Set of ULIDs that already exist
    :return: List of pending tasks
    """
    # T086: Return list of pending tasks (those whose ULID doesn't exist)
    pending = [task for task in all_tasks if str(task_ulids[id(task)]) not in existing_ulids]

    logger.debug(f"Filtered to {len(pending)} pending tasks from {len(all_tasks)} total")

    return pending
