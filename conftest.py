# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest

from dataclasses import dataclass


@dataclass
class TestsWithMarkSkipper:
    ''' Util to skip tests with mark, unless cli option provided. '''

    test_mark: str
    cli_option_name: str
    cli_option_help: str


cli_skipper = TestsWithMarkSkipper(
    test_mark='cli',
    cli_option_name="--runcli",
    cli_option_help="run cli system tests",
)
api_skipper = TestsWithMarkSkipper(
    test_mark='api',
    cli_option_name="--runapi",
    cli_option_help="run api system tests",
)

MARKER_SKIPPERS = (
    cli_skipper,
    api_skipper,
)


def _skip_items_with_mark(marker_skipper: TestsWithMarkSkipper, items):
    reason = "need {} option to run".format(marker_skipper.cli_option_name)
    skip_marker = pytest.mark.skip(reason=reason)
    for item in items:
        if marker_skipper.test_mark in item.keywords:
            item.add_marker(skip_marker)


def pytest_addoption(parser):
    for marker_skipper in MARKER_SKIPPERS:
        parser.addoption(
            marker_skipper.cli_option_name,
            action="store_true",
            default=False,
            help=marker_skipper.cli_option_help,
        )


def pytest_collection_modifyitems(config, items):
    for marker_skipper in MARKER_SKIPPERS:
        if not config.getoption(marker_skipper.cli_option_name):
            _skip_items_with_mark(marker_skipper, items)
