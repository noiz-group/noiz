import pytest


def pytest_collection_modifyitems(config, items):
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return

    skip_system = pytest.mark.skip(reason='system test marker is not selected')
    for item in items:
        if 'system' in item.keywords:
            item.add_marker(skip_system)
