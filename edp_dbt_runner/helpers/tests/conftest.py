import os
import sys

import pytest

abs_pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(abs_pkg_path)

collect_ignore = ["setup.py"]
if sys.version_info[0] > 2:
    collect_ignore_glob = ["*_py2.py"]


@pytest.fixture(scope="session")
def test_get_package_path():
    assert True


@pytest.fixture(scope="session")
def test_copytree():
    assert True
