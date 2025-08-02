import os
import sys

import pytest

abs_pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(abs_pkg_path)

collect_ignore = ["setup.py"]
if sys.version_info[0] > 2:
    collect_ignore_glob = ["*_py2.py"]


@pytest.fixture(scope="session")
def test_dbt_action_run():
    return "run"


@pytest.fixture(scope="session")
def test_dbt_action_vars():
    return '--vars {"job_id":"{{job_id}}","run_id":"{{run_id}}"}'


@pytest.fixture(scope="session")
def test_dbt_action_vars_no_dbt_vars():
    return "no_dbt_vars"


@pytest.fixture(scope="session")
def test_schemas_location():
    return {"loc_raw": "my_path"}
