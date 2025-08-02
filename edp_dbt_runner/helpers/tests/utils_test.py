import os
from uuid import uuid4

import edp_dbt_runner.helpers.utils as utils


def test_get_package_path():
    """Test if get_package_path function is working as expected"""
    package_path = utils.get_package_path()
    is_str = isinstance(package_path, str)
    assert is_str


def test_copytree():
    """Test if copytree function is working as expected"""
    dir_src = "test_copytree_src"
    dir_dst = "test_copytree_dst"
    current_folder = os.getcwd()
    src_abs_path = os.path.join(current_folder, dir_src)
    file_name = str(uuid4())
    file_abs_path_src = os.path.join(src_abs_path, file_name)
    os.mkdir(src_abs_path)
    open(file_abs_path_src, "a").close()
    dst_abs_path = os.path.join(current_folder, dir_dst)
    utils.copytree(src_abs_path, dst_abs_path)
    file_abs_path_dst = os.path.join(dst_abs_path, file_name)
    is_file_src = os.path.exists(file_abs_path_src)
    is_file_dst = os.path.exists(file_abs_path_dst)
    if is_file_src:
        os.remove(file_abs_path_src)
        os.removedirs(src_abs_path)
    if is_file_dst:
        os.remove(file_abs_path_dst)
        os.removedirs(dst_abs_path)

    assert is_file_src and is_file_dst


def test_get_redacted_copy_of_dict():
    """Test if get_redacted_copy_of_dict function is working as expected"""
    # setup variables
    dict_with_key_to_redact = {
        "non_sensitive_data": "some_text",
        "sensitive_data": "some_sensitive_text",
    }
    keys_to_redact = ["sensitive_data"]
    redacted_value = "redacted"

    # create a copy of dictionary
    copy_of_dict_with_key_to_redact = dict_with_key_to_redact.copy()

    # run function to test with fixed redacted value
    redacted_dict = utils.get_redacted_copy_of_dict(
        dict_with_key_to_redact, keys_to_redact, replace_with=redacted_value
    )

    # assert original dict has not changed
    assert copy_of_dict_with_key_to_redact == dict_with_key_to_redact
    # assert that the returned dict is different than the original one
    assert copy_of_dict_with_key_to_redact is not redacted_dict
    # assert the sensitive key is now set to the redacted value
    assert redacted_dict.get("sensitive_data") is redacted_value
    # assert that the none-sensitive key is not changed
    assert copy_of_dict_with_key_to_redact.get("non_sensitive_data") is redacted_dict.get(
        "non_sensitive_data"
    )

    # run function to test WITHOUT fixed redacted value
    redacted_dict = utils.get_redacted_copy_of_dict(
        dict_with_key_to_redact, keys_to_redact
    )
    # assert the sensitive key is changed
    assert copy_of_dict_with_key_to_redact.get("sensitive_data") is not redacted_dict.get(
        "sensitive_data"
    )
