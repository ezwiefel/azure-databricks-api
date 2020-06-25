from collections import namedtuple
from random import choice
from string import ascii_letters

import pytest

from azure_databricks_api.exceptions import ResourceAlreadyExists, IoError, ResourceDoesNotExist, InvalidParameterValue
from tests.utils import create_client

client = create_client()

DBFS_TEMP_DIR = '/tmp'
SMALL_DBFS = '{temp_dir}/small.txt'.format(temp_dir=DBFS_TEMP_DIR)
LARGE_DBFS = '{temp_dir}/large.txt'.format(temp_dir=DBFS_TEMP_DIR)
DBFS_MOVED = '{temp_dir}/small-moved.txt'.format(temp_dir=DBFS_TEMP_DIR)


@pytest.fixture(scope="module")
def temp_files(tmp_path_factory):
    temp_path = tmp_path_factory.mktemp('./tmp')
    large_file_path = temp_path.with_name("large.txt")
    small_file_path = temp_path.with_name("small.txt")

    small_file_path.write_text("This is a test file used for DBFS Testing")
    large_file_path.write_text(str([choice(ascii_letters) for _ in range(1048576)]))

    FileList = namedtuple("FileList", ['small', 'large', "dir"])

    return FileList(small=small_file_path, large=large_file_path, dir=temp_path)


def test_mkdir():
    client.dbfs.mkdirs(DBFS_TEMP_DIR)
    assert DBFS_TEMP_DIR in [file.path for file in client.dbfs.list('/')]


def test_upload_file_to_dbfs(temp_files):
    client.dbfs.upload_file_by_path(file_path=temp_files.small, dbfs_path=SMALL_DBFS)
    assert SMALL_DBFS in [file.path for file in client.dbfs.list('/tmp')]


def test_upload_file_not_exists(temp_files):
    with pytest.raises(FileNotFoundError):
        client.dbfs.upload_file_by_path(file_path="THISFILESHOULDNOTEXISTSANYWHERE.txt", dbfs_path=SMALL_DBFS)


def test_upload_file_dbfs_exists(temp_files):
    with pytest.raises(ResourceAlreadyExists):
        client.dbfs.upload_file_by_path(file_path=temp_files.small, dbfs_path=SMALL_DBFS)


def test_upload_files_raises_must_be_absolute(temp_files):
    with pytest.raises(InvalidParameterValue):
        client.dbfs.upload_file_by_path(file_path=temp_files.small, dbfs_path='raiseanerror.txt', overwrite=True)


def test_download_files_raises_must_be_absolute(temp_files):
    with pytest.raises(InvalidParameterValue):
        client.dbfs.download_file(local_path="thisisanytestfile.txt", dbfs_path='raiseanerror.txt')


def test_download_file(temp_files):
    new_small_path = temp_files.dir.with_name("small_2.txt")
    client.dbfs.download_file(local_path=new_small_path, dbfs_path=SMALL_DBFS)

    assert new_small_path.read_bytes() == temp_files.small.read_bytes()


def test_download_dbfs_file_not_found(temp_files):
    with pytest.raises(ResourceDoesNotExist):
        new_large_path = temp_files.dir.with_name("large_2.txt")
        client.dbfs.download_file(dbfs_path=LARGE_DBFS, local_path=new_large_path)


def test_download_local_file_already_exists_no_overwrite(temp_files):
    new_small_path = temp_files.dir.with_name("small_2.txt")

    with pytest.raises(FileExistsError):
        client.dbfs.download_file(local_path=new_small_path, dbfs_path=SMALL_DBFS, overwrite=False)


def test_download_overwrite_local_file(temp_files):
    new_small_path = temp_files.dir.with_name("small_2.txt")
    client.dbfs.download_file(local_path=new_small_path, dbfs_path=SMALL_DBFS, overwrite=True)


def test_upload_large_file(temp_files):
    client.dbfs.upload_file_by_path(file_path=temp_files.large, dbfs_path=LARGE_DBFS)


def test_upload_existing_without_overwrite(temp_files):
    with pytest.raises(ResourceAlreadyExists):
        client.dbfs.upload_file_by_path(file_path=temp_files.small, dbfs_path=SMALL_DBFS, overwrite=False)


def test_list():
    file_list = client.dbfs.list(DBFS_TEMP_DIR)

    assert SMALL_DBFS in [file.path for file in file_list]


def test_list_not_exists():
    with pytest.raises(ResourceDoesNotExist):
        client.dbfs.list("/thisfoldershouldneverexist")


def test_get_status_is_dir():
    status = client.dbfs.get_status(DBFS_TEMP_DIR)
    assert status.is_dir


def test_get_status_is_file():
    status = client.dbfs.get_status(SMALL_DBFS)
    assert not status.is_dir


def test_get_status_resource_not_found():
    with pytest.raises(ResourceDoesNotExist):
        client.dbfs.get_status("/THISPATHSHOULDNOTEXISTANYWHERE")


def test_get_status_must_be_absolute():
    with pytest.raises(InvalidParameterValue):
        client.dbfs.get_status("THISPATHSHOULDNOTEXISTANYWHERE")


def test_move():
    client.dbfs.move(SMALL_DBFS, DBFS_MOVED)


def test_move_file_not_found():
    with pytest.raises(ResourceDoesNotExist):
        client.dbfs.move(SMALL_DBFS, DBFS_MOVED)


def test_move_already_exists():
    with pytest.raises(ResourceAlreadyExists):
        client.dbfs.move(LARGE_DBFS, DBFS_MOVED)


def test_nonrecursive_delete():
    client.dbfs.delete(SMALL_DBFS, recursive=False)


def test_nonrecursive_delete_fails():
    with pytest.raises(IoError):
        client.dbfs.delete(DBFS_TEMP_DIR, recursive=False, not_exists_ok=False)


def test_recursive_delete():
    client.dbfs.delete(DBFS_TEMP_DIR, recursive=True, not_exists_ok=False)
    assert DBFS_TEMP_DIR not in [file.path for file in client.dbfs.list('/')]
