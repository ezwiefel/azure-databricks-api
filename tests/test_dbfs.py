import os
import shutil
import tempfile
import uuid
from random import choice
from string import ascii_letters

from azure_databricks_api.exceptions import InvalidParameterValue, ResourceDoesNotExist, ResourceAlreadyExists
from tests.base_class import AzureDatabricksTests


class TestDbfsAPI(AzureDatabricksTests):
    """
    add_block, close, create, and delete functions are not explicitly tested here - these functions are tested through
    other calls

    """
    def setUp(self) -> None:
        # Create a temp directory and temp file on the local machine to test upload and download functionality
        self.local_temp_dir = tempfile.mkdtemp()
        self.local_temp_file = os.path.join(self.local_temp_dir, 'test.txt')
        self.local_large_temp_file = os.path.join(self.local_temp_dir, 'test_large.txt')
        self.local_existing_file = os.path.join(self.local_temp_dir, "existing_test.txt")

        self.dbfs_existing_file = '/existing_test.txt'

        # Create a temp file to test upload
        self.create_temp_file(self.local_temp_file, "This is a test file used for DBFS Testing")

        # Create large file to test - must be greater than 1 MB
        self.create_temp_file(self.local_large_temp_file, contents=[choice(ascii_letters) for _ in range(1048576)])

        # Create a pre-existing local file
        self.create_temp_file(self.local_existing_file, contents="This is a test file used for DBFS Testing")

    @staticmethod
    def create_temp_file(path: str, contents) -> None:
        with open(path, 'w+') as fo:
            fo.write(str(contents))

    def tearDown(self) -> None:
        shutil.rmtree(self.local_temp_dir, True)

    def test_get_status_is_file(self):
        dbfs_path_1 = self.generate_file_path('dbfs')
        self.client.dbfs.upload_file_by_path(self.local_temp_file, dbfs_path_1)
        self.assertFalse(self.client.dbfs.get_status(dbfs_path_1).is_dir)

    def test_get_status_resouce_does_not_exist(self):
        dbfs_path_1 = self.generate_file_path('dbfs')
        with self.assertRaises(ResourceDoesNotExist):
            self.client.dbfs.get_status(dbfs_path_1)

    def test_list(self):
        self.assertIn('/existing_test.txt', [x.path for x in self.client.dbfs.list('/')])

    def test_mkdirs_nested(self):
        dbfs_parent = self.generate_file_path('dbfs')
        dbfs_child = dbfs_parent + '/' + self.generate_file_path('dbfs')

        self.client.dbfs.mkdirs(dbfs_child)
        self.assertTrue(self.client.dbfs.get_status(dbfs_child).is_dir)

    def test_move(self):
        dbfs_path_1 = self.generate_file_path('dbfs')
        dbfs_path_2 = self.generate_file_path('dbfs')

        self.client.dbfs.upload_file_by_path(file_path=self.local_temp_file, dbfs_path=dbfs_path_1)

        self.client.dbfs.move(dbfs_path_1, dbfs_path_2)

        self.client.dbfs.delete(dbfs_path_1, not_exists_ok=True)
        self.client.dbfs.delete(dbfs_path_2, not_exists_ok=True)

    def test_move_file_not_found(self):
        dbfs_path_1 = self.generate_file_path('dbfs')
        dbfs_path_2 = self.generate_file_path('dbfs')

        self.client.dbfs.upload_file_by_path(file_path=self.local_temp_file, dbfs_path=dbfs_path_1)

        with self.assertRaises(ResourceDoesNotExist):
            self.client.dbfs.move(dbfs_path_2, dbfs_path_1)

        self.client.dbfs.delete(dbfs_path_1, not_exists_ok=True)
        self.client.dbfs.delete(dbfs_path_2, not_exists_ok=True)

    def test_move_already_exists(self):
        dbfs_path_1 = self.generate_file_path('dbfs')

        self.client.dbfs.upload_file_by_path(file_path=self.local_temp_file, dbfs_path=dbfs_path_1)

        with self.assertRaises(ResourceAlreadyExists):
            self.client.dbfs.move(dbfs_path_1, dbfs_path_1)

        self.client.dbfs.delete(dbfs_path_1, not_exists_ok=True)

    def test_download_overwrite(self):
        self.client.dbfs.download_file(self.local_existing_file, dbfs_path=self.dbfs_existing_file, overwrite=True)

    def test_download_files_raises_already_exists_for_local(self):
        with self.assertRaises(FileExistsError):
            self.client.dbfs.download_file(local_path=self.local_existing_file,
                                           dbfs_path=self.dbfs_existing_file)

    def test_download_files_raises_must_be_absolute(self):
        with self.assertRaises(InvalidParameterValue):
            self.client.dbfs.download_file(local_path=os.path.join(self.local_temp_dir, 'shouldntcreate.txt'),
                                           dbfs_path='thisdoesntexist.txt')

    def test_download_file_not_found(self):
        with self.assertRaises(ResourceDoesNotExist):
            self.client.dbfs.download_file(local_path=os.path.join(self.local_temp_dir, 'shouldntcreate.txt'),
                                           dbfs_path='/thisdoesntexist.txt')

    def test_upload_file_by_path_and_download_file(self):
        """ Test both upload and download file functionality"""
        self.upload_download_compare(local_path=self.local_temp_file)

    def test_upload_download_large_file_by_path(self):
        """Upload file > 1 MB"""
        self.upload_download_compare(local_path=self.local_large_temp_file)

    def test_upload_overwrite(self):
        self.client.dbfs.upload_file_by_path(file_path=self.local_temp_file, dbfs_path=self.dbfs_existing_file,
                                             overwrite=True)

    def upload_download_compare(self, local_path):
        dbfs_path = self.generate_file_path('dbfs')
        downloaded_file_path = self.generate_file_path('local')

        # Upload file then download it to a different filename
        self.client.dbfs.upload_file_by_path(file_path=local_path, dbfs_path=dbfs_path)
        self.client.dbfs.download_file(local_path=downloaded_file_path, dbfs_path=dbfs_path)

        self.compare_two_files(local_path, downloaded_file_path)

        # Clean up file in dbfs (local temp folder deleted in tearDown
        self.client.dbfs.delete(dbfs_path)

    def compare_two_files(self, path1, path2):
        with open(path1, 'r') as orig_file:
            with open(path2, 'r') as dl_file:
                self.assertEqual(dl_file.read(), orig_file.read(),
                                 msg="The downloaded file did not match the uploaded file.")

    def generate_file_path(self, path_type: str = 'dbfs') -> str:
        if path_type.lower() == 'local':
            return os.path.join(self.local_temp_dir, str(uuid.uuid4()))
        elif path_type.lower() == 'dbfs':
            return "/tests/{0}".format(str(uuid.uuid4()))
        else:
            raise NotImplementedError("Only local or DBFS paths can be created")
