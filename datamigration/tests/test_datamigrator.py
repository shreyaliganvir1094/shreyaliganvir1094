"""
Tests for `datamigrator` module.
"""

import unittest
from unittest.mock import patch, MagicMock
from src import datamigrator as dm
from botocore.exceptions import ClientError


class TestFileFinder(unittest.TestCase):

    def setUp(self):
        self.root_directory = "my-data-product"
        self.expected_list_of_files = ['my-data-product/dbt/models/my_model.sql',
                                       'my-data-product/dbt/packages.yml',
                                       'my-data-product/dbt/dbt_project.yml',
                                       'my-data-product/airflow/src/dags/my_dag.py',
                                       'my-data-product/airflow/src/tasks/my_task.py']
        self.file_finder_instance = dm.FileFinder(self.root_directory)

    # Test to check correct list of files return
    def test_find_files(self):
        outputfile = self.file_finder_instance.find_files()
        self.assertEqual(outputfile, self.expected_list_of_files)


class TestS3FilesUploader(unittest.TestCase):

    def setUp(self):
        self.env_vars = {
            'AWS_ACCESS_KEY_ID': '**************',
            'AWS_SECRET_ACCESS_KEY': '**************',
            'S3_BUCKET_NAME': 'kiwibank-data-platform',
            'AWS_REGION': 'ap-southeast-2'
        }
        self.files_to_copy = ['my-data-product/dbt/models/my_model.sql',
                              'my-data-product/dbt/packages.yml',
                              'my-data-product/dbt/dbt_project.yml',
                              'my-data-product/airflow/src/dags/my_dag.py',
                              'my-data-product/airflow/src/tasks/my_task.py']
        # Mock boto3.client() to avoid actual AWS calls during testing
        self.s3_client = MagicMock()
        self.s3_uploader = dm.S3FilesUploader(self.env_vars)
        self.s3_uploader.s3connect = self.s3_client
        self.file_path = 'my-data-product/dbt/models/my_model.sql'

    def test_0_get_s3_client(self):
        # Test to check if an S3 client is created correctly
        self.assertIsNotNone(self.s3_uploader.s3connect)

    def test_1_create_bucket_if_not_exists(self):
        # Mock successful bucket creation
        self.s3_client.create_bucket.return_value = None
        with patch('logging.info') as mock_logging_info:
            self.s3_uploader.create_bucket_if_not_exists()
            mock_logging_info.assert_called_once_with("Bucket {} created successfully.".format(
                self.env_vars['S3_BUCKET_NAME']))

    @patch('logging.error')
    def test_2_create_bucket_already_exists(self, mock_logging_error):
        # Mock bucket already exists or bucket already owned error
        error_codes = ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]
        for error_code in error_codes:
            self.s3_client.create_bucket.side_effect = (
                ClientError({'Error': {'Code': '{}'.format(error_code)}}, 'create_bucket'))
            with patch('logging.info') as mock_logging_info:
                self.s3_uploader.create_bucket_if_not_exists()
                mock_logging_info.assert_called_once_with("S3 bucket {} exists.".format(
                    self.env_vars['S3_BUCKET_NAME']))
            mock_logging_error.assert_not_called()  # Ensure error logging is not called

    def test_3_upload_file_success(self):
        # Mock successful file upload
        with patch('builtins.open', unittest.mock.mock_open(read_data=b'file content')) as mock_file:
            self.s3_uploader.s3connect.head_object.return_value = None  # File doesn't exist in bucket
            self.s3_uploader.upload_file(self.file_path)
            mock_file.assert_called_once_with(self.file_path, 'rb')
            self.s3_uploader.s3connect.head_object.assert_called_once_with(Bucket='{}'.format(
                self.env_vars['S3_BUCKET_NAME']), Key=self.file_path)
            self.s3_uploader.s3connect.put_object.assert_called_once_with(Bucket='{}'.format(
                self.env_vars['S3_BUCKET_NAME']), Key=self.file_path, Body=mock_file.return_value)
            self.assertTrue(self.s3_uploader.s3connect.put_object.called)

    def test_4_upload_file_replace_existing(self):
        # Mock replacing of existing file into s3 bucket
        with patch('builtins.open', unittest.mock.mock_open(read_data=b'file content')) as mock_file:
            self.s3_uploader.s3connect.head_object.return_value = {
                'File_Path': {'key': 'file_path'}}  # File already exists in bucket

            with self.assertLogs(level='WARNING') as cm:
                self.s3_uploader.upload_file(self.file_path)
                mock_file.assert_called_once_with(self.file_path, 'rb')
                self.s3_uploader.s3connect.head_object.assert_called_once_with(Bucket='{}'.format(
                    self.env_vars['S3_BUCKET_NAME']), Key=self.file_path)
                self.s3_uploader.s3connect.put_object.assert_called_once_with(Bucket='{}'.format(
                    self.env_vars['S3_BUCKET_NAME']), Key=self.file_path, Body=mock_file.return_value)
                self.assertTrue(self.s3_uploader.s3connect.put_object.called)
                self.assertIn("Replacing existing file", cm.output[0])  # Check if warning message is logged

    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_5_upload_files_to_s3(self, mock_executor):
        # Mock the ThreadPoolExecutor
        mock_executor.return_value.__enter__.return_value.map = self.s3_client
        # Call the method to be tested
        self.s3_uploader.upload_files_to_s3(self.files_to_copy)
        # Assert that executor.map was called once with the upload_file method and files_to_copy list
        self.s3_client.assert_called_once_with(self.s3_uploader.upload_file, self.files_to_copy)


if __name__ == '__main__':
    unittest.main()
