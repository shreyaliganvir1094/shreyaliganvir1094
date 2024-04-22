"""
Created on 17/04/2024
@author: Shreyali Ganvir


AWS S3 utilities classes

"""
import os
import boto3
import logging
import concurrent.futures
from botocore.exceptions import ClientError


class FileFinder:

    """
    Class to traverse input root directory path
    """

    def __init__(self, root_directory):
        self.root_directory = root_directory
        self.files_to_copy = []

    def find_files(self):
        """
        Traverse input root directory and find all files to copy
        :return: list of file paths to copy into s3
        """
        for root, dirs, files in os.walk(self.root_directory):
            if root.endswith('airflow/src') or root.endswith('dbt'):
                if "logs" in dirs and root.endswith('dbt'):
                    dirs.remove("logs")
                if dirs:
                    self.files_to_copy.extend([os.path.join(root, each_dir, each_file) for each_dir in dirs
                                               for each_file in os.listdir(os.path.join(root, each_dir))])
                if files:
                    self.files_to_copy.extend([os.path.join(root, each_file) for each_file in files])
        return self.files_to_copy


class S3FilesUploader:

    """
    Class to upload files to s3
    """

    def __init__(self, env_vars):
        self.env_vars = env_vars
        self.s3connect = self.__get_s3_client()

    def __get_s3_client(self):
        """
        Create an S3 client
        :return: S3 client connection object
        """
        try:
            # Check if aws access key and secret key is provided in env.
            if 'AWS_ACCESS_KEY_ID' in self.env_vars and 'AWS_SECRET_ACCESS_KEY' in self.env_vars:
                self.s3connect = boto3.client('s3', aws_access_key_id=self.env_vars['AWS_ACCESS_KEY_ID'],
                                              aws_secret_access_key=self.env_vars['AWS_SECRET_ACCESS_KEY'])
            else:
                # use aws cli configured keys
                self.s3connect = boto3.client('s3')
            return self.s3connect
        except ClientError as e:
            logging.error("Error creating S3 client: {}".format(e))
            raise

    def create_bucket_if_not_exists(self):
        """
        Create bucket if its not present in s3
        :return:
        """
        try:
            if "AWS_REGION" not in self.env_vars:
                self.s3connect.create_bucket(Bucket=self.env_vars['S3_BUCKET_NAME'])
            else:
                self.s3connect.create_bucket(
                    Bucket=self.env_vars['S3_BUCKET_NAME'],
                    CreateBucketConfiguration={
                        'LocationConstraint': '{}'.format(self.env_vars['AWS_REGION'])
                    }
                )
            logging.info("Bucket {} created successfully.".format(self.env_vars['S3_BUCKET_NAME']))
        except ClientError as e:
            if e.response['Error']['Code'] in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
                logging.info("S3 bucket {} exists.".format(self.env_vars['S3_BUCKET_NAME']))
            else:
                logging.error("Error creating S3 bucket: {}".format(e))
                raise

    def upload_file(self, file_path):
        """
        Upload a file to an S3 bucket
        :param file_path:
        :return:
        """
        try:
            with open(file_path, 'rb') as f:
                # Check if file already exists in same bucket location. Display warning of replacing existing file
                if self.s3connect.head_object(Bucket=self.env_vars['S3_BUCKET_NAME'], Key=file_path):
                    logging.warning("File {} already exists in bucket {}. "
                                    "Replacing existing file".format(file_path, self.env_vars['S3_BUCKET_NAME']))
                self.s3connect.put_object(Bucket=self.env_vars['S3_BUCKET_NAME'], Key=file_path, Body=f)
                logging.info("File {} uploaded to s3 bucket {}".format(file_path, self.env_vars['S3_BUCKET_NAME']))
        except ClientError as e:
            logging.error("Error uploading file {}: {}".format(file_path, e))

    def upload_files_to_s3(self, files_to_copy):
        """
        Wrapper function to check bucket and upload files to s3 bucket
        :param files_to_copy:
        :return:
        """
        self.create_bucket_if_not_exists()
        # Use multi-threading to upload files concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.upload_file, files_to_copy)
