"""
Created on 17/04/2024
@author: Shreyali Ganvir


Wrapper module dataplatform to call data migrator and utils modules

"""
import logging
from utils import EnvConf, ParseArguments
from datamigrator import FileFinder, S3FilesUploader


class DataPlatform:

    def __init__(self, env_var):
        self.env_vars = env_var

    def data_manager(self):
        """
        DataManager to traverse root directory and perform data migration activity to s3
        :return:
        """
        files_to_copy = FileFinder(self.env_vars['ROOT_DIRECTORY']).find_files()
        logging.info('Files to copy: {}'.format(",".join(files_to_copy)))
        s3manager = S3FilesUploader(self.env_vars)
        s3manager.upload_files_to_s3(files_to_copy=files_to_copy)
        return


if __name__ == '__main__':
    # Read env file
    cmd_args = ParseArguments().parse_arguments()
    env_file = cmd_args.e or ".env"
    env_conf = EnvConf(env_file=env_file)
    env_vars = env_conf.read_env()
    # env file pass validation check, proceed further
    if env_vars:
        DataPlatform(env_var=env_vars).data_manager()
