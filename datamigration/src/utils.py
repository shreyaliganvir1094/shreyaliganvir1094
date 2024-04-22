"""
Created on 17/04/2024
@author: Shreyali Ganvir

General utilities classes for dataplatform

"""

from argparse import ArgumentParser
import logging
from pathlib import Path
from dotenv import dotenv_values


class PathManager:

    """
    Class to validate paths
    """

    def __init__(self, path_string):
        self.path_object = Path(path_string)

    def _exists(self):
        """
        Check if path exists
        :return: boolean
        """
        return self.path_object.exists()

    def get_path(self):
        """
        Get path object if path exists
        :return: path object
        """
        if self._exists():
            return self.path_object
        else:
            return None


class ParseArguments:

    """
    Class to read input arguments
    """

    def __init__(self):
        self.arg_parser = ArgumentParser()
        self.arg_parser.add_argument('--e', '--env_file', type=str, help="Path to .env file")

    def parse_arguments(self):
        """
        Parse input arguments
        :return: input arguments object
        """
        args = self.arg_parser.parse_args()
        return args


class EnvConf:

    """
    Class to validate and read environment variables
    """
    def __init__(self, env_file):
        self.env_file = env_file
        self.env_vars = dotenv_values(self.env_file)
        self.required_env_vars = ['S3_BUCKET_NAME', 'ROOT_DIRECTORY']

    def validate_env(self):
        """
        Validate environment variables
        :return: boolean
        """
        if not self.env_vars:
            logging.error("No environment variables found in {}".format(self.env_file))
            return False

        # Check if all required keys are present
        missing_keys = [key for key in self.required_env_vars if key not in self.env_vars]
        if missing_keys:
            logging.error("Missing required keys in {}: {}".format(self.env_file, ",".join(missing_keys)))
            return False

        # Check if keys have valid non-empty values
        invalid_keys = [key for key, value in self.env_vars.items() if not value]
        if invalid_keys:
            logging.error("Invalid keys with empty values in {}:{}".format(self.env_file, ",".join(invalid_keys)))
            return False

        logging.info("Environment variables validated")
        return True

    def read_env(self):
        """
        Read environment variables
        :return: dict with environment variables
        """
        # check if env file passes validation checks
        if self.validate_env():
            # check if root directory is a valid path
            root_directory = PathManager(self.env_vars['ROOT_DIRECTORY']).get_path()
            if root_directory is None:
                raise FileNotFoundError("Input root directory {} does not exist".format(
                    self.env_vars['ROOT_DIRECTORY']))
            return self.env_vars
        else:
            return None
