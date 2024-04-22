"""
Tests for `utils` module.
"""

import unittest
from unittest.mock import patch
from src.utils import PathManager, ParseArguments, EnvConf


class TestPathManager(unittest.TestCase):
    def test_get_path(self):
        path_manager = PathManager('my-data-product')
        self.assertIsNotNone(path_manager.get_path())


class TestParseArguments(unittest.TestCase):
    def setUp(self):
        self.parser = ParseArguments()

    @patch('sys.argv', ['dataplatform.py', '--e', '.env'])
    def test_parse_arguments(self):
        args = self.parser.parse_arguments()
        self.assertEqual(args.e, '.env')

    @patch('sys.argv', ['dataplatform.py'])
    def test_parse_arguments_no_args(self):
        args = self.parser.parse_arguments()
        self.assertIsNone(args.e)


class TestEnvConf(unittest.TestCase):
    def setUp(self):
        self.env = ".env"
        self.env_conf = EnvConf(self.env)

    def test_validate_env_no_env_vars(self):
        self.env_conf.env_vars = {}
        result = self.env_conf.validate_env()
        self.assertFalse(result)

    def test_validate_env_missing_keys(self):
        self.env_conf.env_vars = {'S3_BUCKET_NAME': 'kiwibank-data-platform'}
        result = self.env_conf.validate_env()
        self.assertFalse(result)

    def test_validate_env_invalid_keys(self):
        self.env_conf.env_vars = {'S3_BUCKET_NAME': ''}
        result = self.env_conf.validate_env()
        self.assertFalse(result)

    def test_validate_invalid_env_path(self):
        env = "../.env"
        env_conf = EnvConf(env)
        result = env_conf.validate_env()
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
