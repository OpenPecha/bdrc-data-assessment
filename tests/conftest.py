import os
from dotenv import load_dotenv


def pytest_configure(config):

    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.test_env')
    load_dotenv(dotenv_path)
