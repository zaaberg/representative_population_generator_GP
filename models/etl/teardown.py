"""Methods to conclude the ETL process and remove artifacts."""
import shutil
import sys
import os
# sys.path.insert(0,"C:/code/repos/representative_population_generator/models/")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))
from models.etl import initiate


def _recursively_remove_directory(directory):
    try:
        shutil.rmtree(directory)
    except FileNotFoundError:
        pass


def remove_all_etl_data():
    """Remove all data created during the ETL process."""
    _recursively_remove_directory(initiate.ARTIFACTS_DIRECTORY)
    _recursively_remove_directory(initiate.EXTRACT_DIRECTORY)
    _recursively_remove_directory(initiate.TRANSFORM_DIRECTORY)
    _recursively_remove_directory(initiate.PREDICT_DIRECTORY)
    _recursively_remove_directory(initiate.SCORES_DIRECTORY)


if __name__ == '__main__':
    remove_all_etl_data()
