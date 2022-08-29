"""Methods to create a master list of California ZIP codes."""
import os

import sys

# sys.path.insert(0,"C:/code/repos/representative_population_generator/models/")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))

from models.etl import initiate

import pandas as pd


def update_zip_list(input_filepath, output_filepath):
    """From county-level census data, generate polygons for California counties."""
    df = pd.read_csv(input_filepath, dtype={'zip': str}, encoding='latin')
    california_only = df[df['state'] == 'CA']

    try:
        os.remove(output_filepath)
    except FileNotFoundError:
        pass

    california_only.to_csv(output_filepath, sep='\t')
    return california_only


if __name__ == '__main__':
    input_filepath = os.path.join(initiate.RAW_DIRECTORY, 'zips.csv')

    update_zip_list(
        input_filepath=input_filepath,
        output_filepath='C:/code/working/DMHC_Cal_HHS/data/california_zips.tsv'
    )
