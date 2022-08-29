"""Methods to create polygons for California counties from census data."""
import os
import sys

# sys.path.insert(0,"C:/code/repos/representative_population_generator/models/")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))
import geopandas as gpd

from models.etl import initiate


def update_counties(input_filepath):
    """From county-level census data, generate polygons for California counties."""
    gdf = gpd.read_file(input_filepath)
    california_code = gdf[gdf['NAME'] == 'Los Angeles'].iloc[0]['STATEFP']
    california_only = gdf[gdf['STATEFP'] == california_code]

    # try:
    #     os.remove(output_filepath)
    # except FileNotFoundError:
    #     pass

    california_only = california_only.to_crs('epsg:4326')
    # california_only.to_file(output_filepath, driver='GeoJSON')
    return california_only


if __name__ == '__main__':
    raw_county_filepath = os.path.join(initiate.RAW_DIRECTORY, 'cb_2016_us_county_500k')

    update_counties(
        input_filepath=raw_county_filepath,
        output_filepath=initiate.COUNTIES_FILEPATH
    )
