"""Methods to kick off the ETL process."""
import collections
import csv
import datetime
import os
import geojson
import geopandas as gpd
import json

from shapely import speedups

speedups.enable()

json_config = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'ETL_Toolbox_Config.json')

with open(json_config) as json_file:

    payload = json.load(json_file)

    EXTRACT_DIRECTORY = payload["extract_dir"]
    TRANSFORM_DIRECTORY = payload["transform_dir"]
    PREDICT_DIRECTORY = payload["predict_dir"]
    SCORES_DIRECTORY = payload["scores_dir"]
    ARTIFACTS_DIRECTORY = payload["artifacts_dir"]
    RAW_DIRECTORY = payload["raw_dir"]

    COUNTIES_FILEPATH = payload["counties_json_dir"]
    CENSUS_FILEPATH = payload["census_feats_dir"] 
    ZIP_LIST_FILEPATH = payload["zipcode_dir"] 

ETLResult = collections.namedtuple('ETLResult', 'name, status, message')

def create_all_necessary_directories():
    """Create all required directories if they don't already exist."""
    for path in [
        EXTRACT_DIRECTORY,
        TRANSFORM_DIRECTORY,
        PREDICT_DIRECTORY,
        ARTIFACTS_DIRECTORY,
        SCORES_DIRECTORY,
        RAW_DIRECTORY,
    ]:
        os.makedirs(path, exist_ok=True)


def load_points(filename, directory, output_type):
    """
    Load GeoJSON results for a ZIP code.

    Returns either a GeoDataFrame or a GeoJSON object.
    """
    path = os.path.join(directory, filename)
    print('Loading points...')
    if output_type.lower() in ('json', 'geojson'):
        with open(path, 'r') as f:
            points = geojson.load(f)
    elif output_type.lower() in ('geopandas', 'geodataframe'):
        try:
            with open(path, 'r') as f:
                raw_points = geojson.load(f)
            points = gpd.GeoDataFrame.from_features(
                raw_points['features'],
                crs='epsg:4326'
            )
        except Exception:
            points = gpd.read_file(path, driver='GeoJSON')

    if points is None:
        raise FileNotFoundError

    return points


def store_artifacts(results, filename, verbose=False):
    """Store ETL result objects as CSV."""
    filepath = os.path.join(ARTIFACTS_DIRECTORY, filename)
    time = datetime.datetime.now()
    print('writing CSV...')
    if not verbose:
        results_to_write = [res for res in results if res.status != 'SUCCESS']
    else:
        results_to_write = results

    with open(filepath, 'a+') as f:
        header = ('name', 'status', 'message', 'timestamp')
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(
            ((name, status, message, time) for name, status, message in results_to_write)
        )


def store_points(data, filename, directory):
    """Store points as GeoJSON."""
    filepath = os.path.join(directory, filename)

    try:
        os.remove(filepath)
    except FileNotFoundError:
        pass

    if isinstance(data, gpd.GeoDataFrame):
        try:
            data.to_file(
                filename=filepath,
                driver='GeoJSON',
            )
        except ValueError:
            # If the DataFrame contains lists, serialize as JSON before writing to file.
            with open(filepath, 'w+') as f:
                geojson.dump(geojson.loads(data.to_json()), f, indent=1)
    else:
        with open(filepath, 'w+') as f:
            geojson.dump(data, f)


if __name__ == '__main__':
    create_all_necessary_directories()
