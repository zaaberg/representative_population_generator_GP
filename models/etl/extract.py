"""This module scrapes data from the USPS EDDM website and stores it in a usable format."""
import multiprocessing.dummy
import os
import sys
import json
# sys.path.insert(0,"C:/code/repos/representative_population_generator/models/")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))
from models.etl import initiate

import pandas as pd

import requests

from tenacity import *

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

pd.options.mode.chained_assignment = None

BASE_URL = (
    'https://gis.usps.com/arcgis/rest/'
    'services/EDDM/selectZIP/GPServer/'
    'routes/execute?f=json&env%3AoutSR={spatial_reference}&ZIP={zip_code}'
    '&Rte_Box={route_or_box}&UserName=EDDM'
)

@retry(reraise=True, stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=20)) # wait_fixed=2000)
def request_item(zip_code, only_return_po_boxes=False, spatial_reference='4326'):
    """
    Request data for a single ZIP code, either routes or PO boxes.

    Note that the spatial reference '4326' returns latitudes and longitudes of results.
    """
    url = BASE_URL.format(
        zip_code=str(zip_code),
        spatial_reference=str(spatial_reference),
        route_or_box='B' if only_return_po_boxes else 'R'
    )
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    return response.json()


def request_zip(zip_code, spatial_reference='4326'):
    """Request data for a single ZIP code."""

    print('...Requesting ZIP {}\n'.format(str(zip_code)))
    
    route_results=''
    try:
        route_results = request_item(
            zip_code, spatial_reference=spatial_reference, only_return_po_boxes=False
        )        
        if route_results['results'][0]['value']['exceededTransferLimit'] == True or str(route_results['results'][0]['value']['exceededTransferLimit']).lower == 'true':
            print(f"{zip_code}: Route exceeds Transfer Limit")
        # print(f"Route Exceed Transfer Limit: {route_results['results'][0]['value']['exceededTransferLimit']}")
    except Exception as e:
        print(f"{zip_code} Route Error: {str(e)}")

    po_boxes_results = ''
    
    try:
        po_boxes_results = request_item(
            zip_code, spatial_reference=spatial_reference, only_return_po_boxes=True
        )
        if po_boxes_results['results'][0]['value']['exceededTransferLimit'] == True or str(po_boxes_results['results'][0]['value']['exceededTransferLimit']).lower == 'true':
            print(f"{zip_code}: PO Box exceeds Transfer Limit")

    except Exception as e:
        print(f"{zip_code} PO Box Error: {str(e)}")

    json_results = ''

    if route_results == '' and po_boxes_results != '':
        try:
            json_results = (
                po_boxes_results['results'][0]['value']['features']
            ) 
        except Exception as e:
            print(f"{zip_code}: JSON Join Error: {str(e)}")

    elif po_boxes_results == '' and route_results != '':
        try:
            json_results = route_results['results'][0]['value']['features'] 

        except Exception as e:
            print(f"{zip_code}: JSON Join Error: {str(e)}")

    elif po_boxes_results == '' and route_results == '':
        try:
            json_results = ''
        except Exception as e:
            print(f"{zip_code}: JSON Join Error: {str(e)}")

    else:
        try:
            json_results = route_results['results'][0]['value']['features'] + (
                po_boxes_results['results'][0]['value']['features']
            ) 
        except Exception as e:
            print(f"{zip_code}: JSON Join Error: {str(e)}")
    
    return json_results

def extract(zip_code):
    """
    Request data from USPS for a single ZIP.

    Return problematic ZIP codes.
    """
    filename = '{}.json'.format(str(zip_code))
    try:
        json_data = request_zip(str(zip_code))
        if not json_data:
            print('No points found for ZIP {} in EDDM!'.format(str(zip_code)))
            status = 'WARNING'
            message = 'No points found in EDDM!'
        else:
            print('ZIP {} completed succesfully!'.format(str(zip_code)))
            initiate.store_points(
                data=json_data,
                filename=filename,
                directory=EXTRACT_DIRECTORY
            )
            status = 'SUCCESS'
            message = ''
    except Exception as ex:
        print('Something went wrong when requesting ZIP code {}! \n {}'.format(str(zip_code), str(ex)))
        status = 'FAILURE'
        message = type(ex).__name__ + ' ' + str(ex)

    return initiate.ETLResult(
        name=zip_code,
        status=status,
        message=message,
    )


def extract_concurrently(zips, threads=1):
    """
    Previously, extracted EDDM data for all provided ZIPs concurrently.
    Currently, extracts individually due to transfer limits...

    Ignores existing ZIPs in the EXTRACT_DIRECTORY.
    """
    
    processed_zips = {f[:5] for f in os.listdir(EXTRACT_DIRECTORY) if '.json' in f}
    remaining_zips = [code for code in zips if code not in processed_zips]

    pool = multiprocessing.dummy.Pool(threads)
    results = pool.map(extract, remaining_zips)
    initiate.store_artifacts(results, 'extract_results.csv', verbose=False)


if __name__ == '__main__':

    if str(ZIP_LIST_FILEPATH[-4:]).lower == '.tsv':
        zip_df = pd.read_csv(ZIP_LIST_FILEPATH, sep='\t')
    else:
        zip_df = pd.read_csv(ZIP_LIST_FILEPATH)

    zips = [str(code) for code in zip_df['zip'].values if str(code)]

    # Get the theoretical safe number of threads to run
    safe_thread_count = int(multiprocessing.cpu_count() / 2)
    print(safe_thread_count)

    extract_concurrently(zips, safe_thread_count)
