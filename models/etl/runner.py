"""Python script to kick off the ETL process."""
import subprocess
import sys
import arcpy

def start_etl(teardown=False):
    """Start the ETL process."""
    arcpy.AddMessage("Start the ETL process.")
    steps = [
        ('Initiate', 'python C:/code/repo/representative_population_generator/models/etl/initiate.py'),
        # ('Fetch raw ETL data', 'make fetch_raw_etl_data'),  # downloads a 'raw_etl_data.zip' dir with sample data? Is AWS even utilized any longer?
        ('Update ZIP list', 'python C:/code/repo/representative_population_generator/models/etl/update_zip_list.py'),
        ('Update county polygons', 'python C:/code/repo/representative_population_generator/models/etl/update_counties.py'),
        ('Extract', 'python C:/code/repo/representative_population_generator/models/etl/extract.py'),
        ('Transform', 'python C:/code/repo/representative_population_generator/models/etl/transform.py'),
        ('Predict', 'python C:/code/repo/representative_population_generator/models/etl/predict.py'),
        ('Merge', 'python C:/code/repo/representative_population_generator/models/etl/merge.py'),
    ]

    if teardown:
        steps = [('Teardown', 'python C:/code/repo/representative_population_generator/models/etl/teardown.py')] + steps

    exit_code = 0
    for step in steps:
        subprocess.call(args='echo "{}"'.format(step[0]), shell=True)
        exit_code = subprocess.call(args=step[1], shell=True)
        if exit_code != 0:
            break


if __name__ == '__main__':
    if '--teardown' in sys.argv[1:]:
        start_etl(teardown=True)
    else:
        start_etl(teardown=False)
