# -*- coding: utf-8 -*-
import arcpy
import csv
import datetime
import json
import multiprocessing
import multiprocessing.dummy
import os
import shutil
import pandas as pd
import sys

from models.etl.predict import predict


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "ETL Toolbox"
        self.alias = "etl toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [Runner]


class Runner(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Runner"
        self.description = "Full run of the workflow"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        cutoffs = arcpy.Parameter(
            displayName="Distance Cutoff Value in Miles",
            name="distance_cutoffs",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        cutoffs.value = "0.5"

        teardown_bool = arcpy.Parameter(
            displayName="Teardown Previous Data",
            name="teardown",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        teardown_bool.value = True

        extract_bool = arcpy.Parameter(
            displayName="Extract USPS Info",
            name="extract",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        extract_bool.value = True

        transform_bool = arcpy.Parameter(
            displayName="Run Transforms",
            name="transform",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        transform_bool.value = True

        predict_bool = arcpy.Parameter(
            displayName="Run Predictions",
            name="predict",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        predict_bool.value = True

        merge_bool = arcpy.Parameter(
            displayName="Merge Results",
            name="merge",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        merge_bool.value = True

        extract_dir = arcpy.Parameter(
            displayName="Extract Directory",
            name="extract_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        transform_dir = arcpy.Parameter(
            displayName="Transform Directory",
            name="transform_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        predict_dir = arcpy.Parameter(
            displayName="Predict Directory",
            name="predict_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        scores_dir = arcpy.Parameter(
            displayName="Scores Directory",
            name="scores_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        artifacts_dir = arcpy.Parameter(
            displayName="Artifacts Directory",
            name="artifacts_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        raw_dir = arcpy.Parameter(
            displayName="Raw Directory",
            name="raw_dir",
            datatype="DEFolder",
            parameterType="Derived",
            direction="Input")

        counties_path = arcpy.Parameter(
            displayName="Path to County File",
            name="counties_path",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        census_path = arcpy.Parameter(
            displayName="Path to Census File",
            name="census_path",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
        
        zipcode_path = arcpy.Parameter(
            displayName="Path to Zip Code File",
            name="zipcode_path",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")

        output_dir = arcpy.Parameter(
            displayName="Output Directory",
            name="output_dir",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        extract_dir.value = os.path.join(os.path.dirname(__file__), r"data\etl\EDDM\extract") 
        transform_dir.value = os.path.join(os.path.dirname(__file__), r"data\etl\EDDM\transform") 
        predict_dir.value = os.path.join(os.path.dirname(__file__), r"data\etl\EDDM\predict")
        scores_dir.value = os.path.join(os.path.dirname(__file__), r"data\etl\EDDM\score") 
        artifacts_dir.value = os.path.join(os.path.dirname(__file__), r"data\etl\artifacts") 
        raw_dir.value = os.path.join(os.path.dirname(__file__),  r"data\etl\raw") 
        counties_path.value = os.path.join(os.path.dirname(__file__), r"data\input\counties.json")
        census_path.value = os.path.join(os.path.dirname(__file__), r"data\input\cb_2016_06_bg_500k") 
        zipcode_path.value = os.path.join(os.path.dirname(__file__), r"data\input\DMHC_MY_2021_CA_County_and_ZIP_Code_Combinations_Abridged.csv") 
        output_dir.value = os.path.join(os.path.dirname(__file__), r'data')

        output_eddm_json = arcpy.Parameter(
            displayName="EDDM Results",
            name="output_eddm_json",
            datatype="DEFile",
            parameterType="Derived",
            direction="Output")

        output_eddm_json_sample = arcpy.Parameter(
            displayName="Sample EDDM Results",
            name="output_eddm_json",
            datatype="DEFile",
            parameterType="Derived",
            direction="Output")

        params = [cutoffs, teardown_bool, extract_bool, transform_bool,
        predict_bool, merge_bool, extract_dir, transform_dir, predict_dir,
        scores_dir, artifacts_dir, raw_dir, counties_path, census_path,
        zipcode_path, output_dir, output_eddm_json, output_eddm_json_sample]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def updateJSON(self, parameters):
        ''' Modifies the JSON config file to use the latest parameters.
        Future work could modify the existing scripts to take user parameters directly.'''

        json_config = os.path.join(os.path.dirname(__file__), 'ETL_Toolbox_Config.json')
        
        with open(json_config) as json_file:

            payload = json.load(json_file)

            payload["extract_dir"] = parameters[6].valueAsText
            payload["transform_dir"] = parameters[7].valueAsText
            payload["predict_dir"] = parameters[8].valueAsText
            payload["scores_dir"] = parameters[9].valueAsText
            payload["artifacts_dir"] = parameters[10].valueAsText
            payload["raw_dir"] = parameters[11].valueAsText

            payload["counties_json_dir"] = parameters[12].valueAsText
            payload["census_feats_dir"] = parameters[13].valueAsText
            payload["zipcode_dir"] = parameters[14].valueAsText

        with open(json_config, 'w') as f:
            json.dump(payload, f)

    def exportAsCSV(self, input_json):
        '''Uses the existing outputs from script(JSON) and converts to a formatted CSV.'''
        output_csv = input_json.replace(".json",".csv")

        with open(output_csv, 'w', newline='') as datafile:
            with open(input_json) as json_file:
                payload = json.load(json_file)

                # create the csv writer object
                csv_writer = csv.writer(datafile)

                header = ["OriginName", "zipCode", "county", "CountyZip", "population", "longitude", "latitude",
                "censusTract", "censusBlockGroup", "CreateDate"]

                csv_writer.writerow(header)

                for pts in payload:
                    for record in pts["ReprPopPoints"]["PointA"]:
                        if len(record["properties"]["population"]) > 1:
                            # Could add a for loop based on len() above
                            # write a row for each population result & assign the cutoff param to new column
                            last_index = len(record["properties"]["population"]) - 1

                            origin_name = record["properties"]["county"]+str(record["properties"]["zip"])+'_'+str(record["geometry"]["coordinates"][1])+'_'+str(record["geometry"]["coordinates"][0])
                            zipcode = record["properties"]["zip"]
                            county = record["properties"]["county"]
                            countyzip = record["properties"]["county"]+record["properties"]["zip"]
                            population = record["properties"]["population"][last_index]
                            long_y = record["geometry"]["coordinates"][0]
                            lat_x = record["geometry"]["coordinates"][1]
                            cen_tract = record['properties']['census_tract']
                            cen_blkgrp = record['properties']['census_block_group']
                            date_val = datetime.datetime.now().strftime("%x")
                            payload_write = [origin_name, zipcode, county, countyzip, population, long_y, lat_x,
                            cen_tract, cen_blkgrp, date_val]

                            csv_writer.writerow(payload_write)

                        else:
                            origin_name = record["properties"]["county"]+str(record["properties"]["zip"])+'_'+str(record["geometry"]["coordinates"][1])+'_'+str(record["geometry"]["coordinates"][0])
                            zipcode = record["properties"]["zip"]
                            county = record["properties"]["county"]
                            countyzip = record["properties"]["county"]+record["properties"]["zip"]
                            population = record["properties"]["population"][0]
                            long_y = record["geometry"]["coordinates"][0]
                            lat_x = record["geometry"]["coordinates"][1]
                            cen_tract = record['properties']['census_tract']
                            cen_blkgrp = record['properties']['census_block_group']
                            date_val = datetime.datetime.now().strftime("%x")
                            payload_write = [origin_name, zipcode, county, countyzip, population, long_y, lat_x,
                            cen_tract, cen_blkgrp, date_val]

                            csv_writer.writerow(payload_write) 
        return(output_csv)

    def deleteFiles(self, parent_path):
        with os.scandir(parent_path) as entries:
            for entry in entries:
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry.path)
                else:
                    os.remove(entry.path)
    def execute(self, parameters, messages):
        """The source code of the tool."""

        safe_thread_count = int(multiprocessing.cpu_count() / 2)

        distances = parameters[0].valueAsText
        distance_cutoff = [float(i[:i.find(',')]) if i.find(',') != -1 else float(i) for i in distances.split()]

        arcpy.AddMessage(f"Distance Cutoff:{distance_cutoff}")

        # Updates JSON config file with parameters set by user
        self.updateJSON(parameters)

        # Moved import of model tools until later in script
        # so JSON config is instantiated with user params
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))
        import models.etl.initiate as config
        import models.etl.teardown as teardown
        import models.etl.extract as extract
        import models.etl.transform as transform_pop
        import models.etl.predict as predict_pop
        import models.etl.merge as merge_pop

        # # Teardown
        if parameters[1].valueAsText.lower() == 'true':
            arcpy.AddMessage("Run the teardown...")
            teardown.remove_all_etl_data()
            arcpy.AddMessage("Teardown complete.")

        # Initiate
        arcpy.AddMessage("Create directories...")
        config.create_all_necessary_directories()
        arcpy.AddMessage("Create directories complete.")
        
        # Extract
        try:
            if parameters[2].valueAsText.lower() == 'true':
                arcpy.AddMessage(f"Run the extract using {safe_thread_count} threads...")

                if str(parameters[14].valueAsText[-4:]).lower == '.tsv':
                    zip_df = pd.read_csv(parameters[14].valueAsText, sep='\t')
                else:
                    zip_df = pd.read_csv(parameters[14].valueAsText)

                zips = [str(code) for code in zip_df['zip'].values if str(code)]

                extract.extract_concurrently(zips, safe_thread_count)

                arcpy.AddMessage("\tExtract complete.")

        except Exception as e:
            arcpy.AddMessage(e)

        # Transform
        if parameters[3].valueAsText.lower() == 'true':
            # Cleans up any leftover files
            transform_dir = parameters[7].valueAsText
            self.deleteFiles(transform_dir)

            arcpy.AddMessage("Run the transform...")
            transform_pop.transform_concurrently(transform_pop._get_remaining_zips())
            arcpy.AddMessage("\tTransform complete.")

        # Predict
        if parameters[4].valueAsText.lower() == 'true':
            # Cleans up any leftover files
            predict_dir = parameters[8].valueAsText
            self.deleteFiles(predict_dir)
            scores_dir = parameters[9].valueAsText
            self.deleteFiles(scores_dir)

            arcpy.AddMessage("Run the prediction...")    
            zip_county_pairs = predict_pop._get_remaining_service_areas()

            predict_pop.predict_concurrently(zip_county_pairs, distance_cutoff, num_processors=safe_thread_count)
            arcpy.AddMessage("\tPrediction complete.")

        # Merge
        if parameters[5].valueAsText.lower() == 'true':
            arcpy.AddMessage("Run the merge...")
            all_data = merge_pop.merge_predictions()
            all_service_areas = merge_pop.get_service_areas(all_data)

            sample_data = merge_pop.filter_data_by_county(
                all_data, valid_counties={'San Francisco', 'Marin', 'San Diego', 'Alameda'}
            )
            sample_service_areas = merge_pop.get_service_areas(sample_data)

            config.store_points(all_data, 'eddm_data.json', parameters[15].valueAsText)
            config.store_points(sample_data, 'sample_eddm_data.json', parameters[15].valueAsText)

            config.store_points(all_service_areas, 'service_areas.json', parameters[15].valueAsText)
            config.store_points(sample_service_areas, 'sample_service_areas.json', parameters[15].valueAsText)

            eddm_json_output = os.path.join(parameters[15].valueAsText, 'eddm_data.json')
            sample_eddm_json_output = os.path.join(parameters[15].valueAsText, 'sample_eddm_data.json')

            # Export the JSON files as CSV
            csv_output = self.exportAsCSV(eddm_json_output)
            csv_sample_output = self.exportAsCSV(sample_eddm_json_output)

            # Set output parameters for GP response
            parameters[16].value = csv_output
            parameters[17].value = csv_sample_output

            arcpy.AddMessage("\tMerge complete.")

        return
