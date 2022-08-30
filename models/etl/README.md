## Updating Data

### Overview

The ETL consists of four main stages:

1. Extraction of the data from EDDM for each ZIP code
2. Transformation of the data into a form usable by the algorithm and assignment of points to counties
3. Predicting / selecting the representative population points
4. Merging the data for all service areas into a single file

Each stage builds on the previous one and runs only on new files. For example, this means that if a single new ZIP code is detected in the output directory of the `extract` step, re-running the `transform` step will transform only that single file. This is designed for robustness: if the ETL process is interrupted, it can easily be restarted without losing progress.

### Set-up
See the [Contribute/Set-up Readme](https://github.com/zaaberg/representative_population_generator_GP/blob/master/CONTRIBUTE.md)

### Running

- [Open the custom GP tool, 'ETL_Toolbox.pyt', in Pro](https://pro.arcgis.com/en/pro-app/latest/help/analysis/geoprocessing/basics/use-a-custom-geoprocessing-tool.htm)
- Parameters will populate based on script location. User can modify inputs like Distance Cutoff, Zipcodes, Census, Counties, and output directory.
    - *Distance Cutoff Value in Miles* - Single value distance cutoff for generating Population estimates. Default value of 0.5 miles.
    - Check boxes - The following radio buttons indicate whether the respective step will be run.
        - *Teardown* - Cleans out the directories from a previous run of the tool. Uncheck, if you're looking to skip the extract process.
        - *Extract* - Extracts USPS information based on provided Zip codes.
        - *Transforms* - Transforms the extracted info into a useable format for the rest of the script.
        - *Predict* - Runs the population prediction models based on Zip code.
        - *Merge* - Merge the individual results into the final output and subsequently, create output CSV.
    - *Path to County File* - Path to the JSON file containing County information.
    - *Path to Census File* - Path to folder containing the Census information.
    - *Path to Zip Code File* - Path to CSV file that holds the Zip Code information.
    - *Output Directory* - Path to directory that will house the output JSON and CSV files.


### Output Files

All output files are created in the `data` directory.

1. `eddm_data.json`: all representative population points for all service areas for which EDDM points could be found
2. `service_areas.json`: a complete list of service areas contained in the above dataset
3. `eddm_data.csv`: all representative population points for all service areas for which EDDM points could be found formatted into a CSV


### Prerequisites

1. A delimited file listing the ZIP codes to extract from EDDM
2. A GeoJSON file with county polygons
3. A GeoJSON file with census tract, block group, or block information

[TIGER shapefiles](https://www.census.gov/geo/maps-data/data/tiger-line.html) are a natural source for the data in items 2 and 3. Both should be restricted to the region of interest (e.g., the state of California) beforehand.


### Artifacts
Each stage of the ETL logs failures in the `data/etl/artifacts/` directory along with the corresponding error messages. These messages are intended to help users ascertain why certain service areas do not appear in the ETL output (for example, "No points found for ZIP 99999 in EDDM!").

