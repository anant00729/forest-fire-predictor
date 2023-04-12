# "ML Model" Documentation

## Data

The data can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/EkquocwG4AdNqZIIQkBBf5YBdYNPalKS2LWsNpSRcK5-YQ?e=r3WTde

## create_model_input.py

### Description

This script creates the input that is used in the model

### Input
- avickars/weather_data/pivoted_data_mapping
- avickars/weather_data/pivoted_data_with_coordinates
- avickars/forest_fire_data/processed_data/properties
- avickars/forest_fire_data/partition_mapping

### Output
- avickars/model/input

### To Run
spark-submit create_model_input.py avickars/weather_data/pivoted_data_mapping avickars/weather_data/pivoted_data_with_coordinates avickars/forest_fire_data/processed_data/properties avickars/forest_fire_data/partition_mapping

## modelling.py

### Description
This script creates the model input and pipeline, saves the pipeline and creates the model output that is used in the vizualization

### Input
- avickars/model/input

### Output
- avickars/model/viz_output
- avickars/model/model_pipeline

### To Run
spark-submit modelling.py avickars/model/input avickars/model/viz_output avickars/model/model_pipeline