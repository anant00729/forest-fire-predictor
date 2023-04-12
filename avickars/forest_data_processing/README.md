# forest_data_processing

## Data

The data can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/EkquocwG4AdNqZIIQkBBf5YBdYNPalKS2LWsNpSRcK5-YQ?e=r3WTde

## forest_data_preprocessing.py

### Description
This script parses and paralyzes the raw geojson files into a more usable format.

### Input
- avickars/forest_data/raw_data

### Output
- avickars/forest_data/processed/coordinates & avickars/forest_data/processed/properties

### To Run
spark-submit forest_data_preprocessing.py avickars/forest_data/raw_data avickars/forest_data/processed/

## forest_data_mapping.py

### Description
This script maps each forest to a partition (sometimes multiple partitions) to be used in a granular analysis

### Input
- avickars/forest_data/processed/coordinates
- avickars/bc_partitions/partitions_filtered

### Output
- avickars/forest_data/mapping

### To Run
spark-submit forest_data_mapping.py avickars/forest_data/processed/coordinates avickars/bc_partitions/partitions_filtered avickars/forest_data/mapping

## forest_data_mapping_assessment.py

### Description
This script outputs the data that is used in the dashboard to check how many partitions of BC actually have forest information

### Input
- avickars/forest_data/mapping

### Input
- avickars/dashboard_data/forest_assessment_data

### To Run
spark-submit forest_data_mapping_assessment.py avickars/forest_data/mapping avickars/dashboard_data/forest_assessment_data



