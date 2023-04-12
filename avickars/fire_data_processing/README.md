# "fire_data_processing" Documentation

## Data

The data can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/EkquocwG4AdNqZIIQkBBf5YBdYNPalKS2LWsNpSRcK5-YQ?e=r3WTde

## forest_fire_data_processing.py

### Description
This script that reads the raw geojson files that contain the forest fire data, and converts it into two normalized tables containing the properties of the fires and the coordinates of the fires 
NOTE: We found sparks json reader to actually be insufficient in this case as it caused data type issues during processing, it is for this reason why we used pythons built in json package as it was able to correctly read the data.

### Input
- avickars/forest_fire_data/raw_data

### Output
- avickars/forest_fire_data/processed_data/coordinates & avickars/forest_fire_data/processed_data/properties

### To Run
- spark-submit forest_fire_data_processing.py avickars/avickars/forest_fire_data/raw_data/ avickars/forest_fire_data/processed_data

## forest_fire_partition_mapping.py

### Description
Script that reads in the forest fire properties and coordinates and the coordinates of each partition and for every forest fire it determines a mapping to each partition that it overlaps and also attaches the columns 
need in analysis in "forest_fire_correlation.py"

### Input
- avickars/forest_fire_data/processed_data/properties
- avickars/forest_fire_data/processed_data/coordinates
- avickars/bc_partitions/partitions_filtered

### Output
- avickars/dashboard_data/forest_fire_partition_mapping

### To Run
spark-submit forest_fire_square_mapping.py avickars/forest_fire_data/processed_data/properties avickars/forest_fire_data/processed_data/coordinates avickars/bc_partitions/partitions_filtered avickars/dashboard_data/forest_fire_partition_mapping

## forest_fire_weather_mapping.py

### Description
Script that reads in the weather data with coordinates (see weather_data_join.py under "Weather Data Processing"), and for the preceding n days of every fire, maps each day/fire to the nearest weather station that has a TMAX measurement.

### Input
- avickars/weather_data/pivoted_data_with_coordinates
- avickars/forest_fire_data/processed_data/properties
- avickars/forest_fire_data/processed_data/coordinates
- n (number of previous days to grab)

### Output
- avickars/forest_fire_data/weather_mapping/<n>

### To Run
spark-submit forest_fire_weather_mapping.py avickars/weather_data/pivoted_data_with_coordinates avickars/forest_fire_data/processed_data/properties avickars/forest_fire_data/processed_data/coordinates avickars/forest_fire_data/weather_mapping/n

## forest_fire_weather_correlation.py

### Description
Script that reads in the results of forest_fire_weather_mapping.py and computes the preceding n-day average TMAX reading for each forest fire.

### Input
- avickars/weather_data/pivoted_data_with_coordinates
- forest_fire_data/processed_data/properties
- avickars/forest_fire_data/weather_mapping/<n>

### Output
- avickars/dashboard_data/forest_fire_weather_average/<n>

### To Run
spark-submit forest_fire_weather_correlation.py avickars/weather_data/pivoted_data_with_coordinates forest_fire_data/processed_data/properties avickars/forest_fire_data/weather_mapping/<n> avickars/dashboard_data/forest_fire_weather_average/<n>
