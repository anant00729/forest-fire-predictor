# "Weather Data Processing" Documentation

## Data

The data can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/EkquocwG4AdNqZIIQkBBf5YBdYNPalKS2LWsNpSRcK5-YQ?e=r3WTde

## weather_data_download_automation.py.py

### Description
This python script automates the processing of downloading the historical BC weather data from the "Global Historical Climatology Network" using selenium.  It does this by reading in the master list of weather stations contained in
ghcnd-stations.txt, and downloads the historical data for all BC weather stations.  

Note: The "chromedrive.exe" executable that is used by selenium works for Chrome version 95.  If you have a different version of Chrome, you MUST download the chrome driver for that version.
Works on Chrome version 95

### Input
None, the location of "ghcnd-stations.txt" is hard code into the scrypt.

### Output
A separate file for the historical weather data for each BC weather station respectively.  The location of the output will be pre-configured download location set for Chrome.  However, the raw data is located in 
avickars/weather_data/raw_data

### To Run
python3 weather_data_download_automation.py.py

## weather_data_pivot.py

### Description
This script reads in the raw weather Data and pivots it so that each feature has its own column.  It also cleans the data by removing any observations that have a quality issue as well.

### Input
- Input: "avickars/weather_data/raw_data"

### Output
- Output: "avickars/weather_data/pivoted_data"

### To Run
- To Run: spark-submit weather_data_pivot.py "Forest Fire Data Sets/Weather Data/Raw Data" "Forest Fire Data Sets/Weather Data/Pivoted Data"

## weather_data_join.py

### Description
This script reads the information for every weather station, and the pivoted weather data to attach the coordinates for each weather station to each weather observation (per station)

### Input
- ghcnd-stations.txt (NOTE: This file is hard coded in the script)
- avickars/weather_data/pivoted_data

### Output
- avickars/weather_data/pivoted_data_with_coordinates

### To Run
spark-submit "Forest Fire Data Sets/Weather Data/ghcnd-stations.txt" avickars\weather_data\pivoted_data avickars\weather_data\pivoted_data_with_coordinates

## bc_weather_mapping.py

### Description
This script uses the partitions of BC (avickars/bc_partitions/partitions_filtered), and the weather data with coordinates ("avickars/weather_data/pivoted_data_with_coordinates") and constructs a mapping such that 
for every date, each partition is mapped to the weather station that is closest to its center.

### Input
- avickars/weather_data/pivoted_data_with_coordinates
- avickars/bc_partitions/partitions_filtered

### Output
- avickars/weather_data/pivoted_data_mapping

### To Run
spark-submit bc_weather_mapping.py avickars/weather_data/pivoted_data_with_coordinates avickars/bc_partitions/partitions_filtered avickars/weather_data/pivoted_data_mapping