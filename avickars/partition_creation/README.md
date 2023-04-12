# partition_creation

## Data

The data can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/EkquocwG4AdNqZIIQkBBf5YBdYNPalKS2LWsNpSRcK5-YQ?e=r3WTde

## bc_border_coordinate_processing.py

### Description
This script reads in the raw coordinates of the BC border in a geojson format and outputs them into csv tabular format

### Input
- avickars/bc_border_data/raw_data

### Output
- avickars/bc_border_data/tabular_format

### To Run
spark-submit bc_border_coordinate_transformation.py avickars/bc_border_data/raw_data avickars/bc_border_data/tabular_format


## bc_border_partition_creation.py

### Description
This script reads in the processed coordinates of the processed BC border coordinates, and partitions BC into a 10 by 10 grid of squares.

### Input
- avickars/bc_border_data/tabular_format

### Output
- avickars/bc_partitions/partitions

### To Run
- spark-submit bc_border_partition_creation.py avickars/bc_border_data/tabular_format avickars/bc_partitions/partitions

## bc_border_partition_partition_filtering.py

### Description
This script uses the coordinates of the BC border, and the coordinates of the partitions created in bc_border_partition_creation.py, and filters out the partitions that are not in BC using the ray-casting algorithm.  This script also attaches
a unique integer identifier to every partition.

### Input
- avickars/bc_border_data/tabular_format
- avickars/bc_partitions/partitions

### Output
- avickars/bc_partitions/partitions_filtered

### To Run
spark-submit bc_border_partition_partition_filtering.py avickars/bc_border_data/tabular_format avickars/bc_partitions/partitions avickars/bc_partitions/partitions_filtered


## bc_partition_coordinate_adjustments.py

### Description
This script reads in the partitions and creates a subset of border points (coordinates) between each corners for each partition.  For every point that is external to the BC border, 
the coordinate is moved to the nearest point on the border.  This allows for every square that is on the edge of the border to conform to the edges of the border.  Unfortunately, the plotting of these adjusted partitions did not
work and as a result are not used at all.

### Input
- avickars/bc_border_data/tabular_format
- avickars/bc_partitions/partitions_filtered

### Output
- avickars/bc_partitions/partitions_filtered_adjusted

### To Run
spark-submit bc_partition_coordinate_adjustments.py avickars/bc_border_data/tabular_format avickars/bc_partitions/partitions_filtered avickars/bc_partitions/partitions_filtered_adjusted

## bc_border_partition_convert_to_plottable_form.py

### Description
Converts the partition coordinates into a form that can be plotted using geopandas

### Input
- avickars/bc_partitions/partitions_filtered

### Output
- avickars/dashboard_data/partitions_plottable_form

### To Run
spark-submit bc_border_partition_convert_to_plottable_form.py avickars/bc_partitions/partitions_filtered avickars/\dashboard_data/partitions_plottable_form
