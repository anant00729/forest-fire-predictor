import geopandas as gpd
import pandas as pd
from apps.avickars.utils import convert_coords_to_plotable

# ******************* Reading Necessary Data *******************
# Creating the polygons that will be plotted
# Doing it here since it only needs to be done once

partitions = pd.read_parquet("apps/avickars/data/partitions_filtered_viz.parquet")
created_polygons = partitions.groupby('squareID').apply(convert_coords_to_plotable)
crs = {'init': 'epsg:4326'}
polygon = gpd.GeoDataFrame(index=created_polygons.index, crs=crs, geometry=created_polygons.values)

# Reading in the fire-risk data here
risk_data = pd.read_parquet("apps/avickars/data/model_output.parquet")
forest_assessment_data = pd.read_parquet('apps/avickars/data/forest_data_assessment.parquet').set_index('squareID')

# Reading in the fire/partition mapping data here
fire_partition_mapping = pd.read_parquet("apps/avickars/data/fire_partition_mapping.parquet")
fire_partition_mapping = fire_partition_mapping[fire_partition_mapping['FIRE_SIZE_HECTARES'] > 0]

# Reading in the weather averages

seven_day_weather_average = pd.read_parquet("apps/avickars/data/forest_fire_7_day_weather_average.parquet")
fourteen_day_weather_average = pd.read_parquet("apps/avickars/data/forest_fire_14_day_weather_average.parquet")
