# asa404

## Description

This folder contains all ETL done by Anant S Awasthy

# To run the scripts for each file 

## Cleaning the coordinates of forrest stations
spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 cleaning_coordinates_forrest_stattions.py fs-o-1 reduced-fs-o-1

## Cleaning the regional BC data cleaning
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --files /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/secure-connect-fireforrestdb.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-fireforrestdb.zip --conf spark.cassandra.auth.username=bjdCIOofvrpDSwAEKesZPiYA --conf spark.cassandra.auth.password=-9uE-jqWTYoSewAb0QS-vZtb9EgGMn3vl5_a+T8hDmpqRg8yH_zu12z-LkaZ1WNUGSS49dAEH6pb5SBuZcpUCEYRhi5dyobRn29C9lOxf5fd9pL.vleHquZZXgzIIiNb --deploy-mode client --driver-memory 12G --num-executors=8 district_regional_data_cleaning.py

## finding correlation between cleaned region data and fire events
spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 fire_stattion_data_cleaning_corelation.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fire_station.geojson fs-o-1

## wildfire historical record cleaning
spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 wildfire_history_cleaning.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/map_sample.json oo-1

## finding centroid of the polygon of fire events
spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 wildfire_history_for_station_corelation.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/map_sample.json oo-json-1

