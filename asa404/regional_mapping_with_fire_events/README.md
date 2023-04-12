# asa404

## Description

This folder contains all ETL done by Anant S Awasthy

# To run the scripts for each file 

## Cleaning and storing the data on the astra db cluster (Cassandra)
spark-submit --deploy-mode client --conf spark.executor.cores=4 --conf spark.executor.memory=72g --conf spark.driver.memory=72g --conf spark.executor.memoryOverhead=8192 --conf spark.dynamicAllocation.enabled=true  --conf spark.shuffle.service.enabled=true --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=2044 --conf spark.driver.maxResultSize=1g --conf spark.driver.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxDirectMemorySize=2g' --conf spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxDirectMemorySize=2g' link_fire_station_with_wildfire_events.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-2 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1

## Cleaning fire regions in CSV format (coordinates not included)
spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 fire_stattion_data_cleaning.py
