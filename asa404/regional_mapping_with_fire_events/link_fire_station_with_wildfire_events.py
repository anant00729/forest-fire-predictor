from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, TimestampType, FloatType, DoubleType
import datetime
import re
import uuid as uuid
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, unix_time_from_uuid1
from pyspark.sql import SparkSession
import pandas as pd
# import org.apache.spark.sql.cassandra
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import uuid as uuid
spark = SparkSession.builder.appName('historical_fire_plots').getOrCreate()
from pyspark.sql.functions import col, sum, count, lit, dayofmonth, hour, dayofyear, month, year, weekofyear, explode, reverse, lit, udf, substring, to_timestamp, collect_list, monotonically_increasing_id, array
from ast import literal_eval
from geopy.distance import distance
from turfpy.measurement import boolean_point_in_polygon
from geojson import Point, Polygon, Feature
import sys



def main(input_1, input_2, output):
  df = spark.read.csv(input_1, multiLine=True, header=True)
  df_s = spark.read.json(input_2)
  # df = spark.read.csv('oo-1', multiLine=True, header=True)
  # df_s = spark.read.json('fs-o-1')

  df = df.withColumn('one', lit(1))
  df_count = df.groupBy('one').agg({'one': 'sum'})

  df_count.show()

  df = df.filter(df['type'] == 'Polygon')

  df = df.withColumn('one', lit(1))
  df_count = df.groupBy('one').agg({'one': 'sum'})

  df_count.show()

  df_s = df_s.withColumn('one', lit(1))
  dfs_count = df_s.groupBy('one').agg({'one': 'sum'})

  # df_s_1 = df_s.drop('coordinates')

  fire_centre_id_list = df_s.select('mof_fire_centre_id').rdd.collect()

  centre_id_list = []
  for f in fire_centre_id_list:
    centre_id_list.append(int(f[0]))

  # df.withColumn('fire_centre_id', array())

  all_wildfire_colms = df.columns.copy()

  all_wildfire_colms.append('station_ids')

  def add_firestation_ids(x):
    return (*x, centre_id_list)

  df = df.rdd.map(add_firestation_ids).toDF()

  col_num = 1
  for col_name in all_wildfire_colms:
    df = df.withColumnRenamed(f"_{col_num}", col_name)
    col_num += 1

  # df.select(*df.columns, explode('station_ids')).show(n=4)
  df_with_st_ids = df.select(*df.columns, explode('station_ids'))
  df_with_st_ids = df_with_st_ids.withColumnRenamed('col', 'sta_id')

  df_s_1 = df_s.select('reduced_coordinates', 'mof_fire_centre_id', 'mof_fire_centre_name', 'objectid')
  df_s_1 = df_s_1.withColumnRenamed('objectid', 'sta_objectid')

  # print(df_with_st_ids.columns)

  # print(df_s_1.columns)

  df_with_st_ids = df_with_st_ids.drop('one')
  df_with_st_ids = df_with_st_ids.join(df_s_1, df_s_1['mof_fire_centre_id'] == df_with_st_ids['sta_id'])

  # print(df_with_st_ids.columns)

  # df_with_st_ids.select('coordinates', 'sta_id', 'objectid', 'sta_objectid').show(n=10)

  # use this to test on the small chunk of data
  # df_sam_1 = df_with_st_ids\
  # .select('coordinates', 'sta_id', 'objectid', 'sta_objectid', 'centeroid_lat', 'centeroid_lng')\
  # .filter((df_with_st_ids['objectid'] == 1884006) | (df_with_st_ids['objectid'] == 1884007))\
  # .sort(df_with_st_ids['sta_objectid'])

  df_sam_1 = df_with_st_ids \
    .select('reduced_coordinates', 'sta_id', 'objectid', 'sta_objectid', 'centeroid_lat', 'centeroid_lng', 'fire_year', 'mof_fire_centre_name', 'fire_size_hectares', 'fire_cause').sort(
    df_with_st_ids['sta_objectid'])

  # df_sam_1.write.json(output, mode='append')

  # df_sam_1 = df_with_st_ids.select('coordinates', 'sta_id', 'objectid', 'sta_objectid', 'centeroid_lat', 'centeroid_lng').sort(df_with_st_ids['sta_objectid'])

  # df_sam_1.show(n=10)

  # 'coordinates', 'sta_id', 'objectid', 'sta_objectid'

  # 'centeroid_lat', 'centeroid_lng'

  # print(df_sam_1.columns)

  # print(df_with_st_ids.columns)

  df_sam_1_colm_names = ['coordinates', 'sta_id', 'objectid', 'sta_objectid', 'centeroid_lat', 'centeroid_lng', 'fire_year', 'mof_fire_centre_name', 'fire_size_hectares', 'fire_cause']

  def check_point_in_station_polygon(x):
    coord = x[0]
    c_lat = float(x[4])
    c_lng = float(x[5])
    point = Feature(geometry=Point((c_lng, c_lat)))
    polygon = Polygon(coord)
    is_present = boolean_point_in_polygon(point, polygon)
    return (*x, is_present)

  df_sam_1 = df_sam_1.rdd.map(check_point_in_station_polygon).toDF()
  df_sam_1_colm_names.append("is_present")

  k_ind = 1
  for colms in df_sam_1_colm_names:
    df_sam_1 = df_sam_1.withColumnRenamed(f"_{k_ind}", colms)
    k_ind += 1

  df_sam_1 = df_sam_1.filter(df_sam_1['is_present'] == 'true')
  df_sam_1 = df_sam_1.drop('coordinates')
  df_sam_1 = df_sam_1.drop('is_present')

  df_sam_1 = df_sam_1.sort(
    df_sam_1['sta_id'], df_sam_1['fire_year'])
  df_sam_1.show(n=10)
  df_sam_1.coalesce(1).write.csv(output, mode='append', header=True)


if __name__ == '__main__':
  spark = SparkSession.builder.appName('regional data cleanup').getOrCreate()
  assert spark.version >= '3.0'  # make sure we have Spark 3.0+
  spark.sparkContext.setLogLevel('WARN')
  sc = spark.sparkContext

  input_1 = sys.argv[1]
  input_2 = sys.argv[2]
  output = sys.argv[3]
  main(input_1, input_2 ,output)



# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 link_fire_station_with_wildfire_events.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-1 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1
# spark-submit  --deploy-mode client --driver-memory 50G --num-executors=20 link_fire_station_with_wildfire_events.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-1 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1
# spark-submit  --deploy-mode client --driver-memory 72G --num-executors=70 link_fire_station_with_wildfire_events.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-1 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1
# spark-submit --deploy-mode client --conf spark.executor.cores=4 --conf spark.executor.memory=72g --conf spark.driver.memory=72g --conf spark.executor.memoryOverhead=8192 --conf spark.dynamicAllocation.enabled=true  --conf spark.shuffle.service.enabled=true --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=2044 --conf spark.driver.maxResultSize=1g --conf spark.driver.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxDirectMemorySize=2g' --conf spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxDirectMemorySize=2g' link_fire_station_with_wildfire_events.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-2 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1

# /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-1 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1
# /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/oo-2 /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fs-o-1 link-output-1


# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 link_fire_station_with_wildfire_events.py oo-1 reduced-fs-o-1 map-link-output-1
# df = spark.read.csv('oo-1', multiLine=True, header=True)
  # df_s = spark.read.json('fs-o-1')


