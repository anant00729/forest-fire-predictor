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
from pyspark.sql.functions import col, sum, count, lit, dayofmonth, hour, dayofyear, month, year, weekofyear, explode, reverse, lit, udf, substring, to_timestamp, collect_list, monotonically_increasing_id
from ast import literal_eval
import sys

from utils import find_centeroid as fc

def main(input,output):
  df = spark.read.json(input, multiLine=True)
  df = df.select(explode('features'))
  # df.show()
  df.printSchema()

  df = df \
    .select(
    "col.geometry.coordinates",
    "col.properties.FEATURE_AREA_SQM",
    "col.properties.FEATURE_LENGTH_M",
    "col.properties.MOF_FIRE_CENTRE_ID",
    "col.properties.MOF_FIRE_CENTRE_NAME",
    "col.properties.OBJECTID"
  )

  for colm in df.columns:
    df = df.withColumnRenamed(colm, colm.lower())

  print(df.columns)

  rdd = df.rdd

  def process_coordinates(x):
    STEP_SIZE = 10000
    #     d = []
    e = []
    l1 = []
    for d1 in x[0]:
      l2 = []
      for d2 in d1:
        l3 = [float(d2[1]), float(d2[0])]
        l2.append(l3)
      l1.append(l2)

    for l2 in l1:
      e = l2[0:len(l2) - 1:STEP_SIZE]
    #     center_cord = [float(np_float) for np_float in average_cord(d)]
    center_cord_e = [float(np_float) for np_float in fc.average_cord(e)]
    #     avg_cord = [int(x) for x, in avg_cord]
    #     avg_cord1 = [float(avg_cord[0], float(avg_cord[1]))]
    #     return (*x, list(avg_cord1), type(x[0]))
    #     return (*x, center_cord, center_cord_e, x[0][0])
    #     return (center_cord, center_cord_e, x[0][0][0])
    #     return (x[0][0][0], x[0][0][1], x[0][0][3], x[0][0][4], center_cord_e, center_cord)
    return (*x, center_cord_e[0], center_cord_e[1])

  df = rdd.map(process_coordinates).toDF()

  col_names = ['coordinates', 'feature_area_sqm', 'feature_length_m', 'mof_fire_centre_id', 'mof_fire_centre_name',
               'objectid', 'centeroid_lat', 'centeroid_lng']
  for i in range(0, len(df.columns)):
    df = df.withColumnRenamed(df.columns[i], col_names[i])

  # print(df.select('centeroid', 'coordinates').show(n=1))
  # df = df.drop('coordinates')

  # df = df.select(
  #     'feature_area_sqm',
  #     'feature_length_m',
  #     'mof_fire_centre_id',
  #     'mof_fire_centre_name',
  #     'objectid',
  #     explode('centeroid'))

  # df.printSchema()

  # df = df.drop('centeroid')

  # df = df.select('col')

  # print(df.columns)

  # df.show(n=1)
  # df.coalesce(1).write.json("fs-2", mode='append')
  df.coalesce(1).write.json(output, mode='append')

  # df\
  #   .select(
  #     "coordinates",
  #     "FEATURE_AREA_SQM",
  #     "FEATURE_LENGTH_M"
  #   ).show()

  # df\
  #   .select(
  #     "MOF_FIRE_CENTRE_ID",
  #     "MOF_FIRE_CENTRE_NAME",
  #     "OBJECTID"
  #   ).show()


if __name__ == '__main__':
  spark = SparkSession.builder.appName('regional data cleanup').getOrCreate()
  assert spark.version >= '3.0'  # make sure we have Spark 3.0+
  spark.sparkContext.setLogLevel('WARN')
  sc = spark.sparkContext

  input = sys.argv[1]
  output = sys.argv[2]
  main(input,output)


# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 fire_stattion_data_cleaning_corelation.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/geo_partition/fire_station.geojson fs-o-1


