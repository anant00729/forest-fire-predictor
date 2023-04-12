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
from math import sqrt

def distance(a, b):
    return  sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


def point_line_distance(point, start, end):
    if (start == end):
        return distance(point, start)
    else:
        n = abs(
            (end[0] - start[0]) * (start[1] - point[1]) -
            (start[0] - point[0]) * (end[1] - start[1])
        )
        d = sqrt(
            (end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2
        )
        return n / d


def rdp(pointList, epsilon):
    dmax = 0.0
    index = 0
    for i in range(1, len(pointList) - 1):
        d = point_line_distance(pointList[i], pointList[0], pointList[-1])
        if d > dmax:
            index = i
            dmax = d

    if dmax >= epsilon:
        resultList = rdp(pointList[:index+1], epsilon)[:-1] + rdp(pointList[index:], epsilon)
    else:
        resultList = [pointList[0], pointList[-1]]

    return resultList


def main(input,output):
  df_s = spark.read.json(input)

  station_colms = list(df_s.columns).copy()
  station_colms.append('reduced_coordinates')
  station_colms.append('coordinates_count')
  station_colms.append('r_coordinates_count')

  print(df_s.columns)

  def reduce_station_polygon(x):
    coord = x[2]
    new_coord = [rdp(coord[0], 1)]
    return (*x, new_coord, len(coord[0]), len(new_coord[0]))



  df_s = df_s.rdd.map(reduce_station_polygon).toDF()

  j = 1
  for colms in station_colms:
    df_s = df_s.withColumnRenamed(f"_{j}", colms)
    j += 1

  df_s.select('reduced_coordinates', 'coordinates', 'r_coordinates_count', 'coordinates_count',
              'mof_fire_centre_name').show(n=10)

  df_s = df_s.drop('coordinates')
  df_s = df_s.drop('coordinates_count')
  df_s.coalesce(1).write.json(output, mode='append')

if __name__ == '__main__':
  spark = SparkSession.builder.appName('regional data cleanup').getOrCreate()
  assert spark.version >= '3.0'  # make sure we have Spark 3.0+
  spark.sparkContext.setLogLevel('WARN')
  sc = spark.sparkContext

  input = sys.argv[1]
  output = sys.argv[2]
  main(input,output)



# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 cleaning_coordinates_forrest_stattions.py fs-o-1 reduced-fs-o-1


