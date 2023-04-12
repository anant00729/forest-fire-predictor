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
spark = SparkSession.builder.appName('wildfire_history_cleaning').getOrCreate()
from pyspark.sql.functions import col, sum, count, lit, dayofmonth, hour, dayofyear, month, year, weekofyear, explode, reverse, lit, udf, substring, to_timestamp, collect_list, monotonically_increasing_id
from ast import literal_eval
import sys

# from regional_mapping_with_fire_events.utils import find_centeroid as fc

from utils import find_centeroid as fc

def main(input, output):
  df = spark.read.json(input, multiLine=True)

  df = df.select(explode(df['features']))

  # df.printSchema()

  df = df.select([
    'col.geometry.coordinates',
    'col.geometry.type',
    'col.properties.LOAD_DATE',
    'col.properties.CREATION_METHOD',
    'col.properties.FEATURE_CODE',
    'col.properties.OBJECTID',
    'col.properties.FEATURE_AREA_SQM',
    'col.properties.FEATURE_LENGTH_M',
    'col.properties.FIRE_NUMBER',
    'col.properties.FIRE_CAUSE',
    'col.properties.FIRE_YEAR',
    'col.properties.FIRE_LABEL',
    'col.properties.FIRE_SIZE_HECTARES',
    'col.properties.SOURCE'
  ])

  for colms in df.columns:
    df = df.withColumnRenamed(colms, colms.lower())

  df = df.withColumn('id', monotonically_increasing_id())

  mul_df = df.filter(df['type'] == 'MultiPolygon')
  s_df = df.filter(df['type'] == 'Polygon')

  s_df = s_df.withColumn('one', lit(1))
  count_s = s_df.groupBy(s_df['one']).agg({'one': 'sum'})

  mul_df = mul_df.withColumn('one', lit(1))
  count_mul = mul_df.groupBy(mul_df['one']).agg({'one': 'sum'})

  cm_df = mul_df.select(mul_df['coordinates'], mul_df['id'])
  cm_df = cm_df.withColumnRenamed('id', 'coord_id')
  mul_df = mul_df.drop('coordinates')

  def rev_m(d):
    l1 = []
    e = []
    for c1 in d[0]:
      l2 = []
      for c2 in c1:
        l3 = []
        for c3 in c2:
          c4 = literal_eval(c3)
          c5 = [float(c4[1]), float(c4[0])]
          l3.append(c5)
        l2.append(l3)
      l1.append(l2)

    #     l1[l2[l3[123,123]]]
    for l2 in l1:
      e = l2[0]
    center_cord_e = [float(np_float) for np_float in fc.average_cord(e)]
    return [d[1], l1, center_cord_e[0], center_cord_e[1]]

  cm_df = cm_df.rdd.map(rev_m).toDF()
  cm_df = cm_df.withColumnRenamed('_1', 'coord_id')
  cm_df = cm_df.withColumnRenamed('_2', 'coordinates')
  cm_df = cm_df.withColumnRenamed('_3', 'mc_lat')
  cm_df = cm_df.withColumnRenamed('_4', 'mc_lng')

  print(cm_df.show(n=1))

  mul_df = mul_df.join(cm_df, mul_df['id'] == cm_df['coord_id'])

  mul_df = mul_df.withColumn('one', lit(1))
  count_m = mul_df.groupBy(mul_df['one']).agg({'one': 'sum'})

  # START

  cp_df = s_df.select(s_df['coordinates'], s_df['id'])
  cp_df = cp_df.withColumnRenamed('id', 'coord_id')
  s_df = s_df.drop('coordinates')

  def rev(d):
    l1 = []
    e = []
    for d1 in d[0]:
      l2 = []
      for d2 in d1:
        l3 = [float(d2[1]), float(d2[0])]
        l2.append(l3)
      l1.append(l2)
    for l2 in l1:
      e = l2
    center_cord_e = [float(np_float) for np_float in fc.average_cord(e)]
    return [d[1], l1, center_cord_e[0], center_cord_e[1]]

  cp_df = cp_df.rdd.map(rev).toDF()
  cp_df = cp_df.withColumnRenamed('_1', 'coord_id')
  cp_df = cp_df.withColumnRenamed('_2', 'coordinates')
  cp_df = cp_df.withColumnRenamed('_3', 'c_lat')
  cp_df = cp_df.withColumnRenamed('_4', 'c_lng')

  # print(cp_df.show(n=1))

  # print(coord_df.show(n=5))

  s_df = s_df.join(cp_df, s_df['id'] == cp_df['coord_id'])

  s_df = s_df.withColumn('one', lit(1))
  count_s = s_df.groupBy(s_df['one']).agg({'one': 'sum'})

  df = df.drop('coordinates')

  for colms in s_df.columns:
    if not (colms == 'coord_id' or colms == 'coordinates' or colms == 'c_lat' or colms == 'c_lng'):
      s_df = s_df.drop(colms)

  df = df.join(s_df, df['id'] == s_df['coord_id'], how="left")

  for colms in mul_df.columns:
    if not (colms == 'coord_id' or colms == 'coordinates' or colms == 'mc_lat' or colms == 'mc_lng'):
      mul_df = mul_df.drop(colms)

  mul_df = mul_df.withColumnRenamed('coordinates', 'ploy_coordinates')
  df = df.join(mul_df, df['id'] == mul_df['coord_id'], how="left")
  df = df.drop('coord_id')
  # df.printSchema()

  # null_df = df.filter(df['fire_year'].isNull()).select('type', 'ploy_coordinates', 'coordinates', 'fire_year')
  # null_df = null_df.withColumn('one', lit(1))
  # count_n = null_df.groupBy(null_df['one']).agg({'one' : 'sum'})
  # count_n.show()
  # df = df.drop('ploy_coordinates')
  # df = df.drop('coordinates')
  # df.printSchema()

  df = df.na.fill(0, subset=['mc_lat', 'mc_lng', 'c_lat', 'c_lng'])

  df = df.withColumn('centeroid_lat', df['mc_lat'] + df['c_lat'])
  df = df.withColumn('centeroid_lng', df['mc_lng'] + df['c_lng'])

  df = df.drop('mc_lng')
  df = df.drop('mc_lat')
  df = df.drop('c_lat')
  df = df.drop('c_lng')
  # df.select('centeroid_lat', 'centeroid_lng', 'type').show()
  df.repartition(500).write.json(output, mode='append')
  df.printSchema()

  # df = df.withColumn('two', lit(1))
  # df_count = df.groupBy('two').agg({'two': 'sum'})
  # df_count.show()


if __name__ == '__main__':
  spark = SparkSession.builder.appName('regional data cleanup').getOrCreate()
  assert spark.version >= '3.0'  # make sure we have Spark 3.0+
  spark.sparkContext.setLogLevel('WARN')
  sc = spark.sparkContext

  input = sys.argv[1]
  output = sys.argv[2]
  main(input, output)


# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 wildfire_history_for_station_corelation.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/map_single_sample.json oo-1
# spark-submit  --deploy-mode client --driver-memory 12G --num-executors=8 wildfire_history_for_station_corelation.py /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/map_sample.json oo-json-1





