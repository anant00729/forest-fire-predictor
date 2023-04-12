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
from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext
import uuid as uuid
from pyspark.sql.functions import col, sum, count, lit, dayofmonth, hour, dayofyear, month, year, weekofyear, explode, reverse, lit, udf, substring, to_timestamp, collect_list, monotonically_increasing_id
from ast import literal_eval
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, unix_time_from_uuid1
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, sum, count, lit, explode, reverse, udf,to_timestamp,substring
import uuid as uuid
import math

import  sys


def main(input, keyspace, table):
  session = cluster.connect(keyspace)
  session.execute(f"USE {keyspace}")
  df = spark.read.json(input, multiLine=True)

  df = df.select(explode(df['features']))

  df = df.select(df['col.geometry.coordinates'], df['col.geometry.type'],
                 df['col.properties.ADMIN_AREA_NAME'],
                 df['col.properties.FEATURE_AREA_SQM'],
                 df['col.properties.OBJECTID']
                 )

  for colms in df.columns:
    df = df.withColumnRenamed(colms, colms.lower())

  df = df.withColumn('id', monotonically_increasing_id())

  mul_df = df.filter(df['type'] == 'MultiPolygon')
  s_df = df.filter(df['type'] == 'Polygon')

  cm_df = mul_df.select(mul_df['coordinates'], mul_df['id'])
  cm_df = cm_df.withColumnRenamed('id', 'coord_id')
  mul_df = mul_df.drop('coordinates')

  def rev_m(d):
    l1 = []
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
    return [d[1], l1]

  cm_df = cm_df.rdd.map(rev_m).toDF()
  cm_df = cm_df.withColumnRenamed('_1', 'coord_id')
  cm_df = cm_df.withColumnRenamed('_2', 'coordinates')

  mul_df = mul_df.join(cm_df, mul_df['id'] == cm_df['coord_id'])

  # mul_df = mul_df.withColumn('one', lit(1))
  # count_m = mul_df.groupBy(mul_df['one']).agg({'one': 'sum'})

  cp_df = s_df.select(s_df['coordinates'], s_df['id'])
  cp_df = cp_df.withColumnRenamed('id', 'coord_id')
  s_df = s_df.drop('coordinates')

  def rev(d):
    l1 = []
    for d1 in d[0]:
      l2 = []
      for d2 in d1:
        l3 = [float(d2[1]), float(d2[0])]
        l2.append(l3)
      l1.append(l2)
    return [d[1], l1]

  cp_df = cp_df.rdd.map(rev).toDF()
  cp_df = cp_df.withColumnRenamed('_1', 'coord_id')
  cp_df = cp_df.withColumnRenamed('_2', 'coordinates')

  s_df = s_df.join(cp_df, s_df['id'] == cp_df['coord_id'])

  # s_df = s_df.withColumn('one', lit(1))
  # count_s = s_df.groupBy(s_df['one']).agg({'one': 'sum'})

  df = df.drop('coordinates')

  for colms in s_df.columns:
    if not (colms == 'coord_id' or colms == 'coordinates'):
      s_df = s_df.drop(colms)

  df = df.join(s_df, df['id'] == s_df['coord_id'], how="left")

  for colms in mul_df.columns:
    if not (colms == 'coord_id' or colms == 'coordinates'):
      mul_df = mul_df.drop(colms)

  mul_df = mul_df.withColumnRenamed('coordinates', 'ploy_coordinates')
  df = df.join(mul_df, df['id'] == mul_df['coord_id'], how="left")
  df = df.drop('coord_id')

  print(df.select('coordinates', 'ploy_coordinates', 'id', 'objectid').show(n=30))
  # df.printSchema()

  df.write.format("org.apache.spark.sql.cassandra").options(
    table=table, keyspace=keyspace).save(mode="append")


if __name__ == '__main__':

  spark = SparkSession.builder.appName('regional data cleanup').getOrCreate()
  assert spark.version >= '3.0'  # make sure we have Spark 3.0+
  spark.sparkContext.setLogLevel('WARN')
  sc = spark.sparkContext
  keyspace = 'asa404'
  table = 'regional_partition_bc'
  input = '/Users/anantsawasthy/Documents/ReactWorkspace/leaflet/regional_data.geojson'

  secure_zip_path = "/Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/secure-connect-fireforrestdb.zip"
  cloud_config = {
    "secure_connect_bundle": secure_zip_path
  }
  username = "anant_awasthy@sfu.ca"
  password = "Bigdata@123"

  auth_provider = PlainTextAuthProvider('bjdCIOofvrpDSwAEKesZPiYA', '-9uE-jqWTYoSewAb0QS-vZtb9EgGMn3vl5_a+T8hDmpqRg8yH_zu12z-LkaZ1WNUGSS49dAEH6pb5SBuZcpUCEYRhi5dyobRn29C9lOxf5fd9pL.vleHquZZXgzIIiNb')
  # auth_provider = PlainTextAuthProvider(username=username, password=password)
  cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
  main(input, keyspace, table)


# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --files /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/ETL/secure-connect-fireforrestdb.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-fireforrestdb.zip --conf spark.cassandra.auth.username=bjdCIOofvrpDSwAEKesZPiYA --conf spark.cassandra.auth.password=-9uE-jqWTYoSewAb0QS-vZtb9EgGMn3vl5_a+T8hDmpqRg8yH_zu12z-LkaZ1WNUGSS49dAEH6pb5SBuZcpUCEYRhi5dyobRn29C9lOxf5fd9pL.vleHquZZXgzIIiNb --deploy-mode client --driver-memory 12G --num-executors=8 district_regional_data_cleaning.py