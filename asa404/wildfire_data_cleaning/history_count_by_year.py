from cassandra.cluster import Cluster
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, TimestampType
import sys
import datetime
import re
import uuid as uuid
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, unix_time_from_uuid1
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, sum, count, lit, explode, reverse, udf, to_date,expr, substring, date_format, unix_timestamp, to_timestamp, year

import uuid as uuid
import math


def main(keyspace, table):
  # cluster_seeds = ['127.0.0.1']
  # cluster = Cluster(cluster_seeds, port=9042)

  secure_zip_path = "../../../../../ReactWorkspace/leaflet copy/ETL/secure-connect-fireforrestdb.zip"
  cloud_config = {
    "secure_connect_bundle" : secure_zip_path
  }

  username = "anant_awasthy@sfu.ca"
  password = "Hanuman@999"
  auth_provider = PlainTextAuthProvider(username=username, password=password)
  cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
  # cluster_seeds = ['127.0.0.1']
  # cluster = Cluster(cluster_seeds, port=9042)
  spark = SparkSession.builder.appName('Extract Historical Data') \
    .config("spark.driver.memory", "15g")\
    .getOrCreate()
  # spark = SparkSession.builder.appName('Spark Cassandra example') \
  #   .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
  session = cluster.connect(keyspace)
  session.execute(f"USE {keyspace}")
  # session.execute(f"TRUNCATE {table}")

  file_path = "../map_sample.json"
  df = spark.read.json(path=file_path, multiLine=True)
  df = df.select(explode('features'))
  df = df.select('col.geometry.coordinates', 'col.properties.CREATION_METHOD',
                 'col.properties.FEATURE_CODE',
                 'col.properties.OBJECTID',
                 'col.properties.FEATURE_AREA_SQM',
                 'col.properties.FEATURE_LENGTH_M',
                 'col.properties.FIRE_NUMBER',
                 'col.properties.FIRE_CAUSE',
                 'col.properties.FIRE_YEAR',
                 'col.properties.FIRE_LABEL',
                 'col.properties.FIRE_DATE',
                 'col.properties.FIRE_SIZE_HECTARES',
                 'col.properties.SOURCE',
                 'col.properties.GPS_TRACK_DATE',
                 )
  for colm in df.columns:
    df = df.withColumnRenamed(colm, colm.lower())
  default_time = '19180102'
  df = df.fillna({'fire_date': default_time})
  df = df.withColumn("fire_date", substring('fire_date', 1, 8))
  df = df.withColumn("new_fire_date", to_timestamp("fire_date", "yyyyMMdd"))
  df = df.drop('fire_date')
  df = df.withColumnRenamed('new_fire_date', 'fire_date')
  df = df.withColumn("year", year("fire_date"))
  df = df.groupBy('year').count()
  df = df.orderBy('year')

  df.write.format("org.apache.spark.sql.cassandra").options(
    table=table, keyspace=keyspace).save(mode="append")

if __name__ == '__main__':
    keyspace = 'asa404'
    table = 'history_count_by_year'
    main(keyspace, table)


# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions extract_historical_data.py


# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0 --files /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/secure-connect-fireforrestdb.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-fireforrestdb.zip --conf spark.cassandra.auth.username=anant_awasthy@sfu.ca --conf spark.cassandra.auth.password=Hanuman@999 extract_historical_data.py

# spark-submit --deploy-mode client --driver-memory 12G --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --files /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/secure-connect-fireforrestdb.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-fireforrestdb.zip --conf spark.cassandra.auth.username=anant_awasthy@sfu.ca --conf spark.cassandra.auth.password=Hanuman@999 --num-executors=8 extract_historical_bar.py


# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions extract_historical_bar.py


# spark-submit --deploy-mode client --driver-memory 12G --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --files /Users/anantsawasthy/Documents/ReactWorkspace/leaflet/secure-connect-fireforrestdb.zip --conf spark.cassandra.connection.config.cloud.path=secure-connect-fireforrestdb.zip --conf spark.cassandra.auth.username=anant_awasthy@sfu.ca --conf spark.cassandra.auth.password=Hanuman@999 --num-executors=8 history_count_by_year.py

# https://stackoverflow.com/questions/40161879/pyspark-withcolumn-with-two-conditions-and-three-outcomes