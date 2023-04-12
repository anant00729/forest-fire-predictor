import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql import Row
from py4j.java_gateway import java_import # to rename the output file


air_schema = types.StructType([
    types.StructField('NAPS_ID', types.IntegerType()),
    types.StructField('EMS_ID', types.StringType()),
    types.StructField('STATION_NAME', types.StringType()),
    types.StructField('REGION', types.StringType()),
    types.StructField('OWNER', types.StringType()),
    types.StructField('DATE', types.StringType()),
    types.StructField('MONTH', types.IntegerType()),
    types.StructField('YEAR', types.IntegerType()),
    types.StructField('MONTHLY1-HR_AVG-STATION', types.FloatType()),
    types.StructField('ANNUAL1-HR_AVG-STATION', types.FloatType()),
    types.StructField('MONTHLY1-HR_AVG-REGION', types.FloatType()),
    types.StructField('ANNUAL1-HR_AVG-REGION', types.FloatType()),

])

geo_schema = types.StructType([
    types.StructField('STATION_NAME_FULL', types.StringType()),
    types.StructField('STATION_NAME', types.StringType()),
    types.StructField('EMS_ID', types.StringType()),
    types.StructField('SERIAL', types.IntegerType()),
    types.StructField('ADDRESS', types.StringType()),
    types.StructField('CITY', types.StringType()),
    types.StructField('LAT', types.FloatType()),
    types.StructField('LONG', types.FloatType()),
    types.StructField('ELEVATION', types.IntegerType()),
    types.StructField('STATUS_DESCRIPTION', types.StringType()),
    types.StructField('OWNER', types.StringType()),
    types.StructField('REGION', types.StringType()),
    types.StructField('STATUS', types.StringType()),
    types.StructField('OPENED', types.TimestampType()),
    types.StructField('CLOSED', types.StringType()),
    types.StructField('NAPS_ID', types.IntegerType()),

])


def main(air_file, geo_file, output):
    # main logic starts here

    # Read air quality data file
    air = spark.read.csv(air_file, header=True, schema=air_schema)
    # Read bc_air_monitoring_stations data file
    geo = spark.read.csv(geo_file, header=True, schema=geo_schema)

    # ref: https://spark.apache.org/docs/2.2.0/sql-programming-guide.html
    # "Register the DataFrame as a SQL temporary view"
    air.createOrReplaceTempView("air")
    geo.createOrReplaceTempView("geo")

    # JOIN air and geo tables and only add lat and long to air_df's columns for the final df
    air_stats_df = spark.sql("SELECT a.*, g.LAT, G.LONG FROM air a \
                                JOIN geo g ON a.STATION_NAME = g.STATION_NAME ")

    # drop rows with empty lat/long to avoid issues during data ingestion into Postgres
    air_stats_df = air_stats_df.na.drop("any")

    # write as a single csv file with header information
    air_stats_df.coalesce(1).write.option("header",True).csv(output, mode='overwrite')

    # ref: https://stackoverflow.com/questions/40792434/spark-dataframe-save-in-single-file-on-hdfs-location?rq=1
    # rename the output filename from output/part-00000-* to output.csv (eg: CO_1980_2008_cleaned.csv)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(sc._jvm.Path(output + '/part*'))[0].getPath().getName()
    fs.rename(sc._jvm.Path(output + '/' + file), sc._jvm.Path(output + '.csv'))
    fs.delete(sc._jvm.Path(output), True)

if __name__ == '__main__':
    air_file = sys.argv[1] # example: "/Volumes/RK - T7/big data project data/spark transformed files/CO_1980_2008_cleaned_stats.csv"
    geo_file = sys.argv[2] # example: "/Volumes/RK - T7/big data project data/Annual_Summary/1980-2008/bc_air_monitoring_stations.csv"
    output = sys.argv[3] # example: "/Volumes/RK - T7/big data project data/spark transformed files with geo coords/CO_1980_2008_cleaned_stats"

    spark = SparkSession.builder.appName('Add Lat/Long to AIR quality files - SQL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(air_file, geo_file, output)
