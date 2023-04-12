import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql import Row
from py4j.java_gateway import java_import # to rename the output file

air_schema = types.StructType([
    types.StructField('DATE_PST', types.StringType()),
    types.StructField('DATE', types.DateType()),
    types.StructField('TIME', types.StringType()),
    types.StructField('STATION_NAME', types.StringType()),
    types.StructField('STATION_NAME_FULL', types.StringType()),
    types.StructField('EMS_ID', types.StringType()),
    types.StructField('NAPS_ID', types.IntegerType()),
    types.StructField('RAW_VALUE', types.FloatType()),
    types.StructField('ROUNDED_VALUE', types.FloatType()),
    types.StructField('UNIT', types.StringType()),
    types.StructField('INSTRUMENT', types.StringType()),
    types.StructField('PARAMETER', types.StringType()),
    types.StructField('OWNER', types.StringType()),
    types.StructField('REGION', types.StringType()),
])

def main(inputs, output):
    air = spark.read.option("header","true").csv(inputs, schema=air_schema) # to ignore headers
    # air.show(5)
    # NA values are imported as NULL for TimestampType, DateType, IntegerType and FloatType
    # So drop all the rows if any of the cols in the dataframe has NULL values.
    air = air.na.drop("any")

    # since StringType cols are read still read as NA, remove all the rows having NA values in the following cols
    air = air.filter((air.TIME != 'NA'))
    air = air.filter((air.STATION_NAME != 'NA'))
    air = air.filter((air.STATION_NAME_FULL != 'NA'))
    air = air.filter((air.REGION != 'NA'))
    # air.show(5)

    # write as a single csv file with header information
    air.coalesce(1).write.option("header",True).csv(output, mode='overwrite')

    # ref: https://stackoverflow.com/questions/40792434/spark-dataframe-save-in-single-file-on-hdfs-location?rq=1
    # rename the output filename from output/part-00000-* to output.csv (eg: CO_1980_2008_cleaned.csv)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(sc._jvm.Path(output + '/part*'))[0].getPath().getName()
    fs.rename(sc._jvm.Path(output + '/' + file), sc._jvm.Path(output + '.csv'))
    fs.delete(sc._jvm.Path(output), True)

if __name__ == '__main__':
    inputs = sys.argv[1] # example: "/Volumes/RK - T7/big data project data/Annual_Summary/1980-2008/CO.csv"
    output = sys.argv[2] # example: "/Volumes/RK - T7/big data project data/raw cleaned files/CO_1980_2008_cleaned"
    spark = SparkSession.builder.appName('AIR Extract').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
