import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import IntegerType,BooleanType,DateType
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
    # main logic starts here
    air = spark.read.csv(inputs, header=True, schema=air_schema)

    # ref: https://spark.apache.org/docs/2.2.0/sql-programming-guide.html
    # "Register the DataFrame as a SQL temporary view"
    air.createOrReplaceTempView("air_raw")

    air_df = spark.sql("SELECT first_value(NAPS_ID) as NAPS_ID, first_value(EMS_ID) as EMS_ID, STATION_NAME, first_value(REGION) as REGION, first_value(OWNER) as OWNER, first_value(DATE) as `DATE`, \
                            YEAR(DATE_PST) as `YEAR`, \
                            round(avg(ROUNDED_VALUE), 3) as `ANNUAL1-HR_AVG` \
                        FROM `air_raw`  \
                        GROUP By STATION_NAME, YEAR \
                        ORDER BY STATION_NAME, YEAR")
    air_df.createOrReplaceTempView("air_stats_annual")
    # air_df.show(n=5)

    air_df = spark.sql("SELECT first_value(NAPS_ID) as NAPS_ID, first_value(EMS_ID) as EMS_ID, STATION_NAME, first_value(REGION) as REGION, first_value(OWNER) as OWNER, first_value(DATE) as `DATE`, \
                            YEAR(DATE_PST) as `YEAR`, MONTH(DATE_PST) as `MONTH`, \
                            round(avg(ROUNDED_VALUE), 3) as `MONTHLY1-HR_AVG` \
                        FROM `air_raw`  \
                        GROUP By STATION_NAME, YEAR, MONTH \
                        ORDER BY STATION_NAME, YEAR, MONTH")
    air_df.createOrReplaceTempView("air_stats_monthly")
    # air_df.show(n=5)


    air_df = spark.sql("SELECT m.NAPS_ID, m.EMS_ID, m.STATION_NAME, m.REGION, m.OWNER, m.DATE, \
                            m.MONTH, a.YEAR, \
                            m.`MONTHLY1-HR_AVG`, a.`ANNUAL1-HR_AVG` \
                        from `air_stats_monthly` m  \
                        join `air_stats_annual` a on m.STATION_NAME = a.STATION_NAME and m.`YEAR` = a.`YEAR` \
                        ORDER BY STATION_NAME, YEAR, MONTH")
    air_df.createOrReplaceTempView("air_stats")
    # air_df.show(n=5)

    # write as a single csv file with header information
    air_df.coalesce(1).write.option("header",True).csv(output, mode='overwrite')

    # ref: https://stackoverflow.com/questions/40792434/spark-dataframe-save-in-single-file-on-hdfs-location?rq=1
    # rename the output filename from output/part-00000-* to output.csv (eg: CO_1980_2008_cleaned.csv)
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(sc._jvm.Path(output + '/part*'))[0].getPath().getName()
    fs.rename(sc._jvm.Path(output + '/' + file), sc._jvm.Path(output + '.csv'))
    fs.delete(sc._jvm.Path(output), True)

if __name__ == '__main__':
    inputs = sys.argv[1] # example: "/Volumes/RK - T7/big data project data/raw cleaned files/CO_1980_2008_cleaned"
    output = sys.argv[2] # example: "/Volumes/RK - T7/big data project data/raw cleaned files/CO_1980_2008_cleaned"
    spark = SparkSession.builder.appName('AIR (Transform) Clean SQL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
