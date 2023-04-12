import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types
from pyspark import SparkConf


def main(inputs, output):
    # Defining the schema of the data set
    observation_schema = types.StructType([
        types.StructField('ID', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    # Reading in the data set
    weatherData = spark.read.csv(inputs, header=False, schema=observation_schema)

    # Removing any observations with quality issues
    weatherData = weatherData.where(weatherData['qflag'].isNull())

    # Pivoting the data
    weatherDataPivoted = weatherData.groupby("ID", "date").pivot("observation").sum("value")

    # # Writing to disk, partitioning by the station ID
    weatherDataPivoted.write.csv(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
