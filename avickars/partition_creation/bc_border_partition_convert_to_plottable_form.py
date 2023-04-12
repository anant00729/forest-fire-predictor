import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.window import Window
from pyspark import SparkConf


def main(squaresCoordinatesPath, output):
    # ***************** Reading in the data *****************

    squaresSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType()),
        types.StructField('squareID', types.LongType())
    ])

    # Reading in the partitions coordinates
    partitionsCoordinates = spark.read.csv(squaresCoordinatesPath, schema=squaresSchema)

    # Creating the coordinates of the squares to plot using geopandas
    partitionsCoordinates = partitionsCoordinates.withColumn('topLeft', functions.array(partitionsCoordinates['longLeft'], partitionsCoordinates['latTop']))
    partitionsCoordinates = partitionsCoordinates.withColumn('topRight', functions.array(partitionsCoordinates['longRight'], partitionsCoordinates['latTop']))
    partitionsCoordinates = partitionsCoordinates.withColumn('bottomRight', functions.array(partitionsCoordinates['longRight'], partitionsCoordinates['latBottom']))
    partitionsCoordinates = partitionsCoordinates.withColumn('BottomLeft', functions.array(partitionsCoordinates['longLeft'], partitionsCoordinates['latBottom']))

    # Merging the coordinates into a single column
    partitionsCoordinates = partitionsCoordinates.withColumn('coordinates', functions.array(partitionsCoordinates['topLeft'], partitionsCoordinates['topRight'], partitionsCoordinates['bottomRight'], partitionsCoordinates['BottomLeft']))

    # Then exploding the coordinates so that each coordinate is its own row
    partitionsCoordinates = partitionsCoordinates.withColumn('coordinates', functions.explode(partitionsCoordinates['coordinates']))

    # Adjusting the data so each long and lat is its own column
    partitionsCoordinates = partitionsCoordinates.withColumn('long', partitionsCoordinates['coordinates'][0]).withColumn('lat', partitionsCoordinates['coordinates'][1])

    # Selecting the columns we want
    partitionsCoordinates = partitionsCoordinates.select(partitionsCoordinates['squareID'], partitionsCoordinates['long'], partitionsCoordinates['lat'])

    # Coalescing to 1 so it can be read, and writing to disk
    partitionsCoordinates.coalesce(1).write.parquet(output, mode='overwrite')


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]

    spark = SparkSession.builder.appName('forest_fire_counts').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
