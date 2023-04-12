import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import numpy as np
from math import pi


# it is an implementation of the ray-casting algorithm
# CITATION: https://en.wikipedia.org/wiki/Point_in_polygon
@functions.udf(returnType=types.BooleanType())
def is_square_in_bc(longLeft, longRight, latBottom, latTop, borderCoordinates):
    # Going from top left - top right - bottom right - bottom left
    squareCoordinates = [(longLeft, latTop), (longRight, latTop), (longRight, latBottom), (longLeft, latBottom), ((longLeft + longRight) / 2, (latBottom + latTop) / 2)]
    length = len(borderCoordinates)
    count = 0
    for squareCoordinate in squareCoordinates:
        for i in range(0, length):
            if i == length - 1:
                coord1 = borderCoordinates[i]
                coord2 = borderCoordinates[(i + 1) % length]
            else:
                coord1 = borderCoordinates[i]
                coord2 = borderCoordinates[i + 1]

            if (coord1[0] >= squareCoordinate[0]) & (coord2[0] >= squareCoordinate[0]):
                test_1 = (coord1[1] <= squareCoordinate[1]) & (coord2[1] >= squareCoordinate[1])
                test_2 = (coord2[1] <= squareCoordinate[1]) & (coord1[1] >= squareCoordinate[1])
                if test_1 | test_2:
                    count += 1

        # We just need to know if a single coordinate is inside
        if count % 2 == 1:
            return True

    return False


def main(input_1, input_2, output):
    # ************************************************************** Performing the initial work to create the paritions **************************************************************

    # *************** Reading in coordinates of the BC border **************************************

    # Defining the schema of the data
    schema = types.StructType([
        types.StructField('long', types.DoubleType()),
        types.StructField('lat', types.DoubleType())
    ])

    longLat = spark.read.csv(path=input_1, schema=schema)

    # Caching this as it will be used multiple times
    longLat.cache()

    # Broadcasting this as I KNOW its not too big, makes the filtering in is_square_in_bc() that gets called later faster
    longLatCompressed = longLat.select((functions.array(longLat['long'], longLat['lat'])).alias('coordinates'))
    longLatCompressed = longLatCompressed.select(((functions.collect_list(longLatCompressed['coordinates']))).alias('borderCoordinates'))
    longLatCompressed = functions.broadcast(longLatCompressed)

    partitionsSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType())
    ])

    # Reading in the partitions coordinates
    partitions = spark.read.csv(input_2, schema=partitionsSchema)

    # Cross joining so each partition gets tested
    partitions = partitions.crossJoin(longLatCompressed)

    partitions = partitions.select(partitions['longLeft'], partitions['longRight'], partitions['latBottom'], partitions['latTop'],
                             is_square_in_bc(partitions['longLeft'], partitions['longRight'], partitions['latBottom'], partitions['latTop'], partitions['borderCoordinates']).alias('in_bc'))

    # There is 1 square that is in BC that still gets filtered out (its a freak situation where every corner and the center is outside BC but still covers area in BC)
    partitions = partitions.where((partitions['in_bc'] == True) | ((partitions['longLeft'] == -139.0613058785958) & (partitions['latTop'] == 60.00478255729097))).drop('in_bc')

    # Creating a unique identifier for each square
    partitionsDFWithIdentifier = partitions.withColumn('squareID', functions.monotonically_increasing_id())

    # Writing to disk
    partitionsDFWithIdentifier.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    input_1 = sys.argv[1]
    input_2 = sys.argv[2]
    output = sys.argv[3]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_1, input_2, output)
