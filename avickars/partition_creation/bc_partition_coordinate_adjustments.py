import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import numpy as np
from math import pi, floor


@functions.udf(returnType=types.ArrayType(elementType=types.ArrayType(elementType=types.DoubleType())))
def create_granular_coordinates(longLeft, longRight, latBottom, latTop):
    # This variable defines the level of granularity we will use.
    granularity = 10

    leftToRight = np.linspace(start=longLeft, stop=longRight, num=granularity).tolist()
    topToBottom = np.linspace(start=latTop, stop=latBottom, num=granularity).tolist()

    i = 0.0
    coordinates = []

    # Top left to top right
    for longitude in leftToRight:
        coordinates += [[longitude, latTop, i]]
        i += 1

    # Top Right to Bottom Right
    for latitude in topToBottom:
        coordinates += [[longLeft, latitude, i]]
        i += 1

    # Bottom Right to Bottom Left
    leftToRight.reverse()
    for longitude in leftToRight:
        coordinates += [[longitude, latBottom, i]]
        i += 1

    # Bottom Left to Top Left
    topToBottom.reverse()
    for latitude in topToBottom:
        coordinates += [[longLeft, latitude, i]]
        i += 1

    return coordinates


@functions.pandas_udf(returnType=types.DoubleType())
def haversine(lat1, lon1, lat2, lon2):
    p = pi / 180
    a = 0.5 - np.cos((lat2 - lat1) * p) / 2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
    return 12742 * np.arcsin(np.sqrt(a))


# it is an implementation of the ray-casting algorithm
# CITATION: https://en.wikipedia.org/wiki/Point_in_polygon
@functions.udf(returnType=types.BooleanType())
def is_square_in_bc(coordinate, borderCoordinates):
    length = len(borderCoordinates)
    count = 0
    for i in range(0, length):
        if i == length - 1:
            coord1 = borderCoordinates[i]
            coord2 = borderCoordinates[(i + 1) % length]
        else:
            coord1 = borderCoordinates[i]
            coord2 = borderCoordinates[i + 1]

        if (coord1[0] >= coordinate[0]) & (coord2[0] >= coordinate[0]):
            test_1 = (coord1[1] <= coordinate[1]) & (coord2[1] >= coordinate[1])
            test_2 = (coord2[1] <= coordinate[1]) & (coord1[1] >= coordinate[1])
            if test_1 | test_2:
                count += 1

    # We just need to know if a single coordinate is inside
    if count % 2 == 1:
        return True

    return False


def main(borderCoordinates, squares, output):
    # ************************************************************** Creating partition granular coordinates **************************************************************

    # *************** Reading in coordinates of the BC border **************************************

    # Defining the schema of the data
    schema = types.StructType([
        types.StructField('long', types.DoubleType()),
        types.StructField('lat', types.DoubleType())
    ])

    # Reading in the data set
    longLat = spark.read.csv(path=borderCoordinates,
                             schema=schema)

    # Repartitioning as it is read in as a single file
    longLat = longLat.repartition(16)

    # Caching this as it will be used multiple times
    longLat.cache()

    # Broadcasting this as I KNOW its not too big, makes the filtering in calculate_square_type() that gets called later faster
    longLatCompressed = longLat.select((functions.array(longLat['long'], longLat['lat'])).alias('coordinates'))
    longLatCompressed = longLatCompressed.select(((functions.collect_list(longLatCompressed['coordinates']))).alias('borderCoordinates'))
    longLatCompressed = functions.broadcast(longLatCompressed)

    # **************** Reading in the partition information **************************

    # Defining the schema of the data
    squaresSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType()),
        types.StructField('squareID', types.LongType())
    ])

    # Reading in the squares data
    squares = spark.read.csv(squares, header=False, schema=squaresSchema)

    # Creating granular coordinates between each corner of every square
    squaresWithCoords = squares.withColumn('coords', create_granular_coordinates(squares[0],
                                                                                 squares[1],
                                                                                 squares[2],
                                                                                 squares[3]))

    # Exploding the list of lists into rows for the coordinates
    # CITATION: https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/
    squaresWithCoordExpanded = squaresWithCoords.withColumn("coordinates", functions.explode(squaresWithCoords['coords'])).drop("coords")

    # Extracting the coordinate number
    squaresWithCoordExpanded = squaresWithCoordExpanded.withColumn('coordNumber', squaresWithCoordExpanded['coordinates'][2])

    # Creating a column of coordinates that contains only the coordinate
    squaresWithCoordExpanded = squaresWithCoordExpanded.withColumn('coordinates', functions.array(squaresWithCoordExpanded['coordinates'][0], squaresWithCoordExpanded['coordinates'][1]))

    # **************** Finding the external and internal coordinates **************************

    squaresAndBorder = squaresWithCoordExpanded.crossJoin(longLatCompressed)

    squaresWithInBCIndicator = squaresAndBorder.select(squaresAndBorder['squareID'],
                                                       squaresAndBorder['coordNumber'],
                                                       squaresAndBorder['coordinates'],
                                                       is_square_in_bc(squaresAndBorder['coordinates'], squaresAndBorder['borderCoordinates']).alias('in_bc'))

    squaresWithInBCIndicator.cache()

    coordsInBC = squaresWithInBCIndicator.where(squaresWithInBCIndicator['in_bc'] == True)
    coordsNotInBC = squaresWithInBCIndicator.where(squaresWithInBCIndicator['in_bc'] == False)

    coordsNotInBCWithBorderCoords = coordsNotInBC.crossJoin(longLat)

    # Getting the distance between the coordinates and all border coordinates
    coordsNotInBCWithBorderCoords = coordsNotInBCWithBorderCoords.withColumn('distance',
                                                                             haversine(coordsNotInBCWithBorderCoords['lat'],
                                                                                       coordsNotInBCWithBorderCoords['long'],
                                                                                       coordsNotInBCWithBorderCoords['coordinates'][1],
                                                                                       coordsNotInBCWithBorderCoords['coordinates'][0])
                                                                             )

    # Caching this as it is used multiple times
    coordsNotInBCWithBorderCoords.cache()

    # Computing the closest coordinate on the border
    minDistances = coordsNotInBCWithBorderCoords.groupby('squareID', 'coordNumber').agg(functions.min('distance').alias('minDistance'))

    # Defining these dfs as a sql object
    minDistances.createOrReplaceTempView('minDistances')
    coordsNotInBCWithBorderCoords.createOrReplaceTempView('coordsNotInBCWithBorderCoords')

    # Performing a filtering join to reduce down to the closest internal point for each external point
    squaresWithCoordTypes_External_Fixed = spark.sql("""select S.squareID,
                                                        S.coordNumber,
                                                        S.long as long,
                                                        S.lat as lat
                                                from coordsNotInBCWithBorderCoords S
                                                inner join minDistances MD
                                                on S.squareID = MD.squareID and
                                                    S.coordNumber = MD.coordNumber and
                                                    S.distance = MD.minDistance""")

    squaresWithCoordTypes_External_Fixed = squaresWithCoordTypes_External_Fixed.select(squaresWithCoordTypes_External_Fixed['squareID'],
                                                                                       squaresWithCoordTypes_External_Fixed['coordNumber'],
                                                                                       squaresWithCoordTypes_External_Fixed['long'],
                                                                                       squaresWithCoordTypes_External_Fixed['lat'])

    # Adjusting the format of the coordinates to make a join with squaresWithCoordTypes_External_Fixed, and so it is on plottable form
    coordsInBC = coordsInBC.withColumn('long', coordsInBC['coordinates'][0]).withColumn('lat', coordsInBC['coordinates'][1])
    coordsInBC = coordsInBC.select(coordsInBC['squareID'], coordsInBC['coordNumber'], coordsInBC['long'], coordsInBC['lat'])

    # Merging the adjusted coordinates
    partitionCoordinates = coordsInBC.union(squaresWithCoordTypes_External_Fixed)

    # Coalescing to 1 so it can be plotted
    partitionCoordinates.coalesce(1).orderBy(['squareID', 'coordNumber']).write.parquet(output, mode='overwrite')


if __name__ == '__main__':
    borderCoordinates = sys.argv[1]
    squares = sys.argv[2]
    output = sys.argv[3]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(borderCoordinates, squares, output)
