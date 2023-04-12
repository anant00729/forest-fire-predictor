import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf


def main(forestCoordinates, squaresCoordinates, output):
    # ***************** Reading in data sets *****************

    # Reading in coordinates of forests

    # Defining the schema of the data
    coordSchema = types.StructType([
        types.StructField('FOREST_COVER_ID', types.LongType()),
        types.StructField('polygonID', types.LongType()),
        types.StructField('long', types.DoubleType()),
        types.StructField('lat', types.DoubleType()),

    ])

    coordinates = spark.read.json(forestCoordinates, schema=coordSchema)

    # Reading in the partitions
    squaresSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType()),
        types.StructField('squareID', types.LongType())
    ])

    partitionsCoordinates = spark.read.csv(squaresCoordinates, schema=squaresSchema)

    # ***************** Creating the mapping *****************

    # Broadcasting this as it is as there is only about 50ish rows
    partitionsCoordinates = functions.broadcast(partitionsCoordinates)

    # Defining both DFs as a sql object
    partitionsCoordinates.createOrReplaceTempView('partitionsCoordinates')
    coordinates.createOrReplaceTempView('coordinates')

    # Joining the two data frames to create the mapping of squares and forests that they contain
    forestSquareMapping = spark.sql("""select distinct P.squareID, 
                                    FC.FOREST_COVER_ID
                    from partitionsCoordinates P
                    left join coordinates FC 
                        on P.longLeft <= FC.long and
                            p.longRight >= FC.long and 
                            p.latBottom <= FC.lat and 
                            p.latTop >= FC.lat""")

    # Writing to disk
    forestSquareMapping.write.csv(output, mode='overwrite', header=False, compression='gzip')


if __name__ == '__main__':
    forestCoordinates = sys.argv[1]
    squaresCoordinates = sys.argv[2]
    output = sys.argv[3]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_data_preprocessing').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(forestCoordinates, squaresCoordinates, output)
