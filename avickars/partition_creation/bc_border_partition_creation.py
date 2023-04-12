import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import numpy as np


def main(inputs, output):
    # ************************************************************** Performing the initial work to create the paritions **************************************************************

    # *************** Reading in coordinates of the BC border **************************************

    # Defining the schema of the data
    schema = types.StructType([
        types.StructField('long', types.DoubleType()),
        types.StructField('lat', types.DoubleType())
    ])

    longLat = spark.read.csv(path=inputs, schema=schema)

    # Caching this as it will be used multiple times
    longLat.cache()

    # Defining this df as a sql object
    longLat.createOrReplaceTempView('longLat')

    # *************** Computing the extrema of border coords **************************************

    # Getting the extrema of the lat and long
    bottomleft = spark.sql("""select min(long) as long, min(lat) as lat from longLat""")
    topleft = spark.sql("""select min(long) as long, max(lat) as lat from longLat""")
    topRight = spark.sql("""select max(long) as long, max(lat) as lat from longLat""")

    # ************************************************************** Creating the partitions **************************************************************

    # Defining the number of squares we want (its actually numSquares*numSquares, just defining it this way as its easier to work with
    numSquares = 10

    # *************** Computing the longitude intervals of each square **************************************

    # Taking the left most longitude to the right most longitude with a range
    # Using numpy instead of sc.range() as it can handle floats
    # Immediately parallelizing it and putting it in a data frame
    topLeft_TO_topRight = sc.parallelize(np.linspace(start=topleft.rdd.take(1)[0][0], stop=topRight.rdd.take(1)[0][0], num=numSquares).tolist(), numSlices=numSquares)
    topLeft_TO_topRight = spark.createDataFrame(topLeft_TO_topRight, types.DoubleType())

    # Defining this df as a sql object
    topLeft_TO_topRight.createOrReplaceTempView('topLeft_TO_topRight')

    # Adding a unique identifier to each row to be used to join the table with itself
    topLeft_TO_topRight_with_ID = spark.sql("""
    SELECT
        row_number() OVER (
            PARTITION BY ''
            ORDER BY value
        ) as id,
        value as long
    FROM
        topLeft_TO_topRight
    """)

    # Defining this df as a sql object
    topLeft_TO_topRight_with_ID.createOrReplaceTempView('topLeft_TO_topRight_with_ID')

    # Joining it with itself except shift by one, this is so we have for each square we have the longitudes of each corner of the square
    topLeft_TO_topRight_Complete = spark.sql("""select T2.long as longLeft, T1.long as longRight  from topLeft_TO_topRight_with_ID T1 left join topLeft_TO_topRight_with_ID T2 on T1.id = T2.id + 1""")

    # Taking the top most latitude to the bottom most latitude
    topLeft_TO_bottomLeft = sc.parallelize(np.linspace(start=topleft.rdd.take(1)[0][1], stop=bottomleft.rdd.take(1)[0][1], num=numSquares).tolist(), numSlices=numSquares)
    topLeft_TO_bottomLeft = spark.createDataFrame(topLeft_TO_bottomLeft, types.DoubleType())

    # Defining this df as a sql object
    topLeft_TO_bottomLeft.createOrReplaceTempView('topLeft_TO_bottomLeft')

    # Adding a unique identifier to each row to be used to join the table with itself
    topLeft_TO_bottomLeft_with_ID = spark.sql("""
    SELECT
        row_number() OVER (
            PARTITION BY ''
            ORDER BY value desc
        ) as id,
        value as lat
    FROM
        topLeft_TO_bottomLeft
    """)

    # Defining this df as a sql object
    topLeft_TO_bottomLeft_with_ID.createOrReplaceTempView('topLeft_TO_bottomLeft_with_ID')

    # Joining it with itself except shift by one, this is so we have for each square we have the latitudes of each corner of the square
    topLeft_TO_bottomLeft_Complete = spark.sql(
        """select T1.lat as latBottom, T2.lat as latTop  from topLeft_TO_bottomLeft_with_ID T1 left join topLeft_TO_bottomLeft_with_ID T2 on T1.id = T2.id + 1""")

    # **************** Computing the squares **************************
    # Doing a cross join each since every latitude segment will be paired with every longitude segment to create the squares
    partitions = topLeft_TO_topRight_Complete.crossJoin(topLeft_TO_bottomLeft_Complete).dropna()

    # Repartitioned this as for some reason it had 40 000 partitions that caused a MASSIVE slowdown
    partitions = partitions.repartition(16)

    squares.write.csv(output, mode='overwrite', compression='gzip')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
