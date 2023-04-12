import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.window import Window
from pyspark import SparkConf


def main(firePropertiesPath, fireCoordinatesPath, squaresCoordinatesPath, output):
    # ***************** Reading in the data *****************

    # Defining the schema of the data
    propertySchema = types.StructType([
        types.StructField('FIRE_NUMBER', types.StringType()),
        types.StructField('FIRE_YEAR', types.IntegerType()),
        types.StructField('FIRE_CAUSE', types.StringType()),
        types.StructField('FIRE_SIZE_HECTARES', types.DoubleType()),
        types.StructField('FIRE_DATE', types.DateType()),
        types.StructField('id', types.LongType())
    ])

    # Reading in the data set
    fireProperties = spark.read.csv(firePropertiesPath, schema=propertySchema)

    # Defining the schema of the data
    coordSchema = types.StructType([
        types.StructField('FIRE_NUMBER', types.StringType()),
        types.StructField('coordinates', types.ArrayType(elementType=types.ArrayType(elementType=types.DoubleType()))),
        types.StructField('id', types.LongType())
    ])

    # Reading in the data set
    fireCoordinates = spark.read.json(fireCoordinatesPath, schema=coordSchema)

    # Defining the schema of the data
    partitionSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType()),
        types.StructField('squareID', types.LongType())
    ])

    # Reading in the partitions coordinates
    partitionsCoordinates = spark.read.csv(squaresCoordinatesPath, schema=partitionSchema)

    # Broadcasting this as it is as there is only about 50ish rows
    partitionsCoordinates = functions.broadcast(partitionsCoordinates)

    # ***************** Parallelizing the coordinates of each fire *****************

    # Expanding the fire coordinates to maximize parallelism
    fireCoordinatesExpanded = fireCoordinates.withColumn('coordinates', functions.explode(fireCoordinates['coordinates']))

    # Extracing the lat/long into separate columns
    fireCoordinatesExpanded = fireCoordinatesExpanded.withColumn('long', fireCoordinatesExpanded['coordinates'][0]).withColumn('lat', fireCoordinatesExpanded['coordinates'][1]).drop('coordinates')

    # Defining sql objects
    partitionsCoordinates.createOrReplaceTempView('partitionsCoordinates')
    fireCoordinatesExpanded.createOrReplaceTempView('fireCoordinatesExpanded')

    # Joining the two data frames to create the mapping of squares and fires that they contain
    fireSquareMapping = spark.sql("""select distinct P.squareID,
                                    FP.FIRE_NUMBER,
                                    FP.id
                    from partitionsCoordinates P
                    left join fireCoordinatesExpanded FP
                        on P.longLeft <= FP.long and
                            p.longRight >= FP.long and
                            p.latBottom <= FP.lat and
                            p.latTop >= FP.lat""")

    # Defining sql objects
    fireSquareMapping.createOrReplaceTempView('fireSquareMapping')
    fireProperties.createOrReplaceTempView('fireProperties')

    # Joining the date, year and hectares here as its just easier since its already read in.
    partitionsAndFireProperties = spark.sql("""select FSM.squareID,
                        FSM.FIRE_NUMBER,
                        FSM.id,
                        FP.fire_year,
                        FP.FIRE_DATE,
                        FP.FIRE_SIZE_HECTARES
                from fireSquareMapping FSM
                inner join fireProperties FP on FSM.FIRE_NUMBER = FP.FIRE_NUMBER and
                            FP.id = FSM.id""")

    # Writing to disk
    # Coalescing to 1 so as to be able to read in pandas.  See the README under forest_fire_correlation.py for why I am using pandas
    partitionsAndFireProperties.coalesce(1).write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    input_1 = sys.argv[1]
    input_2 = sys.argv[2]
    input_3 = sys.argv[3]
    output = sys.argv[4]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_fire_counts').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_1, input_2, input_3, output)
