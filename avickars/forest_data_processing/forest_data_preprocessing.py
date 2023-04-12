import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import os
import json


def get_coordinates(forest):
    coordinates = forest['geometry']['coordinates']
    forestIdentifier = forest['properties']['FOREST_COVER_ID']
    return forestIdentifier, coordinates


def get_properties(forest):
    return forest['properties']


def flatteen_coordinates(x): return x


def convert_values(row):
    # Have the try/except statement here since some rows are 3 layers deep, while others are 2
    try:
        return row[0], [[float(i[0]), float(i[1])] for i in row[1][0]]
    except TypeError:
        return row[0], [[float(i[0]), float(i[1])] for i in row[1]]


def select_properties(properties):
    return properties['FOREST_COVER_ID'], \
           float(properties['SILV_POLYGON_AREA']), \
           properties['REFERENCE_YEAR'], \
           properties['SITE_INDEX'], \
           properties['I_TOTAL_STEMS_PER_HA'], \
           properties['I_CROWN_CLOSURE_PERCENT']


def main(inputs, output):
    # A indicator if this is the first file we are working on, used when writing to disk
    firstIteration = True

    for file in os.listdir(inputs):
        print("****************************************************************************************************************************************************************************************************")
        print('Working on:', file)
        # Have to read in each file into memory since they are complete json files and aren't separated by a new line
        # CITATION: https://www.geeksforgeeks.org/read-json-file-using-python/
        # Opening the file
        f = open(inputs + file, )

        # Reading in the entire file into memory (largest is about 5gb, so I can handle it)
        forestData = json.load(f)

        # Closing the file
        f.close()

        # Immediately subsetting to the data we want
        forestFeatures = forestData['features']

        # Immediately parallelizing it
        # Kept increasing the number of slices until this issue was gone: "21/11/05 13:13:56 WARN TaskSetManager: Stage 0 contains a task of very large size (1051 KiB). The maximum recommended task size is 1000 KiB."
        forestDataRDD = sc.parallelize(forestFeatures, numSlices=2000)

        # Caching this as it will be used twice
        forestDataRDD.cache()

        # ***************** Extracting the coordinates of each forest area *****************

        # Extracting the coordinates
        coordinates = forestDataRDD.map(get_coordinates)

        # Mapping each FOREST_COVER_ID/coord circle to its own row
        coordinates = coordinates.flatMapValues(flatteen_coordinates)

        # Indexing one layer deeper in the list
        coordinates = coordinates.map(convert_values)

        # Defining the schema of the data
        coordSchema = types.StructType([
            types.StructField('FOREST_COVER_ID', types.LongType()),
            types.StructField('coordinates', types.ArrayType(elementType=types.ArrayType(elementType=types.DoubleType()))),
        ])

        # Converting the RDD to a DF
        coordinatesDF = spark.createDataFrame(coordinates, schema=coordSchema)

        # Adding a unique identifier to each row since each FOREST_COVER_ID can have multiple polygons
        coordinatesDF = coordinatesDF.withColumn('polygonID', functions.monotonically_increasing_id())

        # CITATION: https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/
        coordinatesDFExpanded = coordinatesDF.withColumn("coordinates", functions.explode(coordinatesDF['coordinates']))

        # Creating separate columns for the long and lat
        coordinatesDFExpanded = coordinatesDFExpanded.withColumn('long', coordinatesDFExpanded['coordinates'][0]).withColumn('lat', coordinatesDFExpanded['coordinates'][1]).drop('coordinates')

        # ***************** Extracting the properties of each forest area *****************

        # Extracting the properties
        properties = forestDataRDD.map(get_properties)

        # Subsetting the properties
        propertiesSubset = properties.map(select_properties)

        propertiesSchema = types.StructType([
            types.StructField('FOREST_COVER_ID', types.LongType()),
            types.StructField('SILV_POLYGON_AREA', types.DoubleType()),
            types.StructField('REFERENCE_YEAR', types.StringType()),
            types.StructField('SITE_INDEX', types.IntegerType()),
            types.StructField('I_TOTAL_STEMS_PER_HA', types.IntegerType()),
            types.StructField('I_CROWN_CLOSURE_PERCENT', types.IntegerType()),
        ])

        propertiesDF = spark.createDataFrame(propertiesSubset, schema=propertiesSchema)

        # ***************** Writing the processed forest information to disk *****************

        if firstIteration:
            print('Overwriting')
            # Writing to disk
            coordinatesDFExpanded.write.json(output+"/coordinates",  mode='overwrite', compression='gzip')
            propertiesDF.write.csv(output + "/properties", mode='overwrite', header=False, compression='gzip')

            # Setting the indicator to be false so on the next file we won't overwrite
            firstIteration = False
        else:
            print('Appending')
            # Writing to disk
            coordinatesDFExpanded.write.json(output + "/coordinates", mode='append', compression='gzip')
            propertiesDF.write.csv(output + "/properties", mode='append', header=False, compression='gzip')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_data_preprocessing').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
