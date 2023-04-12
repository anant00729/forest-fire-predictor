import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf


def main(inputs, output):
    # *************** Processing the coordinates of the BC border **************************************

    # Reading in the data set
    bcBorderRaw = spark.read.option("multiline", "true").json(inputs)

    bcBorder = bcBorderRaw.select(bcBorderRaw['features'])

    geometry = bcBorder.select(bcBorder['features.geometry'])

    coordinates = geometry.select(geometry['geometry.coordinates'])

    # The coordinates are contained in a recursive list that is 3 layers deep, so incrementing to them to access the list
    # CITATION: https://stackoverflow.com/questions/47585279/how-to-access-values-in-array-column
    innerCoordinates = coordinates.withColumn("coordinates2", functions.element_at("coordinates", 1)).drop("coordinates")
    innerCoordinates = innerCoordinates.withColumn("coordinates3", functions.element_at("coordinates2", 1)).drop("coordinates2")
    # innerCoordinates = innerCoordinates.withColumn("coordinates4", functions.element_at("coordinates3", 1)).drop("coordinates3")

    # Exploding the list of lists into rows for each element
    # CITATION: https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/
    coordinatesExpanded = innerCoordinates.withColumn("coordinates", functions.explode(innerCoordinates['coordinates3'])).drop("coordinates3")

    # Expanding the lat and long into separate columns
    longLat = coordinatesExpanded.select(coordinatesExpanded['coordinates'][0], coordinatesExpanded['coordinates'][1]).withColumnRenamed('coordinates[0]', 'long').withColumnRenamed('coordinates[1]',
                                                                                                                                                                                     'lat')
    # Writing to disk
    longLat.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_data_preprocessing').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
