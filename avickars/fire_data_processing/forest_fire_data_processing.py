import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import json
import os
from datetime import datetime


def flatteen_coordinates(x): return x


def get_coordinates(forest):
    forestValues = forest[0]
    coordinates = forestValues['geometry']['coordinates']
    forestIdentifier = forestValues['properties']['FIRE_NUMBER']
    return forestIdentifier, coordinates, forest[1]


def convert_values(row):
    # Have the try/except statement here since some rows are 3 layers deep, while others are 4
    try:
        return row[0], [[float(i[0]), float(i[1])] for i in row[1][0]], row[2]
    except TypeError:
        return row[0], [[float(i[0]), float(i[1])] for i in row[1][0][0]], row[2]


def get_properties(forest):
    return forest[0]['properties'], forest[1]


def select_properties(properties):
    p = properties[0]
    props = []
    for property in ['FIRE_NUMBER', 'FIRE_YEAR', 'FIRE_CAUSE', 'FIRE_SIZE_HECTARES', 'FIRE_DATE']:
        try:
            if property == 'FIRE_SIZE_HECTARES' and p[property] is not None:
                props.append(float(p[property]))
            elif property == 'FIRE_DATE':
                # If we have a fire date we'll use it
                if 'FIRE_DATE' in p.keys():
                    if p[property] is not None:
                        # There were some inconstent date formats, just indexing to make them consistent
                        d = p[property][0:8]
                        date = datetime.strptime(d, '%Y%m%d')
                        props.append(date.date())
                    elif p['LOAD_DATE'] is not None:
                        date = datetime.strptime(p['LOAD_DATE'], '%Y%m%d%H%M%S')
                        props.append(date.date())
                    else:
                        props.append(None)

                # If we don't have a fire date, we'll use the load date instead
                elif p['LOAD_DATE'] is not None:
                    date = datetime.strptime(p['LOAD_DATE'], '%Y%m%d%H%M%S')
                    props.append(date.date())
                else:
                    props.append(None)
            else:
                props.append(p[property])
        except KeyError:
            props.append(None)

    props.append(properties[-1])
    return props


def main(input, output):
    # A indicator if this is the first file we are working on, used when writing to disk
    firstIteration = True

    # NOTE: I know that pyspark has its own json reading, unfortunately it either wants to read it into a dataframe (everything ends up in 1 row) or into an RDD row by row, neither case is sufficient, hence going this avenue.
    for file in os.listdir(inputs):
        print("****************************************************************************************************************************************************************************************************")
        print('Working on:', file)
        f = open(input + file, )

        # Reading in the entire file into memory (largest is about 5gb, so I can handle it)
        forestFiresRaw = json.load(f)

        # Closing the file
        f.close()

        # Immediately subsetting to the data we want
        forestFeatures = forestFiresRaw['features']

        # Immediately parallelizing it
        forestDataRDD = sc.parallelize(forestFeatures, numSlices=1000)

        # Adding a unique ID to each
        forestDataRDD = forestDataRDD.zipWithUniqueId()

        # Caching this as it will be used twice
        forestDataRDD.cache()

        # ***************** Extracting the coordinates of each forest fire *****************

        # Extracting the coordinates
        coordinates = forestDataRDD.map(get_coordinates)

        # Indexing one layer deeper in the list
        coordinates = coordinates.map(convert_values)

        # Defining the schema of the data
        coordSchema = types.StructType([
            types.StructField('FIRE_NUMBER', types.StringType()),
            types.StructField('coordinates', types.ArrayType(elementType=types.ArrayType(elementType=types.DoubleType()))),
            types.StructField('id', types.LongType()),
        ])

        # Converting the RDD to a DF
        # I know I could have some forest fires in coordinatesDF that aren't in propertiesDF, when I read it in forest_fire_partition_mapping.py, I filter them out with an inner join
        coordinatesDF = spark.createDataFrame(coordinates, schema=coordSchema)

        # Removing fires without coordinates
        coordinatesDF = coordinatesDF.where(~coordinatesDF['coordinates'].isNull())

        # ***************** Extracting the properties of each forest fire *****************

        # Extracting the properties
        properties = forestDataRDD.map(get_properties)

        # Subsetting the properties
        propertiesSubset = properties.map(select_properties)

        # Defining the schema of the data
        schema = types.StructType([
            types.StructField('FIRE_NUMBER', types.StringType()),
            types.StructField('FIRE_YEAR', types.IntegerType()),
            types.StructField('FIRE_CAUSE', types.StringType()),
            types.StructField('FIRE_SIZE_HECTARES', types.DoubleType()),
            types.StructField('FIRE_DATE', types.DateType()),
            types.StructField('id', types.LongType())
        ])

        # Converting to a dataframe
        propertiesDF = spark.createDataFrame(propertiesSubset, schema=schema)

        # Filtering out properties that don't have a fire_data
        propertiesDF = propertiesDF.where(~propertiesDF['FIRE_DATE'].isNull())

        # ***************** Writing the processed forest information to disk *****************

        if firstIteration:
            print('Overwriting')
            # Writing to disk
            coordinatesDF.write.json(output + "/coordinates", mode='overwrite', compression='gzip')
            propertiesDF.write.csv(output + "/properties", mode='overwrite', header=False, compression='gzip')

            # Setting the indicator to be false so on the next file we won't overwrite
            firstIteration = False
        else:
            print('Appending')
            # Writing to disk
            coordinatesDF.write.json(output + "/coordinates", mode='append', compression='gzip')
            propertiesDF.write.csv(output + "/properties", mode='append', header=False, compression='gzip')





if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_fire_counts').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
