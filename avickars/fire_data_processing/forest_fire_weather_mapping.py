import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import numpy as np
from math import pi
import datetime


@functions.pandas_udf(returnType=types.DoubleType())
def haversine(lat1, lon1, lat2, lon2):
    p = pi / 180
    a = 0.5 - np.cos((lat2 - lat1) * p) / 2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
    return 12742 * np.arcsin(np.sqrt(a))


# @functions.udf(returnType=types.DateType())
# def convert_string_to_date(date):
#     return datetime.datetime.strptime(str(date), '%Y%m%d').date()


@functions.udf(returnType=types.ArrayType(elementType=types.DateType()))
def get_last_n_days(date, n):
    z = date
    previousDays = []
    for i in range(0, int(n)):
        z = z + datetime.timedelta(days=-1)
        previousDays.append(z)

    return previousDays


def main(weather_data, firePropertiesPath, fireCoordinatesPath, n, output):
    # ***************** Reading in the data *****************
    propertySchema = types.StructType([
        types.StructField('FIRE_NUMBER', types.StringType()),
        types.StructField('FIRE_YEAR', types.IntegerType()),
        types.StructField('FIRE_CAUSE', types.StringType()),
        types.StructField('FIRE_SIZE_HECTARES', types.DoubleType()),
        types.StructField('FIRE_DATE', types.DateType()),
        types.StructField('id', types.LongType()),
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

    # Expanding the fire coordinates to maximize parallelism
    fireCoordinatesExpanded = fireCoordinates.withColumn('coordinates', functions.explode(fireCoordinates['coordinates']))

    fireCoordinatesExpanded = fireCoordinatesExpanded.withColumn('long', fireCoordinatesExpanded['coordinates'][0]).withColumn('lat', fireCoordinatesExpanded['coordinates'][1]).drop('coordinates')

    # Computing the approximate center of the fire
    fireCenter = fireCoordinatesExpanded.groupby(fireCoordinatesExpanded['FIRE_NUMBER'], fireCoordinatesExpanded['id']).mean('lat', 'long').withColumnRenamed('avg(lat)', 'latCenter').withColumnRenamed('avg(long)', 'longCenter')

    # Merging the properties of each fire with its approximate center
    fireProperties.createOrReplaceTempView('fireProperties')
    fireCenter.createOrReplaceTempView('fireCenter')
    fires = spark.sql("""select FP.id,
                        FP.FIRE_NUMBER,
                        FE.latCenter,
                        FE.longCenter,
                        FP.FIRE_DATE
                        from fireProperties FP inner join fireCenter FE on FP.FIRE_NUMBER = FE.FIRE_NUMBER and FP.id = FE.id""")

    fires = fires.withColumn('previousDays', get_last_n_days(fires['FIRE_DATE'], functions.lit(n)))

    fires = fires.withColumn('previousDays', functions.explode(fires['previousDays']))

    # Defining the schema of the data
    stationWeather_schema = types.StructType([
        types.StructField('ID', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('MDPR', types.LongType()),
        types.StructField('MDSF', types.LongType()),
        types.StructField('PRCP', types.LongType()),
        types.StructField('SNOW', types.LongType()),
        types.StructField('SNWD', types.LongType()),
        types.StructField('TAVG', types.LongType()),
        types.StructField('TMAX', types.LongType()),
        types.StructField('TMIN', types.LongType()),
        types.StructField('WDFG', types.LongType()),
        types.StructField('WSFG', types.LongType()),
        types.StructField('LONGITUDE', types.DoubleType()),
        types.StructField('LATITUDE', types.DoubleType()),
    ])

    # Reading in the weather data
    weatherData = spark.read.csv(weather_data, header=False, schema=stationWeather_schema)

    # Removing Null values
    weatherData = weatherData.where(~weatherData['TMAX'].isNull())

    # Defining the SQL objects
    fires.createOrReplaceTempView('fires')
    weatherData.createOrReplaceTempView('weatherData')

    # Finding the weather stations for each fire that were collecting data for the n weeks leading up to the fire
    fireStationsAll = spark.sql("""select SSE.LONGITUDE,
                        SSE.LATITUDE,
                        SSE.ID,
                        F.id as fireID,
                        F.FIRE_NUMBER,
                        F.latCenter,
                        F.longCenter,
                        F.previousDays
                from weatherData SSE
                inner join fires F on SSE.date = F.previousDays""")

    # Computing the distance between the fire and the weather station for that specific date
    fireStationsAll = fireStationsAll.withColumn('distance', haversine(fireStationsAll['LATITUDE'], fireStationsAll['LONGITUDE'], fireStationsAll['latCenter'], fireStationsAll['longCenter']))

    # Computing the min distance between
    minDistancesToStations = fireStationsAll.groupby('FIRE_NUMBER', 'fireID', 'previousDays').min('distance').withColumnRenamed('min(distance)', 'minDistance')

    # Defining the SQL objects
    minDistancesToStations.createOrReplaceTempView('minDistancesToStations')
    fireStationsAll.createOrReplaceTempView('fireStationsAll')
    mapping = spark.sql("""select MDS.FIRE_NUMBER, MDS.fireID, FSA.ID, FSA.previousDays as previousFireDate
                from fireStationsAll FSA
                    inner join minDistancesToStations MDS on FSA.FIRE_NUMBER = MDS.FIRE_NUMBER and
                                                    FSA.fireID = MDS.fireID and
                                                    FSA.distance = MDS.minDistance and
                                                    FSA.previousDays = MDS.previousDays""")
    # Writing to disk
    mapping.write.csv(output+n, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    input_1 = sys.argv[1]
    input_2 = sys.argv[2]
    input_3 = sys.argv[3]
    input_4 = sys.argv[4]
    output = sys.argv[5]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_fire_counts').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_1, input_2, input_3, input_4,output)
