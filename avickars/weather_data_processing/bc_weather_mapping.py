import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import numpy as np
from math import cos, asin, sqrt, pi
import datetime


@functions.pandas_udf(returnType=types.DoubleType())
def haversine(lat1, lon1, lat2, lon2):
    # Haversine implementation
    # CITATION: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
    p = pi / 180
    a = 0.5 - np.cos((lat2 - lat1) * p) / 2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
    return 12742 * np.arcsin(np.sqrt(a))


def main(weather_data, squares, output):
    # ************************************************************** Creating the date dataframe that holds all dates we need data for for each partition **************************************************************
    # **************** Reading in the weather data with the coordinates **************************

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

    # Reducing the amount of data
    # weatherData = weatherData.where(weatherData['date'] >= datetime.date(day=1, year=1980, month=1))

    # Dropping rows that don't have the data we want
    weatherData = weatherData.where(~weatherData['TMAX'].isNull())

    # Selecting only the columns we need
    weatherData = weatherData.select(weatherData['ID'], weatherData['date'], weatherData['LONGITUDE'], weatherData['LATITUDE'])

    # Caching this as it will be used several times
    weatherData.cache()

    # Extracting the min and max weather dates
    start = weatherData.sort('date', ascending=True).select(weatherData['date']).limit(1).collect()[0][0]
    end = weatherData.sort('date', ascending=False).select(weatherData['date']).limit(1).collect()[0][0]

    # **************** Creating Dataframe that contains all the dates from the 1st weather date to the last **************************
    dates = []
    dates.append(
        (start,)
    )
    while start <= end:
        start += datetime.timedelta(days=1)
        dates.append((start,))

    # Defining the schema for this DF
    dateSchema = types.StructType([
        types.StructField('date', types.DateType()),
    ])

    # Creating the dataframe
    dateDF = spark.createDataFrame(data=dates, schema=dateSchema)

    # Broadcasting this as it is literally just a column of dates.
    dateDF = functions.broadcast(dateDF)

    # ************************************************************** Reading the partitions **************************************************************

    # Defining the schema of the data
    squaresSchema = types.StructType([
        types.StructField('longLeft', types.DoubleType()),
        types.StructField('longRight', types.DoubleType()),
        types.StructField('latBottom', types.DoubleType()),
        types.StructField('latTop', types.DoubleType()),
        types.StructField('squareID', types.LongType())
    ])

    # Reading in the squares data
    squaresDFWithIdentifier = spark.read.csv(squares, header=False, schema=squaresSchema)

    # ************************************************************** Determining which partitions contain a weather station, and if not, finding the closest **************************************************************

    # **************** Extracting squares that have a weather station inside them and which don't *************************

    squaresForAllDates = squaresDFWithIdentifier.withColumn('centerLat', (squaresDFWithIdentifier['latBottom'] + squaresDFWithIdentifier['latTop']) / 2)
    squaresWithCenter = squaresForAllDates.withColumn('centerLong', (squaresForAllDates['longLeft'] + squaresForAllDates['longRight']) / 2)

    # Broadcasting as I know this dataframe is quite small (i.e. I control how many squares there are, about 50ish)
    squaresDFBroadcasted = functions.broadcast(squaresWithCenter)

    # Cross joining with the dates to add every date to every square
    # Adding the weather dates for every square
    squaresForAllDates = squaresDFBroadcasted.crossJoin(dateDF)

    # Defining this df as a sql object
    squaresForAllDates.createOrReplaceTempView('squaresForAllDates')
    weatherData.createOrReplaceTempView('weatherData')

    # Joining on the weather data
    squaresAndWeather = spark.sql("""
                                    select
                                        S.centerLat,
                                        S.centerLong,
                                        S.squareID,
                                        S.date,
                                        W.ID,
                                        W.LONGITUDE,
                                        W.LATITUDE
                                    from
                                        squaresForAllDates S
                                    inner join weatherData W on
                                        W.date = S.date
                                    """)

    squaresAndWeather = squaresAndWeather.withColumn('distance',
                                                     haversine(squaresAndWeather['centerLat'],
                                                               squaresAndWeather['centerLong'],
                                                               squaresAndWeather['LATITUDE'],
                                                               squaresAndWeather['LONGITUDE'])
                                                     )

    minDistances = squaresAndWeather.groupby('squareID','date').agg(functions.min('distance').alias('minDistance'))

    # Defining this df as a sql object
    minDistances.createOrReplaceTempView('minDistances')
    squaresAndWeather.createOrReplaceTempView('squaresAndWeather')

    # Filtering the squares/date/stations to the squares/dates/stations where the stations are the closest to each square center
    mapping = spark.sql("""select SAW.squareID, 
                                    SAW.date, 
                                    SAW.ID 
                            from squaresAndWeather SAW 
                                inner join minDistances MD on SAW.distance = MD.minDistance and 
                                                        MD.squareID = SAW.squareID and 
                                                        MD.date = SAW.date""")

    # #
    # # Caching as it is used twice
    # squaresAndWeather.cache()
    #
    # # Extracting for every square and date, determining which weather stations to use for that station.
    # squaresWithWeather = squaresAndWeather.select(squaresAndWeather['squareID'], squaresAndWeather['date'], squaresAndWeather['ID']).filter(~squaresAndWeather['weatherDate'].isNull())
    #
    # # Extracting the squares that don't have a weather station inside them
    # squaresNoWeather = squaresAndWeather.drop('ID', 'weatherDate').dropDuplicates().filter(squaresAndWeather['weatherDate'].isNull())
    #
    # # **************** For the squares that have no weather stations, finding the closest station to use *************************
    #
    # # Defining this df as a sql object
    # squaresForAllDates.createOrReplaceTempView('squaresForAllDates')
    #
    # # Defining this df as a sql object
    # squaresNoWeather.createOrReplaceTempView('squaresNoWeather')
    #
    # # For each square/date, attaching weather stations that have weather for that date
    # squaresNoWeatherWithOptions = spark.sql("""select SW.*, W.ID, W.LONGITUDE, W.LATITUDE from squaresNoWeather SW inner join weatherData W on SW.date = W.date""")
    #
    # # Determining the center of each square
    # squaresNoWeatherWithOptions = squaresNoWeatherWithOptions.withColumn('centerLat', (squaresNoWeatherWithOptions['latBottom'] + squaresNoWeatherWithOptions['latTop']) / 2)
    # squaresNoWeatherWithOptions = squaresNoWeatherWithOptions.withColumn('centerLong', (squaresNoWeatherWithOptions['longLeft'] + squaresNoWeatherWithOptions['longRight']) / 2)
    #
    # # Computing the distance between each square center and station
    # squaresNoWeatherWithDistances = squaresNoWeatherWithOptions.withColumn('distance',
    #                                                                        haversine(squaresNoWeatherWithOptions['centerLat'],
    #                                                                                  squaresNoWeatherWithOptions['centerLong'],
    #                                                                                  squaresNoWeatherWithOptions['LATITUDE'],
    #                                                                                  squaresNoWeatherWithOptions['LONGITUDE'])
    #                                                                        )
    #
    # # Finding the closest weather station for each square/date
    # closestStationDistance = squaresNoWeatherWithDistances.groupby('squareID', 'date').min('distance')
    #
    # # Renaming the minimum distance
    # closestStationDistance = closestStationDistance.withColumnRenamed('min(distance)', 'minDistance')
    #
    # # # Defining this df as a sql object
    # closestStationDistance.createOrReplaceTempView('closestStationDistance')
    #
    # # Defining this df as a sql object
    # squaresNoWeatherWithDistances.createOrReplaceTempView('squaresNoWeatherWithDistances')
    #
    # # Filtering the squares/date/stations to the squares/dates/stations where the stations are the closest to each square center
    # squaresNoWeatherWithWeather = spark.sql(
    #     """select SNWD.squareID, SNWD.date, SNWD.ID from squaresNoWeatherWithDistances SNWD inner join closestStationDistance CSD on SNWD.distance = CSD.minDistance""")
    #
    # # **************** Union the squares that had weather stations with squares that didn't (now using station that is closest) *************************
    # squaresWithStation = squaresWithWeather.union(squaresNoWeatherWithWeather)
    #
    # squaresWithStation = squaresWithStation.repartition(16)
    #
    # Writing to disk
    mapping.write.csv(output, mode='overwrite', header=True,
                                 compression='gzip')


if __name__ == '__main__':
    weather_data = sys.argv[1]
    squares = sys.argv[2]
    output = sys.argv[3]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(weather_data, squares, output)
