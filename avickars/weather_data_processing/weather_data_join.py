from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types, functions
import sys
import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def parsing_station_info(line):
    slices = [(0, 11), (12, 20), (21, 30), (32, 37), (38, 40), (41, 71), (73, 75), (76, 79), (80, 85)]
    lineSplit = [line[slice(*slc)] for slc in slices]
    return lineSplit


def convert_data_types(station):
    return station[0], float(station[1]), float(station[2]), float(station[3]), station[4], station[5], station[6], station[7], station[8],


@functions.udf(returnType=types.DateType())
def convert_string_to_date(date):
    return datetime.datetime.strptime(str(date), '%Y%m%d').date()


def main(input, output):
    # **************** Reading in the station information to get the longitude and latitude of each station ************************
    # Reading in the data
    text = sc.textFile('ghcnd-stations.txt')

    # Repartitioning as it was read into only 2 partitions, this caused the program to fail
    text = text.repartition(16)

    # Parsing the text into a usable form
    stationInfo = text.map(parsing_station_info)
    bcStations = stationInfo.filter(lambda station: station[4] == 'BC')
    bcStationsConverted = bcStations.map(convert_data_types)

    # Defining the schema of the data
    stationInfo_schema = types.StructType([
        types.StructField('ID', types.StringType()),
        types.StructField('LATITUDE', types.DoubleType()),
        types.StructField('LONGITUDE', types.DoubleType()),
        types.StructField('ELEVATION', types.DoubleType()),
        types.StructField('STATE', types.StringType()),
        types.StructField('NAME', types.StringType()),
        types.StructField('GSN_FLAG', types.StringType()),
        types.StructField('HCN/CRN_FLAG', types.StringType()),
        types.StructField('WMO_ID', types.StringType()),
    ])

    # Converting the RDD to a dataframe
    bcStationsDF = spark.createDataFrame(bcStationsConverted, schema=stationInfo_schema)

    # Removing stations that have no longitude or latitude
    bcStationsDFFiltered = bcStationsDF.na.drop(subset=['LATITUDE', 'LONGITUDE'], how='any')

    # Selecting only the columns we need
    bcStationsLocation = bcStationsDFFiltered.select(bcStationsDFFiltered['ID'], bcStationsDFFiltered['LONGITUDE'], bcStationsDFFiltered['LATITUDE'])

    # **************** Reading in the daily weather ************************

    # Defining the schema of the data
    stationWeather_schema = types.StructType([
        types.StructField('ID', types.StringType()),
        types.StructField('date', types.IntegerType()),
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
    ])

    # Reading in the data
    weather = spark.read.csv(input, header=False, schema=stationWeather_schema)

    # Filtering to the year with the earliest recorded fire
    weather = weather.filter(weather['date'] >= 19180000)

    # **************** Joining the two data sets ************************
    # Defining this df as a sql object
    weather.createOrReplaceTempView('weather')
    bcStationsLocation.createOrReplaceTempView('bcStationsLocation')

    # Joing the data
    weatherDataComplete = spark.sql("""select W.*, L.LONGITUDE, L.LATITUDE from weather W inner join bcStationsLocation L on W.ID = L.ID""")

    # Converting date to a datetime object
    weatherDataComplete = weatherDataComplete.withColumn('date', convert_string_to_date(weatherDataComplete['date']))

    # Writing to disk
    weatherDataComplete.write.csv(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather_data_join.py').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
