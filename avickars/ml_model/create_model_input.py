import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
import datetime
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array


@functions.udf(returnType=types.ArrayType(elementType=types.DateType()))
def get_last_two_weeks(date):
    dates = []
    for i in range(0, 14):
        date -= datetime.timedelta(days=1)
        dates.append(date)
    return dates


def main(wweather_mapping, weather_data, firePropertiesPath, fireMappingPath, output):
    # **************** Reading in the data **************************

    # Defining the schema of the data
    weatherMappingSchema = types.StructType([
        types.StructField('squareID', types.LongType()),
        types.StructField('date', types.DateType()),
        types.StructField('ID', types.StringType())
    ])

    # Reading in the weather data
    weatherMapping = spark.read.csv(wweather_mapping, header=True, schema=weatherMappingSchema)

    # Caching as it will be used multiple times
    weatherMapping.cache()

    # Defining the schema of the data
    weatherDataSchema = types.StructType([
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
    weatherData = spark.read.csv(weather_data, header=False, schema=weatherDataSchema)

    # Selecting the columns we want
    weatherData = weatherData.select(weatherData['ID'], weatherData['date'], weatherData['TMAX'], weatherData['PRCP'])

    # Converting TMAX to celcius
    weatherData = weatherData.withColumn('TMAX', weatherData['TMAX'] / functions.lit(10))

    fireMappingSchema = types.StructType([
        types.StructField('squareID', types.LongType()),
        types.StructField('FIRE_NUMBER', types.StringType()),
        types.StructField('id', types.LongType()),
        types.StructField('FIRE_YEAR', types.IntegerType()),
        types.StructField('FIRE_DATE', types.DateType()),
        types.StructField('FIRE_SIZE_HECTARES', types.DoubleType()),
    ])

    # Reading in the data
    fireMapping = spark.read.csv(fireMappingPath, header=True, schema=fireMappingSchema)

    # Selecting only the columns we need
    fireMapping = fireMapping.select(fireMapping['squareID'], fireMapping['FIRE_NUMBER'], fireMapping['id'], fireMapping['FIRE_DATE'])

    # **************** Getting the last 14 days of each day we want to look at **************************

    # Extracting the last 14 days to use in the model
    lastTwoWeeks = weatherMapping.withColumn('lastTwoWeeks', get_last_two_weeks(weatherMapping['date'])).drop('ID')

    # Exploding each day into a separate row so I can get the weather using the mapping
    lastTwoWeeks = lastTwoWeeks.withColumn('lastTwoWeeks', functions.explode(lastTwoWeeks['lastTwoWeeks']))

    lastTwoWeeks.createOrReplaceTempView('lastTwoWeeks')
    weatherMapping.createOrReplaceTempView('weatherMapping')
    weatherData.createOrReplaceTempView('weatherData')
    fireMapping.createOrReplaceTempView('fireMapping')

    modelInput = spark.sql("""select LTW.squareID,
                        LTW.date,
                        LTW.lastTwoWeeks,
                        WD.TMAX,
                        WD.PRCP
                from lastTwoWeeks LTW
                inner join weatherMapping WM on LTW.lastTwoWeeks = WM.date and LTW.squareID = WM.squareID
                inner join weatherData WD on WM.ID = WD.ID and WM.date = WD.date
                left join fireMapping FM on FM.squareID = LTW.squareID and LTW.lastTwoWeeks = FM.FIRE_DATE""")

    # Filling the PRCP nulls with zeros
    modelInput = modelInput.fillna(0)

    modelInput.createOrReplaceTempView('modelInput')

    modelInputAveraged = spark.sql("""select squareID, date, avg(TMAX) as TMAX, avg(PRCP) as PRCP from modelInput group by squareID, date""")

    modelInputAveraged.createOrReplaceTempView('modelInputAveraged')

    modelInputWithTarget = spark.sql("""select MIA.*,
                                                dayofyear(MIA.date) as dayOfYear,
                                                case when isNull(FM.FIRE_DATE) then 0 else 1 end as hasFire
                                            from modelInputAveraged MIA
                                             left join fireMapping FM on FM.squareID = MIA.squareID and MIA.date = FM.FIRE_DATE""")

    modelInputWithTarget.write.parquet(output, mode='overwrite', compression='gzip')



if __name__ == '__main__':
    weather_mapping = sys.argv[1]
    weather_data = sys.argv[2]
    fire_mapping = sys.argv[3]
    fire_properties = sys.argv[4]
    output = sys.argv[5]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(weather_mapping, weather_data, fire_mapping, fire_properties, output)
