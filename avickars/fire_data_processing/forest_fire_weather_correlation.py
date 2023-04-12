import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf

def main(weather_data, firePropertiesPath, fireWeatherMapping, output):
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

    mappingSchema = types.StructType([
        types.StructField('FIRE_NUMBER', types.StringType()),
        types.StructField('fire_id', types.LongType()),
        types.StructField('ID', types.StringType()),
        types.StructField('previousFireDate', types.DateType())
    ])

    # Reading in the data set
    mapping = spark.read.csv(fireWeatherMapping, schema=mappingSchema)

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


    # Defining the SQL objects
    fireProperties.createOrReplaceTempView('fireProperties')
    weatherData.createOrReplaceTempView('weatherData')
    mapping.createOrReplaceTempView('mapping')

    # Computing the weather for each fire for the previous n days
    # ALso converting temps to celcius
    fireWeather = spark.sql("""select FP.FIRE_NUMBER,
                        FP.id as fireID,
                        W.date,
                           W.MDPR,
                            W.MDSF,
                            W.PRCP,
                            W.SNOW,
                            W.SNWD,
                            W.TAVG/10 as TAVG,
                            W.TMAX/10 as TMAX,
                            W.TMIN/10 as TMIN,
                            W.WDFG,
                            W.WSFG
                from fireProperties FP
                inner join mapping M on M.FIRE_NUMBER = FP.FIRE_NUMBER and
                                        M.fire_id = FP.id
                inner join weatherData W on W.ID = M.ID and
                            W.date = M.previousFireDate
                                        """)

    # Taking the mean of each day in case somehow a fire has multiple weather observations on the same day (extremely unlikely but you never know)
    fireWeather.createOrReplaceTempView('fireWeather')
    dailyMeans = spark.sql("""select FW.FIRE_NUMBER,
                    FW.fireID,
                    FW.date,
                           avg(FW.MDPR) as MDPR,
                            avg(FW.MDSF) as MDSF,
                           avg(FW.PRCP) as PRCP,
                           avg(FW.SNOW) as SNOW,
                            avg(FW.SNWD) as SNWD,
                            avg(FW.TAVG) as TAVG,
                            avg(FW.TMAX) as TMAX,
                            avg(FW.TMIN) as TMIN,
                            avg(FW.WDFG) as WDFG,
                            avg(FW.WSFG) as WSFG
                    from fireWeather FW
                    group by FW.FIRE_NUMBER, FW.fireID, FW.date
                            """)


    dailyMeans.createOrReplaceTempView('dailyMeans')
    means = spark.sql("""select FW.FIRE_NUMBER,
                    FW.fireID,
                           avg(FW.MDPR) as MDPR,
                            avg(FW.MDSF) as MDSF,
                           avg(FW.PRCP) as PRCP,
                           avg(FW.SNOW) as SNOW,
                            avg(FW.SNWD) as SNWD,
                            avg(FW.TAVG) as TAVG,
                            avg(FW.TMAX) as TMAX,
                            avg(FW.TMIN) as TMIN,
                            avg(FW.WDFG) as WDFG,
                            avg(FW.WSFG) as WSFG
                    from fireWeather FW
                    group by FW.FIRE_NUMBER, FW.fireID
                            """)

    # Coalescing to 1 so it can be vizualized
    means.coalesce(1).write.parquet(output, mode='overwrite')



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
