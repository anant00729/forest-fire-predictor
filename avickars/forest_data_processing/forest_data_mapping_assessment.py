import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf


def main(input, output):
    # ***************** Reading in data sets *****************
    schema = types.StructType([
        types.StructField('squareID', types.LongType()),
        types.StructField('FOREST_COVER_ID', types.LongType()),
    ])

    # Reading in the data
    mapping = spark.read.csv(input, schema=schema)

    # Defining as an SQL object
    mapping.createOrReplaceTempView('mapping')

    # Determining if each square even has forest data
    mappingCheck = spark.sql("""select distinct squareID, case when ISNULL(FOREST_COVER_ID) then 'Has data' else 'No data' end as hasForestData from mapping""")

    # Coalescing to 1 so it can be vizualized, and writing to disk
    mappingCheck.coalesce(1).write.parquet(output, mode='overwrite')






if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]

    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('forest_data_preprocessing').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
