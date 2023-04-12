import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, functions
from pyspark import SparkConf
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import datetime


def main(modelInput, modelOutput, modelSave):
    # **************** Reading in the data **************************
    # Defining the schema
    schema = types.StructType([
        types.StructField('squareID', types.LongType()),
        types.StructField('date', types.DateType()),
        types.StructField('TMAX', types.DoubleType()),
        types.StructField('PRCP', types.DoubleType()),
        types.StructField('dayOfYear', types.IntegerType()),
        types.StructField('hasFire', types.IntegerType()),
    ])

    # Reading in the data
    data = spark.read.parquet(modelInput, schema=schema)

    # Used multiple times
    data.cache()

    vizData = data.where(data['date'] >= datetime.date(day=1, month=1, year=2020))

    # Balancing out the data
    dataHasFires = data.where(data['hasFire'] == 1)
    dataNoFires = data.where(data['hasFire'] == 0)


    # Only taking 5% of the data that doesn't have a fire
    dataNoFires = dataNoFires.sample(fraction=0.05, seed=1)

    modelInput = dataNoFires.union(dataHasFires)

    # **************** Training Model **************************

    train, validation = modelInput.randomSplit([0.75, 0.25], seed=2)

    assemble_features = VectorAssembler(
        inputCols=['TMAX', 'PRCP', 'dayOfYear'],
        outputCol='features')

    standard_scaler = StandardScaler(inputCol='features', outputCol='features_scaled')

    classifier = LogisticRegression(maxIter=10, featuresCol='features', labelCol='hasFire', regParam=0.0001)

    thePipeline = Pipeline(stages=[assemble_features, standard_scaler, classifier])

    model = thePipeline.fit(train)

    evaluator = BinaryClassificationEvaluator(labelCol='hasFire', rawPredictionCol='prediction')

    predictions_train = model.transform(train)
    print('Validation Results:', evaluator.evaluate(predictions_train))

    predictions_valid = model.transform(validation)
    print('Validation Results:', evaluator.evaluate(predictions_valid))

    predictions_viz = model.transform(vizData)

    predictions_viz = predictions_viz.withColumn('probability', vector_to_array(predictions_viz['probability']))

    predictions_viz = predictions_viz.withColumn('prob', predictions_viz['probability'][0])

    predictions_viz.printSchema()

    predictions_viz.createOrReplaceTempView('predictions_viz')

    vizResults = spark.sql("""select squareID,
                            date,
                            case when prob <= 0.75 then 'High Risk'  when prob <=0.8 and prob>0.75 then 'Medium Risk' else 'Low Risk' end as Risk
                from predictions_viz""")

    # Coalescing to pick up in the dashboard
    vizResults.coalesce(1).write.parquet(modelOutput, mode='overwrite')

    # Saving Model
    model.save(modelSave)


if __name__ == '__main__':
    # spark = SparkSession.builder.config(conf=conf).appName('weather etl').getOrCreate()
    spark = SparkSession.builder.appName('bc_border_partitioning').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    outputs_1 = sys.argv[2]
    outputs_2 = sys.argv[3]
    main(inputs, outputs_1, outputs_2)
