# Import Python modules

# Import PySpark modules
import findspark
findspark.init()
from pyspark.sql import SparkSession

def create_spark_session():

    spark_session = SparkSession.builder.appName('econometrics_at_scale').getOrCreate()

    return spark_session