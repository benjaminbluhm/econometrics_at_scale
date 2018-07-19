# Import Python modules
from create_spark_session import create_spark_session
from create_config import create_config
from partition_and_save_dataset import partition_and_save_dataset
from do_non_parallel_forecasting import do_non_parallel_forecasting
from do_parallel_forecasting import do_parallel_forecasting


def main():

    # Create Spark session
    spark_context, spark_session = create_spark_session()

    # Load config dictionary
    config = create_config()

    # Partition and save dataset in Parquet file format to S3
    # partition_and_save_dataset(spark_session, config)

    # Perform non-parallel model fitting and forecasting
    do_non_parallel_forecasting(spark_session, config)

    # # Add Python module to Spark context for distributed model fitting and forecasting
    # spark_context.addPyFile(config['base_path_hadoop'] + 'fit_model_and_forecast.py')
    #
    # # Perform parallel model fitting and forecasting
    # do_parallel_forecasting(spark_session, config)

if __name__ == '__main__':
    main()