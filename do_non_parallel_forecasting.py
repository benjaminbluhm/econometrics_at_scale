from fit_model_and_forecast import fit_model_and_forecast
from datetime import datetime

def do_non_parallel_forecasting(spark_session, config):

    # Load time series data into Spark dataframe
    df = spark_session.read.parquet(config['path_training_data_parquet'])

    # Create RDD with dictinct IDs
    time_series_ids = df.select("ID").distinct().rdd

    # Create Python list with dictinct IDs
    time_series_ids = [int(i.ID) for i in time_series_ids.collect()]

    print('start training')
    # Perform non-parallel forecasting
    time_start_training = datetime.now()
    fit_model_and_forecast(time_series_ids, config)
    time_training = datetime.now() - time_start_training
    print('time training: ' + str(time_training))
