from fit_model_and_forecast import fit_model_and_forecast

def do_non_parallel_forecasting(spark_session, config):

    # Load time series data into Spark dataframe
    df = spark_session.read.parquet(config['path_training_data_parquet'])

    # Create RDD with dictinct IDs
    time_series_ids = df.select("ID").distinct().rdd

    # Create Python list with dictinct IDs
    time_series_ids = [int(i.ID) for i in time_series_ids.collect()]

    # Perform non-parallel forecasting
    fit_model_and_forecast(time_series_ids, config)
