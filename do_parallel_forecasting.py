from datetime import datetime

def do_parallel_forecasting(spark_session, config):

    # Load time series data into Spark dataframe
    df = spark_session.read.parquet(config['path_training_data_parquet'])

    # Create RDD with dictinct IDs and repartition dataframe into 250 chunks
    time_series_ids = df.select("ID").distinct().repartition(33).rdd

    # Function to import model Python module on Spark executor for parallel forecasting
    def import_module_on_spark_executor(time_series_ids, config):
        from fit_model_and_forecast import fit_model_and_forecast
        return fit_model_and_forecast(time_series_ids, config)

    print('start training')
    # Parallel model fitting and forecasting
    time_start_training = datetime.now()
    time_series_ids.foreach(lambda x: import_module_on_spark_executor(x, config))
    time_training = datetime.now() - time_start_training
    print('time training: ' + str(time_training))



