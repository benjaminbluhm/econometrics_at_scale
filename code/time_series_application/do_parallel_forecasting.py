def do_parallel_forecasting(spark_session, config, cloud=False):

    # Load time series data into Spark dataframe
    df = spark_session.read.parquet(config['path_training_data_parquet'])

    # Create RDD with dictinct IDs and repartition dataframe into 100 chunks
    time_series_ids = df.select("ID").distinct().repartition(100).rdd

    # Function to import model Python module on Spark executor for parallel forecasting
    def import_module_on_spark_executor(time_series_ids, config, cloud=False):
        from fit_model_and_forecast import fit_model_and_forecast
        return fit_model_and_forecast(time_series_ids, config, cloud=cloud)

    # Parallel model fitting and forecasting
    time_series_ids.foreach(lambda x: import_module_on_spark_executor(x, config, cloud=cloud))



