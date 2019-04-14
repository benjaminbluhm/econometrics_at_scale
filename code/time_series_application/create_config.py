def create_config(cloud=False):

    config = {}

    if cloud:
        # Define Path
        config['base_path_hadoop'] = '/home/hadoop/spark_parallel_forecasting/'
        config['base_path_data'] = 's3://data-folders/spark_parallel_forecasting/'
        config['path_training_data_csv'] = config['base_path_data'] + 'training_data/csv/rawdata.csv'
        config['path_training_data_parquet'] = config['base_path_data'] + 'training_data/parquet/'
        config['path_forecasts'] = config['base_path_data'] + 'forecasts/'
        config['path_models'] = config['base_path_data'] + 'fitted_models/'
    
        # Define AWS S3 endpoint for your region
        config['s3_host'] = 's3.eu-central-1.amazonaws.com'
    else:
        # Define Path
        config['base_path_hadoop'] = 'code/time_series_application/'
        config['base_path_data'] = 'data/time_series_application/'
        config['path_training_data_csv'] = config['base_path_data'] + 'csv/rawdata_small.csv'
        config['path_training_data_parquet'] = config['base_path_data'] + 'parquet/'
        config['path_forecasts'] = config['base_path_data'] + 'forecasts/'
        config['path_models'] = config['base_path_data'] + 'fitted_models/'

    # Define series and evaluation lengths
    config['len_series'] = 1000
    config['len_eval'] = 50

    return config