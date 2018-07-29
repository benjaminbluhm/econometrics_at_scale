def create_config():

    config = {}

    # Define Path
    config['base_path_hadoop'] = '/home/hadoop/spark_parallel_forecasting/'
    config['base_path_s3'] = 's3://data-folders/spark_parallel_forecasting/'
    config['path_training_data_csv'] = config['base_path_s3'] + 'training_data/csv/rawdata.csv'
    config['path_training_data_parquet'] = config['base_path_s3'] + 'training_data/parquet/'
    config['path_forecasts'] = config['base_path_s3'] + 'forecasts/'
    config['path_models'] = config['base_path_s3'] + 'fitted_models/'

    # Define AWS S3 endpoint for your region
    config['s3_host'] = 's3.eu-central-1.amazonaws.com'

    # Define series and evaluation lengths
    config['len_series'] = 1000
    config['len_eval'] = 50

    return config