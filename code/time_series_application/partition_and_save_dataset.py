def partition_and_save_dataset(spark_session, config):

    df = spark_session.read.option("header", "true")\
        .csv(config['path_training_data_csv'], inferSchema=True)

    df.repartition("ID").write.option("compression", "gzip").mode("overwrite")\
        .partitionBy("ID").parquet(config['path_training_data_parquet'])
