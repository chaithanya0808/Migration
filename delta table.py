def insert_batch_details_statistics(
    batch_id: str,
    input_df: DataFrame,
    batch_start_time: datetime,
    event: str,
) -> None:
    """
    Inserts statistics into the Audit Delta table.

    :param batch_id: Equivalent to job_id coming from Airflow DAG.
    :param input_df: The input DataFrame that contains stats (Map<String, String>)
                     and an additional column.
    :param batch_start_time: The timestamp when the batch started running.
    :param event: The event for which this update is being run (e.g., CASE | ALERT | SMR).
    """
    table_path = f"{FCEM_FC_S3_DELTA_TABLE_PATH}/batch_details_statistics"
    spark = get_or_create_spark_session()

    # Add additional required column to the Delta table
    DeltaTable.forPath(spark, table_path).alias("target").toDF() \
        .withColumn("new_column_name", F.lit(None).cast("String")) \
        .write \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .format("delta") \
        .save(table_path)

    # Add required columns to the source_df
    source_df = (
        input_df.withColumn("batch_start_time", F.lit(batch_start_time))
        .withColumn("batch_end_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processing_id", F.monotonically_increasing_id() + F.unix_timestamp())
        .withColumn("event", F.lit(event))
    )

    merge_condition = (
        "target.batch_id = source.batch_id AND "
        "target.processing_id = source.processing_id AND "
        "target.event = '{event}'"
    )

    DeltaTable.forPath(spark, table_path) \
        .alias("target") \
        .merge(source_df.alias("source"), merge_condition) \
        .whenNotMatchedInsertAll() \
        .whenMatchedUpdateAll() \
        .execute()
