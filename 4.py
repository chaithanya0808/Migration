from pyspark.sql import functions as F

# Explode the fcAlertTransactionRelationship array
result_df = df_to_ser.select(
    F.col("key.id").alias("message_id"),
    F.explode("value.fcAlertTransactionRelationship").alias("relationship")
).select(
    F.col("message_id"),
    F.col("relationship.alertId").alias("alertId"),
    F.col("relationship.transactionId").alias("transactionId")
).groupBy("message_id", "alertId").agg(
    F.collect_list("transactionId").alias("transactions")
).withColumn(
    "txn_count", F.size(F.col("transactions"))
).select(
    F.col("alertId"),
    F.col("transactions"),
    F.col("txn_count")
)

# Display the result
result_df.show(truncate=False)
