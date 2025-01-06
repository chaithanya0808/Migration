from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Explode the fcAlertTransactionRelationship array
exploded_df = df_to_ser.select(
    F.col("key.id").alias("message_id"),
    F.explode("value.fcAlertTransactionRelationship").alias("relationship")
).select(
    F.col("message_id"),
    F.col("relationship.alertId").alias("alertId"),
    F.col("relationship.transactionId").alias("transactionId")
)

# Create a group ID for each alertId based on transactionId ordering
window_spec = Window.partitionBy("alertId").orderBy("transactionId")
grouped_df = exploded_df.withColumn(
    "group",
    F.rank().over(window_spec) - F.dense_rank().over(window_spec)
)

# Group transactions based on alertId and group
result_df = grouped_df.groupBy("alertId", "group").agg(
    F.collect_list("transactionId").alias("transactions"),
    F.count("transactionId").alias("txn_count")
).select(
    "alertId",
    "transactions",
    "txn_count"
)

# Display the result
result_df.show(truncate=False)
