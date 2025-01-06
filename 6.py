df_statistics = df_to_ser.withColumn(
    "alert_id", F.expr("transform(value.fcAlertTransactionRelationship, x -> x.alertId)[0]")
).withColumn(
    "transactions", F.expr("transform(value.fcAlertTransactionRelationship, x -> x.transactionId)")
).withColumn(
    "txn_count", F.size(F.col("transactions"))
).select(
    "alert_id", "transactions", "txn_count"
)

# Display the result
df_statistics.show(1000, truncate=False)
df_statistics.count()
