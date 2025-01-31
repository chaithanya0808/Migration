def generate_details_stats_df(self, input_df: DataFrame) -> DataFrame:
    # Add correlation_Id column to the input DataFrame
    df_with_correlation_id = input_df.withColumn("correlation_Id", F.col("value.metadata.correlationId"))
    
    # Select the required fields from "value.fcCase.*" and the correlation_Id column
    interim_df = df_with_correlation_id.select(
        F.col("value.fcCase.caseId").alias(CASE_ID_CONST),
        F.col("value.fcCase.caseVersion").alias(VERSION_ID_CONST),
        F.size(F.coalesce(F.col("value.fcCase.fcCasePartyRelationship"), F.array())).alias("PARTY_RELATIONSHIP_SIZE"),
        F.size(F.coalesce(F.col("value.fcCase.fcCaseCaseRelationship"), F.array())).alias("CASE_RELATIONSHIP_SIZE"),
        F.size(F.coalesce(F.col("value.fcCase.fcCaseAlertRelationship"), F.array())).alias("ALERT_RELATIONSHIP_SIZE"),
        F.col("correlation_Id")  # Include the correlation_Id column added earlier
    )
    
    # Create the final DataFrame
    return interim_df.select(
        get_map_expr(interim_df).alias("stats"),
        "correlation_Id"
    )
