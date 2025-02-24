def get_modified_object_name(object_name: str, party_type: str) -> str:
    """
    Modifies the object_name based on party_type.
    
    :param object_name: The original object name.
    :param party_type: The type of party.
    :return: Modified object name.
    """
    if party_type == "medfin_advantage":
        return f"{object_name}_medfin_advantage"
    return object_name



upsert_job_execution_delta(
    object_name=get_modified_object_name(object_name, party_type),
    job_id=self.job_id,
    status=AuditStatus.RUNNING.value,
    error=None,
    start_time=F.current_timestamp(),
)
