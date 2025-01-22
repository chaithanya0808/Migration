BEGIN;

DO $$ 
DECLARE
    v_rows_updated BIGINT := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    -- Record start time
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting FC_CASE update at %', v_start_time;

    -- Perform update operation
    UPDATE fcem_data.fc_case_party_relationship
    SET case_party_fc_profile = jsonb_set(
        COALESCE(case_party_fc_profile, '{}'),
        '{party_name}',
        case_party_details->'party_name',
        true
    )
    WHERE case_party_details ? 'party_name'
      AND (
          case_party_fc_profile IS NULL
          OR NOT (case_party_fc_profile ? 'party_name')
          OR case_party_fc_profile->>'party_name' IS NULL
      );

    -- Track rows updated
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    -- Record end time
    v_end_time := CURRENT_TIMESTAMP;

    -- Validation checks
    IF v_rows_updated = 0 THEN
        RAISE EXCEPTION 'No rows were updated in FC_CASE table';
    ELSE
        RAISE NOTICE 'Successfully updated % rows in FC_CASE table', v_rows_updated;
    END IF;

    RAISE NOTICE 'Update completed at %. Total time taken: %', 
                 v_end_time, 
                 age(v_end_time, v_start_time);

END $$;

-- Commit the transaction if no errors occurred
COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction and log error
        ROLLBACK;
        RAISE NOTICE 'Error occurred during update: % - %', SQLERRM, SQLSTATE;
        RAISE NOTICE 'Transaction rolled back';
END;
