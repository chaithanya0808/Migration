BEGIN;
    DO $$
    DECLARE
        v_rows_updated bigint;
        v_start_time timestamp;
        v_end_time timestamp;
    BEGIN
        -- Record start time
        v_start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE 'Starting FC_CASE_PARTY_RELATIONSHIP update at %', v_start_time;

        WITH rows_to_update AS (
            SELECT ctid, case_party_details->'party_name' AS new_party_name
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'
              AND (
                  case_party_fc_profile IS NULL
                  OR NOT (case_party_fc_profile ? 'party_name')
                  OR case_party_fc_profile->>'party_name' IS NULL
              )
        )

        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = jsonb_set(
            COALESCE(case_party_fc_profile, '{}'),
            '{party_name}',
            rows_to_update.new_party_name,
            true
        )
        FROM rows_to_update
        WHERE fcem_data.fc_case_party_relationship.ctid = rows_to_update.ctid;

        -- Get number of rows updated
        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        -- Record end time
        v_end_time := CURRENT_TIMESTAMP;

        -- Validation checks
        IF v_rows_updated = 0 THEN
            RAISE EXCEPTION 'No rows were updated in FC_CASE_PARTY_RELATIONSHIP table';
        ELSE
            RAISE NOTICE 'Successfully updated % rows in FC_CASE_PARTY_RELATIONSHIP table', v_rows_updated;
        END IF;

        RAISE NOTICE 'Update completed at %. Total time taken: %',
            v_end_time,
            age(v_end_time, v_start_time);

    EXCEPTION
        WHEN OTHERS THEN
            -- Roll back transaction
            ROLLBACK;
            RAISE NOTICE 'Error occurred during update: % - %', SQLERRM, SQLSTATE;
            RAISE NOTICE 'Transaction rolled back';
            -- Re-raise the exception
            RAISE;
    END $$;
COMMIT;
