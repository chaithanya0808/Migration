BEGIN;

DO $$ 
DECLARE
    batch_size INTEGER := 10000;               -- Batch size
    v_rows_updated BIGINT := 0;                -- Rows updated in current batch
    v_total_rows_updated BIGINT := 0;          -- Total rows updated
    v_start_time TIMESTAMP;                    -- Execution start time
    v_end_time TIMESTAMP;                      -- Execution end time
BEGIN
    -- Start the timer
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Starting batch update at %', v_start_time;

    LOOP
        -- Batch update
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = jsonb_set(
            COALESCE(case_party_fc_profile, '{}'), -- Initialize JSONB if NULL
            '{party_name}',                        -- Path for the `party_name` key
            case_party_details->'party_name',      -- Value from `case_party_details`
            true                                   -- Overwrite existing value
        )
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists in `case_party_details`
              AND (
                  case_party_fc_profile IS NULL                     -- JSONB column is NULL
                  OR NOT (case_party_fc_profile ? 'party_name')     -- `party_name` key is missing
                  OR case_party_fc_profile->>'party_name' IS NULL   -- `party_name` is NULL
              )
            LIMIT batch_size
        )
        RETURNING 1 INTO v_rows_updated;

        -- Increment total rows updated
        v_total_rows_updated := v_total_rows_updated + v_rows_updated;

        -- Exit the loop when no more rows are updated
        EXIT WHEN v_rows_updated = 0;

        RAISE NOTICE '% rows updated in this batch.', v_rows_updated;
    END LOOP;

    -- End the timer
    v_end_time := clock_timestamp();

    -- Validation checks
    IF v_total_rows_updated = 0 THEN
        RAISE NOTICE 'No rows were updated in fc_case_party_relationship table.';
    ELSE
        RAISE NOTICE 'Successfully updated % rows in fc_case_party_relationship table.', v_total_rows_updated;
    END IF;

    -- Display execution time
    RAISE NOTICE 'Batch migration completed at %. Total time taken: %', 
        v_end_time, age(v_end_time, v_start_time);

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback on error
        ROLLBACK;
        RAISE NOTICE 'Error occurred during update: % - %', SQLERRM, SQLSTATE;
        RAISE NOTICE 'Transaction rolled back.';
        RAISE;
END $$;

COMMIT;





2nd revised approach


BEGIN;

DO $$ 
DECLARE
    batch_size INTEGER := 10000;               -- Batch size
    v_rows_updated BIGINT := 0;                -- Rows updated in current batch
    v_total_rows_updated BIGINT := 0;          -- Total rows updated
    v_start_time TIMESTAMP;                    -- Execution start time
    v_end_time TIMESTAMP;                      -- Execution end time
BEGIN
    -- Start the timer
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Starting batch update at %', v_start_time;

    LOOP
        -- Batch update
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = jsonb_set(
            COALESCE(case_party_fc_profile, '{}'), -- Initialize JSONB if NULL
            '{party_name}',                        -- Path for the `party_name` key
            case_party_details->'party_name',      -- Value from `case_party_details`
            true                                   -- Overwrite existing value
        )
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists in `case_party_details`
              AND (
                  case_party_fc_profile IS NULL                     -- JSONB column is NULL
                  OR NOT (case_party_fc_profile ? 'party_name')     -- `party_name` key is missing
                  OR case_party_fc_profile->>'party_name' IS NULL   -- `party_name` is NULL
              )
            LIMIT batch_size
        );

        -- Get the number of rows updated in this batch
        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        -- Increment the total rows updated
        v_total_rows_updated := v_total_rows_updated + v_rows_updated;

        -- Exit the loop if no more rows are updated
        EXIT WHEN v_rows_updated = 0;

        RAISE NOTICE '% rows updated in this batch.', v_rows_updated;
    END LOOP;

    -- End the timer
    v_end_time := clock_timestamp();

    -- Validation checks
    IF v_total_rows_updated = 0 THEN
        RAISE NOTICE 'No rows were updated in fc_case_party_relationship table.';
    ELSE
        RAISE NOTICE 'Successfully updated % rows in fc_case_party_relationship table.', v_total_rows_updated;
    END IF;

    -- Display execution time
    RAISE NOTICE 'Batch migration completed at %. Total time taken: %', 
        v_end_time, age(v_end_time, v_start_time);

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback on error
        ROLLBACK;
        RAISE NOTICE 'Error occurred during update: % - %', SQLERRM, SQLSTATE;
        RAISE NOTICE 'Transaction rolled back.';
        RAISE;
END $$;

COMMIT;

