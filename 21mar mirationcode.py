DO $$ 
DECLARE
    batch_size INTEGER := 10000;                -- Batch size for updates
    v_rows_updated BIGINT := 0;                 -- Rows updated in current batch
    v_total_rows_updated BIGINT := 0;           -- Total rows updated
    v_start_time TIMESTAMP;                     -- Execution start time
    v_end_time TIMESTAMP;                       -- Execution end time
BEGIN
    -- Start time tracking
    v_start_time := clock_timestamp();
    
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
            WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists
              AND (
                  case_party_fc_profile IS NULL                     -- JSONB column is NULL
                  OR NOT (case_party_fc_profile ? 'party_name')     -- `party_name` key is missing
                  OR case_party_fc_profile->>'party_name' IS NULL   -- `party_name` is NULL
              )
            LIMIT batch_size
        );

        -- Get the number of rows updated in this batch
        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        -- Increment the total count
        v_total_rows_updated := v_total_rows_updated + v_rows_updated;

        -- Exit loop if no more rows are updated
        EXIT WHEN v_rows_updated = 0;
    END LOOP;

    -- End time tracking
    v_end_time := clock_timestamp();

    -- Display the results
    RAISE NOTICE 'Total rows updated: %', v_total_rows_updated;
    RAISE NOTICE 'Total execution time: %', age(v_end_time, v_start_time);

END $$;





----


DO $$
DECLARE
    batch_size INTEGER := 10000;                -- Batch size for updates
    v_start_time TIMESTAMP;                     -- Execution start time
    v_end_time TIMESTAMP;                       -- Execution end time
BEGIN
    -- Start time tracking
    v_start_time := clock_timestamp();

    -- Batch processing loop
    LOOP
        -- Batch update using CTE
        WITH batch_update AS (
            UPDATE fcem_data.fc_case_party_relationship
            SET case_party_fc_profile = jsonb_set(
                COALESCE(case_party_fc_profile, '{}'), -- Initialize JSONB if NULL
                '{party_name}',                        -- Path for `party_name` key
                case_party_details->'party_name',      -- Value from `case_party_details`
                true                                   -- Overwrite existing value
            )
            WHERE ctid IN (
                SELECT ctid
                FROM fcem_data.fc_case_party_relationship
                WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists
                  AND (
                      case_party_fc_profile IS NULL                     -- JSONB column is NULL
                      OR NOT (case_party_fc_profile ? 'party_name')     -- `party_name` key is missing
                      OR case_party_fc_profile->>'party_name' IS NULL   -- `party_name` is NULL
                  )
                LIMIT batch_size
            )
            RETURNING 1
        )
        SELECT COUNT(*) INTO batch_size FROM batch_update;

        -- Exit loop when no rows are updated
        EXIT WHEN batch_size = 0;
    END LOOP;

    -- End time tracking
    v_end_time := clock_timestamp();

    -- Calculate and display the total updated rows and execution time
    RAISE NOTICE 'Total rows updated: %', (SELECT COUNT(*) 
                                           FROM fcem_data.fc_case_party_relationship
                                           WHERE case_party_fc_profile ? 'party_name' 
                                           AND case_party_details ? 'party_name'
                                           AND case_party_fc_profile->>'party_name' = case_party_details->>'party_name');
    RAISE NOTICE 'Total execution time: %', age(v_end_time, v_start_time);

END $$;


------

DO $$
DECLARE
    batch_size INTEGER := 10000;              -- Batch size for updates
    v_start_time TIMESTAMP;                   -- Execution start time
    v_end_time TIMESTAMP;                     -- Execution end time
    v_rows_updated INTEGER := 0;              -- Number of rows updated in the batch
    v_total_updated INTEGER := 0;             -- Total rows updated
BEGIN
    -- Start time tracking
    v_start_time := clock_timestamp();

    -- Loop for batch updates
    LOOP
        -- Perform batch update and get the rows updated
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = jsonb_set(
            COALESCE(case_party_fc_profile, '{}'), -- Initialize JSONB if NULL
            '{party_name}',                        -- Path for `party_name` key
            case_party_details->'party_name',      -- Value from `case_party_details`
            true                                   -- Overwrite existing value
        )
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists
              AND (
                  case_party_fc_profile IS NULL                     -- JSONB column is NULL
                  OR NOT (case_party_fc_profile ? 'party_name')     -- `party_name` key is missing
                  OR case_party_fc_profile->>'party_name' IS NULL   -- `party_name` is NULL
              )
            LIMIT batch_size
        );

        -- Get the number of rows updated in the current batch
        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        -- Track the total number of rows updated
        v_total_updated := v_total_updated + v_rows_updated;

        -- Exit loop if no more rows are updated
        EXIT WHEN v_rows_updated = 0;

        RAISE NOTICE 'Rows updated in batch: %', v_rows_updated;
    END LOOP;

    -- End time tracking
    v_end_time := clock_timestamp();

    -- Display the final result
    RAISE NOTICE 'Total rows updated: %', v_total_updated;
    RAISE NOTICE 'Total execution time: %', age(v_end_time, v_start_time);

END $$;

