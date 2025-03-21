BEGIN;

DO $$ 
DECLARE
    batch_size INTEGER := 10000;
    rows_updated INTEGER;
BEGIN
    LOOP
        -- Batch removal of 'party_name' only from migrated records
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = case_party_fc_profile - 'party_name'
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_fc_profile ? 'party_name'         -- Ensure it exists in fc_profile
              AND case_party_details ? 'party_name'            -- Ensure migrated record
              AND case_party_fc_profile->>'party_name' = case_party_details->>'party_name' -- Match migrated values
            LIMIT batch_size
        )
        RETURNING 1 INTO rows_updated;

        EXIT WHEN rows_updated = 0;

        RAISE NOTICE '% rows cleaned in this batch.', rows_updated;
    END LOOP;

    RAISE NOTICE 'Cleanup of migrated party_name from case_party_fc_profile completed successfully.';

END $$;
