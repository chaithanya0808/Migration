DO $$
BEGIN
    LOOP
        -- Delete the migrated party_name in batches
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = case_party_fc_profile - 'party_name'
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'  -- Ensure `party_name` exists in `case_party_details`
              AND case_party_fc_profile ? 'party_name'  -- `party_name` exists in `case_party_fc_profile`
              AND case_party_fc_profile->>'party_name' = case_party_details->>'party_name'  -- Only delete migrated ones
            LIMIT 10000 -- Batch size for better performance
        );

        -- Exit the loop when no rows are left to update
        EXIT WHEN NOT EXISTS (
            SELECT 1
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'
              AND case_party_fc_profile ? 'party_name'
              AND case_party_fc_profile->>'party_name' = case_party_details->>'party_name'
        );
    END LOOP;

    RAISE NOTICE 'Deletion of migrated party_name completed successfully.';
END $$;
