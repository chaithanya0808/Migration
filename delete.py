DO $$
BEGIN
    LOOP
        -- Delete matching `party_name` in batches
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = case_party_fc_profile - 'party_name'
        WHERE ctid = ANY(
            ARRAY(
                SELECT ctid
                FROM fcem_data.fc_case_party_relationship
                WHERE case_party_fc_profile ? 'party_name'   -- `party_name` exists in `case_party_fc_profile`
                  AND case_party_details ? 'party_name'      -- `party_name` exists in `case_party_details`
                  AND case_party_fc_profile->>'party_name' = case_party_details->>'party_name'  -- Matching values
                LIMIT 10000  -- Batch size for better performance
            )
        );

        -- Exit when no more rows match
        EXIT WHEN NOT FOUND;
    END LOOP;

    RAISE NOTICE 'Deletion of matching party_name completed successfully.';
END $$;
