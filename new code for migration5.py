BEGIN;

DO $$ 
DECLARE
    batch_size INTEGER := 10000;
BEGIN
    LOOP
        UPDATE fcem_data.fc_case_party_relationship
        SET case_party_fc_profile = jsonb_set(
            COALESCE(case_party_fc_profile, '{}'),
            '{party_name}',
            case_party_details->'party_name',
            true
        )
        WHERE ctid IN (
            SELECT ctid
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'
              AND (
                  case_party_fc_profile IS NULL
                  OR NOT (case_party_fc_profile ? 'party_name')
                  OR case_party_fc_profile->>'party_name' IS NULL
              )
            LIMIT batch_size
        );

        EXIT WHEN NOT EXISTS (
            SELECT 1
            FROM fcem_data.fc_case_party_relationship
            WHERE case_party_details ? 'party_name'
              AND (
                  case_party_fc_profile IS NULL
                  OR NOT (case_party_fc_profile ? 'party_name')
                  OR case_party_fc_profile->>'party_name' IS NULL
              )
        );
    END LOOP;

    RAISE NOTICE 'Migration completed successfully.';

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Transaction rolled back due to error: %', SQLERRM;
        RAISE;
END $$;

COMMIT;
