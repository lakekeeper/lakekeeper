CREATE TYPE paimon_commit_state AS ENUM ('stable', 'pending-publish', 'publish-failed');

CREATE TABLE paimon_table (
    warehouse_id         UUID NOT NULL,
    tabular_id           UUID NOT NULL,
    current_snapshot_id  BIGINT,
    metadata_location    TEXT,
    current_branch       TEXT NOT NULL DEFAULT 'main',
    table_options        JSONB NOT NULL DEFAULT '{}'::jsonb,
    schema_id            INT,
    partition_keys       TEXT[] NOT NULL DEFAULT '{}',
    primary_keys         TEXT[] NOT NULL DEFAULT '{}',
    comment              TEXT,
    commit_state         paimon_commit_state NOT NULL DEFAULT 'stable',
    pending_commit_token UUID,
    last_commit_error    TEXT,
    version              BIGINT NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ,
    PRIMARY KEY (warehouse_id, tabular_id),
    FOREIGN KEY (warehouse_id, tabular_id)
        REFERENCES tabular (warehouse_id, tabular_id) ON DELETE CASCADE
);

SELECT trigger_updated_at_and_version_if_distinct('paimon_table');

CREATE INDEX paimon_table_state_scan_idx
    ON paimon_table (warehouse_id, commit_state, updated_at);

CREATE INDEX paimon_table_pending_commit_token_idx
    ON paimon_table (warehouse_id, pending_commit_token)
    WHERE pending_commit_token IS NOT NULL;
