BEGIN;

SET LOCAL ROLE kalshi_crypto_basis_owner;
SET LOCAL search_path = evidence, pg_catalog;

CREATE SCHEMA IF NOT EXISTS evidence AUTHORIZATION kalshi_crypto_basis_owner;
REVOKE ALL ON SCHEMA evidence FROM PUBLIC;

CREATE TABLE IF NOT EXISTS evidence.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS evidence.raw_payloads (
    raw_sha256 text PRIMARY KEY CHECK (raw_sha256 ~ '^[0-9a-f]{64}$'),
    payload bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS evidence.snapshots (
    snapshot_id text PRIMARY KEY CHECK (snapshot_id ~ '^sha256:[0-9a-f]{64}$'),
    idempotency_key text NOT NULL UNIQUE
        CHECK (idempotency_key ~ '^sha256:[0-9a-f]{64}$'),
    raw_sha256 text NOT NULL REFERENCES evidence.raw_payloads (raw_sha256),
    source text NOT NULL CHECK (source <> ''),
    request_fingerprint text NOT NULL
        CHECK (request_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL CHECK (ingested_at >= observed_at),
    parser_version text NOT NULL CHECK (parser_version <> ''),
    envelope_json bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS evidence.collection_runs (
    run_id uuid PRIMARY KEY,
    provider text NOT NULL CHECK (provider <> ''),
    scope_json bytea NOT NULL,
    started_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS evidence.collection_run_snapshots (
    run_id uuid NOT NULL REFERENCES evidence.collection_runs (run_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    snapshot_id text NOT NULL REFERENCES evidence.snapshots (snapshot_id),
    PRIMARY KEY (run_id, ordinal),
    UNIQUE (run_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS evidence.collection_run_events (
    run_id uuid NOT NULL REFERENCES evidence.collection_runs (run_id),
    sequence integer NOT NULL CHECK (sequence IN (0, 1)),
    state text NOT NULL CHECK (state IN ('started', 'complete', 'incomplete', 'failed')),
    occurred_at timestamptz NOT NULL,
    gaps_json bytea NOT NULL,
    expected_snapshot_count integer CHECK (expected_snapshot_count >= 0),
    PRIMARY KEY (run_id, sequence),
    CHECK ((sequence = 0 AND state = 'started') OR (sequence = 1 AND state <> 'started')),
    CHECK (sequence = 1 OR expected_snapshot_count IS NULL)
);

ALTER TABLE evidence.collection_run_events
    ADD COLUMN IF NOT EXISTS expected_snapshot_count integer
    CHECK (expected_snapshot_count >= 0);

CREATE UNIQUE INDEX IF NOT EXISTS one_terminal_collection_run_event
    ON evidence.collection_run_events (run_id)
    WHERE state IN ('complete', 'incomplete', 'failed');

CREATE OR REPLACE FUNCTION evidence.reject_immutable_evidence_change()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'immutable evidence rows cannot be changed'
        USING ERRCODE = '55000';
END;
$$;

CREATE OR REPLACE FUNCTION evidence.validate_collection_snapshot_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    retained_snapshot_count integer;
    retained_snapshot_id text;
BEGIN
    PERFORM 1 FROM evidence.collection_runs
    WHERE run_id = NEW.run_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'collection run does not exist'
            USING ERRCODE = '23503';
    END IF;
    IF EXISTS (
        SELECT 1 FROM evidence.collection_run_events
        WHERE run_id = NEW.run_id AND sequence = 1
    ) THEN
        RAISE EXCEPTION 'terminal collection run lineage cannot be extended'
            USING ERRCODE = '55000';
    END IF;
    SELECT snapshot_id INTO retained_snapshot_id
    FROM evidence.collection_run_snapshots
    WHERE run_id = NEW.run_id AND ordinal = NEW.ordinal;
    IF FOUND THEN
        IF retained_snapshot_id = NEW.snapshot_id THEN
            RETURN NULL;
        END IF;
        RAISE EXCEPTION 'collection snapshot ordinal conflict'
            USING ERRCODE = '23505';
    END IF;
    SELECT count(*) INTO retained_snapshot_count
    FROM evidence.collection_run_snapshots
    WHERE run_id = NEW.run_id;
    IF NEW.ordinal <> retained_snapshot_count THEN
        RAISE EXCEPTION 'snapshot ordinal must be the next contiguous value'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION evidence.validate_collection_event_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    retained_snapshot_count integer;
    retained_min_ordinal integer;
    retained_max_ordinal integer;
    parsed_gaps jsonb;
    canonical_gaps text;
BEGIN
    PERFORM 1 FROM evidence.collection_runs
    WHERE run_id = NEW.run_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'collection run does not exist'
            USING ERRCODE = '23503';
    END IF;
    BEGIN
        parsed_gaps := convert_from(NEW.gaps_json, 'UTF8')::jsonb;
    EXCEPTION WHEN others THEN
        RAISE EXCEPTION 'gaps_json must be a canonical gap array'
            USING ERRCODE = '23514';
    END;
    IF jsonb_typeof(parsed_gaps) <> 'array' THEN
        RAISE EXCEPTION 'gaps_json must be a canonical gap array'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(parsed_gaps) AS item(value)
        WHERE jsonb_typeof(value) <> 'string'
           OR value #>> '{}' = ''
    ) THEN
        RAISE EXCEPTION 'gaps_json must be a canonical gap array'
            USING ERRCODE = '23514';
    END IF;
    SELECT COALESCE(
        '[' || string_agg(to_json(gap)::text, ',' ORDER BY gap COLLATE "C") || ']',
        '[]'
    )
    INTO canonical_gaps
    FROM (
        SELECT DISTINCT value #>> '{}' AS gap
        FROM jsonb_array_elements(parsed_gaps) AS item(value)
    ) AS canonical_items;
    NEW.gaps_json := convert_to(canonical_gaps, 'UTF8');
    IF NEW.sequence = 1 AND NEW.state = 'complete' THEN
        IF NEW.gaps_json <> convert_to('[]', 'UTF8') THEN
            RAISE EXCEPTION 'complete run cannot retain gaps'
                USING ERRCODE = '23514';
        END IF;
        SELECT count(*), min(ordinal), max(ordinal)
        INTO retained_snapshot_count, retained_min_ordinal, retained_max_ordinal
        FROM evidence.collection_run_snapshots
        WHERE run_id = NEW.run_id;
        IF NEW.expected_snapshot_count IS NULL
           OR NEW.expected_snapshot_count <= 0
           OR retained_snapshot_count <> NEW.expected_snapshot_count
           OR retained_min_ordinal <> 0
           OR retained_max_ordinal <> retained_snapshot_count - 1 THEN
            RAISE EXCEPTION 'complete run requires exact nonzero contiguous snapshot count'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    trigger_spec text;
    trigger_name text;
    table_name text;
BEGIN
    FOREACH trigger_spec IN ARRAY ARRAY[
        'raw_payloads_are_immutable:evidence.raw_payloads',
        'snapshots_are_immutable:evidence.snapshots',
        'collection_runs_are_immutable:evidence.collection_runs',
        'collection_run_snapshots_are_immutable:evidence.collection_run_snapshots',
        'collection_run_events_are_immutable:evidence.collection_run_events'
    ]
    LOOP
        trigger_name := split_part(trigger_spec, ':', 1);
        table_name := split_part(trigger_spec, ':', 2);
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = trigger_name
              AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %s '
                'FOR EACH ROW EXECUTE FUNCTION evidence.reject_immutable_evidence_change()',
                trigger_name,
                table_name
            );
        END IF;
    END LOOP;
END;
$$;

DROP TRIGGER IF EXISTS collection_snapshot_insert_guard
    ON evidence.collection_run_snapshots;
CREATE TRIGGER collection_snapshot_insert_guard
BEFORE INSERT ON evidence.collection_run_snapshots
FOR EACH ROW EXECUTE FUNCTION evidence.validate_collection_snapshot_insert();

DROP TRIGGER IF EXISTS collection_event_insert_guard
    ON evidence.collection_run_events;
CREATE TRIGGER collection_event_insert_guard
BEFORE INSERT ON evidence.collection_run_events
FOR EACH ROW EXECUTE FUNCTION evidence.validate_collection_event_insert();

INSERT INTO evidence.schema_migrations (version)
VALUES (1)
ON CONFLICT (version) DO NOTHING;

REVOKE ALL ON ALL TABLES IN SCHEMA evidence FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA evidence FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA evidence FROM PUBLIC;

GRANT USAGE ON SCHEMA evidence TO kalshi_crypto_basis_runtime;
GRANT SELECT ON
    evidence.schema_migrations,
    evidence.raw_payloads,
    evidence.snapshots,
    evidence.collection_runs,
    evidence.collection_run_snapshots,
    evidence.collection_run_events
TO kalshi_crypto_basis_runtime;
GRANT INSERT ON
    evidence.raw_payloads,
    evidence.snapshots,
    evidence.collection_runs,
    evidence.collection_run_snapshots,
    evidence.collection_run_events
TO kalshi_crypto_basis_runtime;

ALTER DEFAULT PRIVILEGES FOR ROLE kalshi_crypto_basis_owner IN SCHEMA evidence
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE kalshi_crypto_basis_owner IN SCHEMA evidence
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE kalshi_crypto_basis_owner IN SCHEMA evidence
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

COMMIT;
