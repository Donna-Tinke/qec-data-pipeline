-- Gold model for the qec_syndromes source (PostgreSQL 16).
--
-- Re-runnable: the objects owned by this file are dropped and rebuilt, and the
-- loader (gold/qec_syndromes.py) runs this file, the data load and the
-- Silver-to-Gold checks in ONE transaction. If anything fails, PostgreSQL rolls
-- it all back and the previous complete version stays (all-or-nothing load).
--
-- Names are unqualified: the loader points search_path at the target schema
-- ("gold"; tests use a throwaway schema). Only the objects below are touched,
-- so the google_* and QASMBench tables in the same schema are safe.
--
-- Silver keeps one row per CSV row, repeating the 16 syndrome bits 75,598 times
-- although only ~32k distinct patterns exist. Gold stores each pattern once and
-- lets observations point at it.

DROP TABLE IF EXISTS syndrome_observation, syndrome_pattern, simulated_experiment CASCADE;
DROP FUNCTION IF EXISTS syndrome_observation_id(text, bytea, boolean);

-- One row = one simulated fault-rate experiment (one source CSV file).
CREATE TABLE simulated_experiment (
    experiment_id       text             PRIMARY KEY,
    distance            integer          NOT NULL CHECK (distance >= 3 AND distance % 2 = 1),
    round_count         integer          NOT NULL CHECK (round_count > 0),
    check_count         integer          NOT NULL CHECK (check_count > 0),
    physical_fault_rate double precision NOT NULL UNIQUE
                                         CHECK (physical_fault_rate > 0 AND physical_fault_rate < 1),
    -- the "nb-10M" filename part; kept so queries can compare it with sum(quantity)
    -- (a mismatch is a Silver data-quality warning, not a Gold load failure)
    nominal_shot_count  bigint           NOT NULL CHECK (nominal_shot_count > 0)
);

COMMENT ON TABLE simulated_experiment IS
    'One row = one simulated d=3 fault-rate experiment (one source CSV file).';

-- One row = one distinct 16-bit syndrome value, shared by every experiment and
-- label it occurs with.
CREATE TABLE syndrome_pattern (
    syndrome_id   integer  PRIMARY KEY,
    -- 16 one-byte values, round first then check (same layout as Silver/ML)
    syndrome_bits bytea    NOT NULL UNIQUE
                           CHECK (length(syndrome_bits) = 16)
                           -- every byte must be 0x00 or 0x01
                           CHECK (btrim(syndrome_bits, '\x0001'::bytea) = ''::bytea),
    -- bytes are 0/1, so the set-bit count equals the number of fired checks
    fired_count   smallint GENERATED ALWAYS AS (bit_count(syndrome_bits)::smallint) STORED
);

COMMENT ON TABLE syndrome_pattern IS
    'One row = one distinct 16-bit syndrome value (4 rounds x 4 checks).';

-- example_id formula, kept in one place: sha256 of experiment | syndrome hex | label
CREATE FUNCTION syndrome_observation_id(experiment_id text, syndrome_bits bytea, label boolean)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN encode(
    sha256(convert_to(
        experiment_id || '|' || encode(syndrome_bits, 'hex') || '|' || CASE WHEN label THEN '1' ELSE '0' END,
        'UTF8'
    )),
    'hex'
);

-- One row = one aggregate (experiment, syndrome, label) observation = one
-- accepted source CSV row; quantity is the number of shots it represents.
CREATE TABLE syndrome_observation (
    experiment_id       text    NOT NULL REFERENCES simulated_experiment (experiment_id),
    syndrome_id         integer NOT NULL REFERENCES syndrome_pattern (syndrome_id),
    logical_error_label boolean NOT NULL,
    quantity            bigint  NOT NULL CHECK (quantity > 0),
    -- stable hash of the natural key; exported as ML example_id
    observation_id      text    NOT NULL UNIQUE CHECK (observation_id ~ '^[0-9a-f]{64}$'),
    -- link back to Silver / results/part1/source_trace.parquet
    source_record_id    text    NOT NULL UNIQUE,
    -- the same syndrome may occur with both labels, so the label is part of the key
    PRIMARY KEY (experiment_id, syndrome_id, logical_error_label)
);

COMMENT ON TABLE syndrome_observation IS
    'One row = one aggregate (experiment, syndrome, label) source CSV row; quantity = shots represented.';

-- The PK index serves per-experiment scans; this one serves "same pattern
-- across fault rates / labels" lookups.
CREATE INDEX syndrome_observation_syndrome_idx ON syndrome_observation (syndrome_id);
