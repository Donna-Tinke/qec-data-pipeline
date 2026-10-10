-- Gold model for the google_qec source (PostgreSQL 16).
--
-- Re-runnable: everything owned by this file is dropped and rebuilt, and the
-- loader runs this file, the data load and 03_checks.sql in ONE transaction.
-- If anything fails PostgreSQL rolls the lot back, so the previous complete
-- version stays visible (all-or-nothing load).
--
-- Names are unqualified: the loader points search_path at the target schema
-- ("gold"; tests use a throwaway schema). Only objects prefixed google_ /
-- v_google_ / v_ml_google_ / decoder are touched, so the other sources can live
-- in the same schema without being affected.

DROP VIEW IF EXISTS v_ml_google_decoder_example CASCADE;
DROP VIEW IF EXISTS v_google_example_lookup CASCADE;
DROP VIEW IF EXISTS v_google_shot_predictions_wide CASCADE;
DROP VIEW IF EXISTS v_google_decoder_outcome CASCADE;
DROP TABLE IF EXISTS google_detector_position_stat CASCADE;
DROP TABLE IF EXISTS google_shot_prediction CASCADE;
DROP TABLE IF EXISTS google_shot_raw CASCADE;
DROP TABLE IF EXISTS google_shot CASCADE;
DROP TABLE IF EXISTS google_experiment CASCADE;
DROP TABLE IF EXISTS decoder CASCADE;

-- One row = one hardware experiment (one directory of the Google archive).
CREATE TABLE google_experiment (
    experiment_id     text PRIMARY KEY,
    source_record_id  text NOT NULL UNIQUE,
    basis             text NOT NULL CHECK (basis IN ('X', 'Z')),
    distance          integer NOT NULL CHECK (distance >= 3 AND distance % 2 = 1),
    rounds            integer NOT NULL CHECK (rounds > 0),
    shots             bigint  NOT NULL CHECK (shots > 0),
    center_row        integer NOT NULL CHECK (center_row >= 0),
    center_col        integer NOT NULL CHECK (center_col >= 0),
    measurement_count integer NOT NULL CHECK (measurement_count > 0),
    detector_count    integer NOT NULL CHECK (detector_count > 0),
    -- same code distance/rounds/basis at a different processor location is a
    -- different experiment; location is part of the identity.
    UNIQUE (basis, distance, rounds, center_row, center_col)
);
COMMENT ON TABLE google_experiment IS
    'One row = one Google hardware experiment (code distance, basis, rounds, processor location).';

-- One row = one supplied decoder. Source-neutral vocabulary: a later source
-- (e.g. our own decoder) can add rows without a schema change.
CREATE TABLE decoder (
    decoder_id   smallint PRIMARY KEY,
    decoder_name text NOT NULL UNIQUE,
    description  text NOT NULL
);
COMMENT ON TABLE decoder IS
    'One row = one decoder whose predictions were supplied with the Google data.';
INSERT INTO decoder (decoder_id, decoder_name, description) VALUES
    (1, 'belief_matching',            'Belief-matching decoder (supplied predictions)'),
    (2, 'correlated_matching',        'Correlated minimum-weight matching decoder (supplied predictions)'),
    (3, 'pymatching',                 'PyMatching minimum-weight matching decoder (supplied predictions)'),
    (4, 'tensor_network_contraction', 'Tensor-network contraction decoder (supplied predictions)');

-- One row = one hardware shot: the "hot" columns used by every analysis and by
-- the ML export. Bulky raw measurements live in google_shot_raw.
CREATE TABLE google_shot (
    experiment_id          text   NOT NULL REFERENCES google_experiment,
    shot_index             bigint NOT NULL CHECK (shot_index >= 0),
    source_record_id       text   NOT NULL UNIQUE,
    -- Stim b8: byte-aligned, little-endian within each byte. Kept packed.
    detector_bits          bytea  NOT NULL,
    detector_event_count   integer NOT NULL,
    -- actual logical outcome (a label), NOT a decoder result
    actual_observable_flip boolean NOT NULL,
    PRIMARY KEY (experiment_id, shot_index),
    -- the stored count must agree with the stored bits
    CONSTRAINT detector_count_matches_bits
        CHECK (detector_event_count = bit_count(detector_bits))
);
COMMENT ON TABLE google_shot IS
    'One row = one aligned hardware shot: packed detector events and the actual logical flip.';

-- One row = the raw (non-derived) bits of one shot; strictly 1:1 with google_shot.
CREATE TABLE google_shot_raw (
    experiment_id    text   NOT NULL,
    shot_index       bigint NOT NULL,
    measurement_bits bytea  NOT NULL,   -- packed b8, raw stabilizer/data measurements
    sweep_bits       bytea  NOT NULL,   -- packed b8, empty if the experiment has no sweep bits
    PRIMARY KEY (experiment_id, shot_index),
    FOREIGN KEY (experiment_id, shot_index)
        REFERENCES google_shot ON DELETE CASCADE
);
COMMENT ON TABLE google_shot_raw IS
    'One row = raw measurement and sweep bits of one shot (kept apart from detector events, which are derived).';

-- One row = what one decoder predicted for one shot. A decoder MISTAKE is not
-- stored; it is derived (predicted_flip <> actual_observable_flip) in
-- v_google_decoder_outcome so the label and the prediction stay separate facts.
CREATE TABLE google_shot_prediction (
    experiment_id text     NOT NULL,
    shot_index    bigint   NOT NULL,
    decoder_id    smallint NOT NULL REFERENCES decoder,
    predicted_flip boolean NOT NULL,
    PRIMARY KEY (experiment_id, shot_index, decoder_id),
    FOREIGN KEY (experiment_id, shot_index)
        REFERENCES google_shot ON DELETE CASCADE
);
COMMENT ON TABLE google_shot_prediction IS
    'One row = one decoder''s predicted logical flip for one shot.';
-- The PK serves "all predictions of a shot / experiment". This one serves
-- "everything one decoder said" (per-decoder scans, FK maintenance).
CREATE INDEX google_shot_prediction_decoder_idx
    ON google_shot_prediction (decoder_id, experiment_id);

-- One row = how often one detector position fired in one experiment.
-- Derived from google_shot.detector_bits at load time (see 03_checks.sql for
-- the reconciliation with detector_event_count).
CREATE TABLE google_detector_position_stat (
    experiment_id  text    NOT NULL REFERENCES google_experiment,
    detector_index integer NOT NULL CHECK (detector_index >= 0),
    shots_observed bigint  NOT NULL CHECK (shots_observed > 0),
    fired_count    bigint  NOT NULL,
    PRIMARY KEY (experiment_id, detector_index),
    CHECK (fired_count BETWEEN 0 AND shots_observed)
);
COMMENT ON TABLE google_detector_position_stat IS
    'One row = firing frequency of one detector position in one experiment (per-position summary).';
CREATE INDEX google_experiment_distance_idx ON google_experiment (distance);
