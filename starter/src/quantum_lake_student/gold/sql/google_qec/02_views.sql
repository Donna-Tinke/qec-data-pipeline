-- Views over the google_qec Gold tables. Created in the same transaction as
-- the tables (see loader).

-- One row = one (shot, decoder) pair with the derived decoder-mistake flag.
CREATE VIEW v_google_decoder_outcome AS
SELECT s.experiment_id,
       s.shot_index,
       d.decoder_id,
       d.decoder_name,
       s.actual_observable_flip,
       p.predicted_flip,
       (p.predicted_flip <> s.actual_observable_flip) AS is_logical_error
FROM google_shot s
JOIN google_shot_prediction p USING (experiment_id, shot_index)
JOIN decoder d USING (decoder_id);

-- One row = one shot with the four supplied predictions side by side.
CREATE VIEW v_google_shot_predictions_wide AS
SELECT experiment_id,
       shot_index,
       bool_or(predicted_flip) FILTER (WHERE decoder_id = 1) AS belief_matching_prediction,
       bool_or(predicted_flip) FILTER (WHERE decoder_id = 2) AS correlated_matching_prediction,
       bool_or(predicted_flip) FILTER (WHERE decoder_id = 3) AS pymatching_prediction,
       bool_or(predicted_flip) FILTER (WHERE decoder_id = 4) AS tensor_network_contraction_prediction
FROM google_shot_prediction
GROUP BY experiment_id, shot_index;

-- Trace link: ML example_id -> Gold shot -> Silver/Bronze (via source_record_id
-- and results/part1/source_trace.parquet). The id is a pure function of
-- (experiment_id, shot_index), so it is stable across reruns.
CREATE VIEW v_google_example_lookup AS
SELECT encode(sha256(convert_to('google_qec:' || experiment_id || ':' || shot_index::text, 'UTF8')), 'hex')
           AS example_id,
       experiment_id,
       shot_index,
       source_record_id
FROM google_shot;
