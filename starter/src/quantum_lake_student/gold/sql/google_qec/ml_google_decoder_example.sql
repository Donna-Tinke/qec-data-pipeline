-- Gold -> ML: content of ml_google_decoder_example (see assignment/required-ml-tables.md).
-- One row = one hardware shot. data_split is added by the export step from the
-- course helper google_data_split(shot_index); everything else is read here.
CREATE OR REPLACE VIEW gold.v_ml_google_decoder_example AS
SELECT l.example_id,
       e.experiment_id,
       s.shot_index,
       e.distance,
       e.rounds,
       e.center_row,
       e.center_col,
       e.detector_count,
       s.detector_event_count,
       s.detector_bits,
       w.belief_matching_prediction,
       w.correlated_matching_prediction,
       w.pymatching_prediction,
       w.tensor_network_contraction_prediction,
       s.actual_observable_flip
FROM gold.google_shot s
JOIN gold.google_experiment e USING (experiment_id)
JOIN gold.v_google_shot_predictions_wide w USING (experiment_id, shot_index)
JOIN gold.v_google_example_lookup l USING (experiment_id, shot_index);
