-- Analysis Q2 (Google part): decoder logical-error rate by code distance and
-- processor location. Joins google_experiment, google_shot, google_shot_prediction
-- and decoder (via v_google_decoder_outcome). Rate = decoder mistakes / shots.
SELECT e.distance,
       e.center_row,
       e.center_col,
       o.decoder_name,
       count(*)                                         AS shots,
       count(*) FILTER (WHERE o.is_logical_error)       AS decoder_mistakes,
       round(avg(o.is_logical_error::int), 5)           AS decoder_error_rate,
       round(avg(o.actual_observable_flip::int), 5)     AS actual_flip_rate
FROM v_google_decoder_outcome o
JOIN google_experiment e USING (experiment_id)
GROUP BY e.distance, e.center_row, e.center_col, o.decoder_name
ORDER BY e.distance, e.center_row, e.center_col, o.decoder_name;
