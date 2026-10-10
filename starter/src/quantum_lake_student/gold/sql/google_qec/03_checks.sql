-- Post-load assertions, run inside the load transaction. Every row returned
-- has violations > 0 and makes the loader abort (and roll back).
SELECT check_name, violations FROM (
    SELECT 'shot_count_differs_from_experiment_shots' AS check_name, count(*) AS violations
    FROM google_experiment e
    WHERE e.shots <> (SELECT count(*) FROM google_shot s WHERE s.experiment_id = e.experiment_id)
  UNION ALL
    SELECT 'shot_without_four_predictions', count(*) FROM (
        SELECT 1 FROM google_shot s
        LEFT JOIN google_shot_prediction p USING (experiment_id, shot_index)
        GROUP BY s.experiment_id, s.shot_index
        HAVING count(p.decoder_id) <> (SELECT count(*) FROM decoder)) t
  UNION ALL
    SELECT 'shot_without_raw_row', count(*) FROM google_shot s
    LEFT JOIN google_shot_raw r USING (experiment_id, shot_index)
    WHERE r.shot_index IS NULL
  UNION ALL
    SELECT 'detector_bits_wrong_byte_length', count(*)
    FROM google_shot s JOIN google_experiment e USING (experiment_id)
    WHERE octet_length(s.detector_bits) <> (e.detector_count + 7) / 8
  UNION ALL
    SELECT 'detector_padding_bits_not_zero', count(*)
    FROM google_shot s JOIN google_experiment e USING (experiment_id)
    WHERE e.detector_count % 8 <> 0
      AND (get_byte(s.detector_bits, octet_length(s.detector_bits) - 1) >> (e.detector_count % 8)) <> 0
  UNION ALL
    SELECT 'measurement_bits_wrong_byte_length', count(*)
    FROM google_shot_raw r JOIN google_experiment e USING (experiment_id)
    WHERE octet_length(r.measurement_bits) <> (e.measurement_count + 7) / 8
  UNION ALL
    SELECT 'position_stats_disagree_with_event_counts', count(*) FROM (
        SELECT e.experiment_id
        FROM google_experiment e
        WHERE (SELECT sum(fired_count) FROM google_detector_position_stat p
               WHERE p.experiment_id = e.experiment_id)
           IS DISTINCT FROM
              (SELECT sum(detector_event_count) FROM google_shot s
               WHERE s.experiment_id = e.experiment_id)) t
  UNION ALL
    SELECT 'position_stat_rows_wrong', count(*)
    FROM google_experiment e
    WHERE (SELECT count(*) FROM google_detector_position_stat p WHERE p.experiment_id = e.experiment_id)
          <> e.detector_count
) c WHERE violations > 0;
