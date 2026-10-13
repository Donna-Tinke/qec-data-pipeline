-- Detector positions that fire most often per experiment, from the per-position
-- summary table (no scan of the shot table).
SELECT experiment_id, detector_index, fired_count,
       round(fired_count::numeric / shots_observed, 4) AS firing_rate
FROM (
    SELECT p.*, row_number() OVER (PARTITION BY p.experiment_id ORDER BY p.fired_count DESC, p.detector_index) AS rnk
    FROM google_detector_position_stat p
) t
WHERE rnk <= 5
ORDER BY experiment_id, rnk;
