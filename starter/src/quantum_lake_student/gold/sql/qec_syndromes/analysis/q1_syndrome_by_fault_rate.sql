-- Q1a: how do weighted syndrome frequency and logical-error labels change with
-- physical fault rate? One row per fault-rate experiment.
-- Joins syndrome_observation, simulated_experiment and syndrome_pattern (3 Gold tables).
WITH both_labels AS (
    -- (experiment, pattern) pairs observed with label 0 and with label 1
    SELECT experiment_id, syndrome_id
    FROM syndrome_observation
    GROUP BY experiment_id, syndrome_id
    HAVING count(*) = 2
)
SELECT
    s.physical_fault_rate,
    count(*)                                                          AS aggregate_rows,
    count(DISTINCT o.syndrome_id)                                     AS distinct_patterns,
    sum(o.quantity)::bigint                                           AS total_shots,
    -- FILTER sums are NULL when no row matches, hence coalesce(..., 0)
    coalesce(sum(o.quantity) FILTER (WHERE o.logical_error_label), 0)::float8
        / sum(o.quantity)                                             AS weighted_logical_error_rate,
    coalesce(sum(o.quantity) FILTER (WHERE p.fired_count = 0), 0)::float8
        / sum(o.quantity)                                             AS trivial_syndrome_share,
    sum(o.quantity * p.fired_count)::float8 / sum(o.quantity)         AS mean_fired_checks,
    coalesce(sum(o.quantity) FILTER (WHERE b.syndrome_id IS NOT NULL), 0)::float8
        / sum(o.quantity)                                             AS both_label_pattern_share
FROM syndrome_observation o
JOIN simulated_experiment s USING (experiment_id)
JOIN syndrome_pattern p USING (syndrome_id)
LEFT JOIN both_labels b USING (experiment_id, syndrome_id)
GROUP BY s.physical_fault_rate
ORDER BY s.physical_fault_rate;
