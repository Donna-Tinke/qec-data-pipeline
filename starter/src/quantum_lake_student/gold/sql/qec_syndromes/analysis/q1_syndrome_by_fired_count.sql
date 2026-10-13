-- Q1b: within each fault rate, how does the weighted logical-error rate depend
-- on how many of the 16 syndrome checks fired? One row per (fault rate, fired count).
SELECT
    s.physical_fault_rate,
    p.fired_count,
    sum(o.quantity)::bigint                                           AS shots,
    sum(o.quantity)::float8
        / sum(sum(o.quantity)) OVER (PARTITION BY s.physical_fault_rate) AS shot_share,
    coalesce(sum(o.quantity) FILTER (WHERE o.logical_error_label), 0)::float8
        / sum(o.quantity)                                             AS weighted_logical_error_rate
FROM syndrome_observation o
JOIN simulated_experiment s USING (experiment_id)
JOIN syndrome_pattern p USING (syndrome_id)
GROUP BY s.physical_fault_rate, p.fired_count
ORDER BY s.physical_fault_rate, p.fired_count;
