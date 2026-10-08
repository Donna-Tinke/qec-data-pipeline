-- How many decoders are wrong on the same shot? If mistakes overlap fully, a
-- combined decoder has nothing to exploit; partial overlap is what a combiner needs.
WITH per_shot AS (
    SELECT experiment_id, shot_index, count(*) FILTER (WHERE is_logical_error) AS wrong_decoders
    FROM v_google_decoder_outcome
    GROUP BY experiment_id, shot_index
)
SELECT e.distance,
       p.wrong_decoders,
       count(*)                                                       AS shots,
       round(count(*)::numeric / sum(count(*)) OVER (PARTITION BY e.distance), 5) AS share_of_distance
FROM per_shot p
JOIN google_experiment e USING (experiment_id)
GROUP BY e.distance, p.wrong_decoders
ORDER BY e.distance, p.wrong_decoders;
