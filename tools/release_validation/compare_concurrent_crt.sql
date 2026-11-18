-- Run in the secure CDM against two builds from the same data snapshot.
-- psql -v baseline_schema=... -v candidate_schema=... -v vocab_schema=... \
--      -f tools/release_validation/compare_concurrent_crt.sql
-- Output contains aggregate counts only. Counts 1-4 are masked as x.
\set ON_ERROR_STOP on
BEGIN READ ONLY;

WITH baseline AS (
    SELECT person_id, condition_episode, min(condition_start_date) AS diagnosis_date
    FROM :"baseline_schema".treatment_envelope_mv
    WHERE concurrent_chemort AND condition_episode IS NOT NULL
    GROUP BY person_id, condition_episode
), candidate AS (
    SELECT person_id, condition_episode,
           min(condition_start_date) AS diagnosis_date,
           min(first_concurrent_rt_start) AS rt_start
    FROM :"candidate_schema".treatment_envelope_mv
    WHERE concurrent_chemort AND condition_episode IS NOT NULL
    GROUP BY person_id, condition_episode
), regimen_windows AS (
    SELECT person_id, condition_episode_id, regimen_id,
           min(first_exposure_date::date) AS start_date,
           max(last_exposure_date::date) AS end_date
    FROM :"baseline_schema".sact_treatment_mv
    GROUP BY person_id, condition_episode_id, regimen_id
), course_windows AS (
    SELECT person_id, condition_episode_id, course_id,
           min(first_exposure_date::date) AS start_date,
           max(last_exposure_date::date) AS end_date
    FROM :"baseline_schema".rt_course_mv
    GROUP BY person_id, condition_episode_id, course_id
), legacy_overlap AS (
    -- Reproduce CONCURRENT_CHEMORT_IMPACT.sql's original A/B/C classes.
    SELECT b.person_id, b.condition_episode,
           min(greatest(c.start_date, r.start_date)) AS first_overlap
    FROM baseline b
    JOIN course_windows c
      ON c.person_id = b.person_id AND c.condition_episode_id = b.condition_episode
    JOIN regimen_windows r
      ON r.person_id = b.person_id AND r.condition_episode_id = b.condition_episode
     AND r.start_date <= c.end_date AND c.start_date <= r.end_date
    GROUP BY b.person_id, b.condition_episode
), classified AS (
    SELECT CASE
               WHEN b.condition_episode IS NULL THEN 'New only'
               WHEN o.first_overlap IS NULL THEN 'C: span only'
               WHEN o.first_overlap > b.diagnosis_date + 180 THEN 'B: late interval overlap'
               ELSE 'A: early interval overlap'
           END AS category,
           b.condition_episode IS NOT NULL AS before_flag,
           n.condition_episode IS NOT NULL AS after_flag,
           n.rt_start - coalesce(b.diagnosis_date, n.diagnosis_date) AS after_days
    FROM baseline b
    FULL JOIN candidate n USING (person_id, condition_episode)
    LEFT JOIN legacy_overlap o
      ON o.person_id = b.person_id AND o.condition_episode = b.condition_episode
), totals AS (
    SELECT category,
           count(*) FILTER (WHERE before_flag) AS before_flag,
           count(*) FILTER (WHERE after_flag) AS after_flag,
           count(*) FILTER (WHERE after_flag AND after_days BETWEEN 0 AND 180) AS after_within_180
    FROM (VALUES ('A: early interval overlap'), ('B: late interval overlap'),
                 ('C: span only'), ('New only')) categories(category)
    LEFT JOIN classified USING (category)
    GROUP BY category
)
SELECT category,
       CASE WHEN before_flag BETWEEN 1 AND 4 THEN 'x' ELSE before_flag::text END AS concurrent_before,
       CASE WHEN after_flag BETWEEN 1 AND 4 THEN 'x' ELSE after_flag::text END AS concurrent_after,
       CASE WHEN after_within_180 BETWEEN 1 AND 4 THEN 'x' ELSE after_within_180::text END AS after_within_180_days
FROM totals
ORDER BY category;

-- SQL cross-check for locked lung indicator 13 (report 1, episode grain).
-- The report cohort is measure 71. Its diagnosis anchor is the earliest
-- qualifying primary lung diagnosis episode start. The denominator is
-- measure 43 (SCLC AND any dated stage I-III), and measure 23 supplies CRT.
-- Compare to the report evaluator before using these as official counts.
WITH scoped AS (
    SELECT 'before' AS version, person_id, condition_episode, condition_concept_id, episode_start_date
    FROM :"baseline_schema".primary_diagnosis_condition_mv
    UNION ALL
    SELECT 'after', person_id, condition_episode, condition_concept_id, episode_start_date
    FROM :"candidate_schema".primary_diagnosis_condition_mv
), stages AS (
    SELECT 'before' AS version, person_id, condition_episode, stage_concept_id
    FROM :"baseline_schema".stage_modifier_mv WHERE stage_date IS NOT NULL
    UNION ALL
    SELECT 'after', person_id, condition_episode, stage_concept_id
    FROM :"candidate_schema".stage_modifier_mv WHERE stage_date IS NOT NULL
), cohort AS (
    SELECT version, person_id, condition_episode, min(episode_start_date) AS anchor_date
    FROM scoped d
    WHERE d.condition_episode IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM :"vocab_schema".concept_ancestor a
          WHERE a.descendant_concept_id = d.condition_concept_id
            AND a.ancestor_concept_id IN (443388, 443399)
      )
    GROUP BY version, person_id, condition_episode
), denominator AS (
    SELECT c.*
    FROM cohort c
    WHERE c.anchor_date IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM scoped d
          JOIN :"vocab_schema".concept_ancestor a ON a.descendant_concept_id = d.condition_concept_id
          WHERE d.version = c.version AND d.person_id = c.person_id AND d.condition_episode = c.condition_episode
            AND d.episode_start_date IS NOT NULL
            AND a.ancestor_concept_id IN (
                4252582, 602695, 4110591, 37395650, 37167517, 602160,
                605477, 605476, 602159, 44502427, 37395648, 44502159,
                608925, 37395649, 44500000, 44499678, 608926, 36713366,
                602696, 602161, 36521732, 37395651
            )
      )
      AND EXISTS (
          SELECT 1 FROM stages s
          JOIN :"vocab_schema".concept_ancestor a ON a.descendant_concept_id = s.stage_concept_id
          WHERE s.version = c.version AND s.person_id = c.person_id AND s.condition_episode = c.condition_episode
            AND a.ancestor_concept_id IN (1635838, 1635131, 1634191)
      )
), numerator AS (
    SELECT 'before' AS version, person_id, condition_episode, condition_start_date AS event_date
    FROM :"baseline_schema".treatment_envelope_mv WHERE concurrent_chemort
    UNION ALL
    SELECT 'after', person_id, condition_episode, first_concurrent_rt_start
    FROM :"candidate_schema".treatment_envelope_mv WHERE concurrent_chemort
), lung_totals AS (
    SELECT v.version, count(d.condition_episode) AS denominator_count,
           count(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM numerator n
               WHERE n.version = d.version AND n.person_id = d.person_id
                 AND n.condition_episode = d.condition_episode
                 AND n.event_date::date - d.anchor_date::date BETWEEN 0 AND 90
           )) AS numerator_count
    FROM (VALUES ('before'), ('after')) v(version)
    LEFT JOIN denominator d USING (version)
    GROUP BY v.version
)
SELECT version,
       CASE WHEN denominator_count BETWEEN 1 AND 4 THEN 'x' ELSE denominator_count::text END AS lung_13_denominator,
       CASE WHEN numerator_count BETWEEN 1 AND 4 THEN 'x' ELSE numerator_count::text END AS lung_13_numerator
FROM lung_totals
ORDER BY version DESC;

ROLLBACK;
