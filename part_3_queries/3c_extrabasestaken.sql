--Returns the players with the most extra bases taken in 2025.
--Includes all game types
SELECT runnerid, runnerfullname, COUNT(*) AS num_extra_bases_taken
FROM runner_play
WHERE is_out = FALSE AND
    ((eventtype = 'single' AND ((startbase = '1B' AND endbase IN ('3B', 'HM')) OR (startbase = '2B' AND endbase = 'HM')))
        OR
     (eventtype = 'double' AND  startbase = '1B' and endbase = 'HM'))
GROUP BY runnerid, runnerfullname
ORDER BY num_extra_bases_taken DESC LIMIT 10