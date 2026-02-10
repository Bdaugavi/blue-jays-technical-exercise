--Assuming all game types
--Most aggressive runners
SELECT runnerid, runnerfullname, COUNT(*) AS num_aggresive_actions
FROM runner_play
WHERE
    ((eventtype = 'single' AND ((startbase = '1B' AND endbase IN ('3B', 'HM')) OR (startbase = '2B' AND endbase = 'HM')))
        OR
     (eventtype = 'double' AND  startbase = '1B' and endbase = 'HM'))
   OR eventtype IN ('caught_stealing_2b', 'caught_stealing_3b', 'caught_stealing_home',
                    'pickoff_1b', 'pickoff_2b', 'pickoff_3b', 'pickoff_caught_stealing_2b', 'pickoff_caught_stealing_3b',
                    'pickoff_caught_stealing_home', 'stolen_base_2b', 'stolen_base_3b', 'stolen_base_home')

GROUP BY runnerid, runnerfullname
ORDER BY num_aggresive_actions DESC LIMIT 10
