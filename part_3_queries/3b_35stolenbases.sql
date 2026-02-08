-- Returns runners wuth 35 or more stolen bases in the regular season
SELECT rp.runnerid, rp.runnerfullname, COUNT(*) AS num_stolen_bases
FROM runner_play rp
INNER JOIN
game g
ON g.gamepk = rp.gamepk
WHERE rp.eventtype IN ('stolen_base_2b', 'stolen_base_3b', 'stolen_base_home')
  AND g.gametype = 'R'
GROUP BY runnerid, runnerfullname
HAVING COUNT(*) >= 35
ORDER BY num_stolen_bases desc
