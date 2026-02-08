
-- Returns the teams with the most come from behind wins
-- Includes all game types
SELECT battingteamid, teamname, count(*) AS num_comebacks FROM (
    SELECT DISTINCT
    l.gamepk,
    l.battingteamid,
    CASE WHEN l.half=0 THEN g.awayteamname ELSE g.hometeamname END AS teamname,
    MIN(l.battingteam_score_diff) OVER (PARTITION BY l.gamepk, l.battingteamid) AS largest_deficit
    FROM linescore l
    INNER JOIN
    game g
    ON l.gamepk = g.gamepk
    AND
    ((l.half = 0 AND g.awayteamscore > g.hometeamscore) OR (l.half = 1 AND g.hometeamscore > g.awayteamscore))) comebacks
WHERE largest_deficit < 0
GROUP BY battingteamid, teamname
ORDER BY num_comebacks DESC
LIMIT 5;