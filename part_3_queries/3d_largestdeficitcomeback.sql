SELECT l.gamepk, l.battingteamid AS teamid,
    CASE WHEN l.half=0 THEN g.awayteamname ELSE g.hometeamname END AS teamname,
    CASE WHEN l.half=0 THEN g.hometeamname ELSE g.awayteamname END AS opposingteamname,
    MIN(l.battingteam_score_diff) AS deficit
FROM linescore l
INNER JOIN
game g
ON l.gamepk = g.gamepk
WHERE (l.half = 0 AND g.awayteamscore > g.hometeamscore)
   OR (l.half = 1 AND g.hometeamscore > g.awayteamscore)
GROUP BY l.gamepk, l.battingteamid, teamname, opposingteamname
ORDER BY deficit
LIMIT 1;