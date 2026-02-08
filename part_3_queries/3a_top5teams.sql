--Returns the top 5 teams in regular season wins
SELECT
    CASE
        WHEN awayteamscore > hometeamscore THEN awayteamname
        WHEN hometeamscore > awayteamscore THEN hometeamname
        ELSE 'TIE'
        END AS team,
    count(*) AS num_wins
FROM game
WHERE gametype = 'R'
GROUP BY team
ORDER BY num_wins DESC
LIMIT 5;
