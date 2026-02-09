CREATE TABLE "linescore" (
     "gamepk" integer,
     "inning" integer,
     "half" integer,
     "battingteamid" integer,
     "runs" integer,
     "hits" integer,
     "errors" integer,
     "leftonbase" integer,
     "battingteam_score" integer,
     "battingteam_score_diff" integer,
     CONSTRAINT "linescore_primary_key" PRIMARY KEY ("gamepk", "inning", "half")
);