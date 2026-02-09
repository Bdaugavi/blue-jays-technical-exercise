CREATE TABLE runner_play (
     gamepk INTEGER NOT NULL,
     atbatindex INTEGER NOT NULL,
     playindex INTEGER NOT NULL,
     runnerid INTEGER NOT NULL,
     playid UUID NULL,
     runnerfullname TEXT NOT NULL,
     startbase TEXT NOT NULL,
     endbase TEXT,
     reachedbase TEXT,
     is_out BOOLEAN NOT NULL,
     eventtype TEXT NOT NULL,
     movementreason TEXT,
     is_risp BOOLEAN NOT NULL,
     is_firsttothird BOOLEAN NOT NULL,
     is_secondtohome BOOLEAN NOT NULL,
     CONSTRAINT "runner_play_primary_key" PRIMARY KEY (gamepk, atbatindex, playindex, runnerid)
);