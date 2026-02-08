import yaml
import psycopg
import io
import zipfile



def load_games_csvs_to_postgres(zip_path, file_name, table_name, yaml_name):
    """Read CSV files from a zip and load data into PostgreSQL table
    The records are first loaded to a temp table so that empty strings can be converted to NULL
    before doing an insert to the main table ensuring the latest date's record of a game is kept"""
    # Connect to Neon database
    with open(yaml_name) as f:
        config = yaml.safe_load(f)
    conn = psycopg.connect(config["database_uri"])
    try:
        with conn.cursor() as cur:
            # Read CSV file
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                # Loop over all files in the zip
                for file_info in zipf.infolist():
                    # Check if the file is named games.csv (regardless of folder)
                    if file_info.filename.endswith(file_name):
                        # Open the file
                        with zipf.open(file_info) as f:
                            # Wrap binary stream in TextIOWrapper so COPY can read it as text
                            text_stream = io.TextIOWrapper(f, encoding="utf-8")
                            #Create a temp table
                            create_temp_sql = f"""
                            CREATE TEMP TABLE IF NOT EXISTS tmp_games (LIKE {table_name} INCLUDING DEFAULTS);
                            """
                            cur.execute(create_temp_sql)

                            #Use psycopg's copy to copy directly into the temp table
                            copy_sql = """
                                       COPY tmp_games FROM STDIN WITH CSV HEADER NULL ''; \
                                       """
                            with cur.copy(copy_sql) as copy:
                                text_stream.seek(0)
                                copy.write(text_stream.read())

                            #Insert the records from the temp table into the main table keeping only the data from the latest date on conflict
                            insert_sql = f"""
                                INSERT INTO {table_name}
                                SELECT *
                                FROM tmp_games
                                ON CONFLICT (gamepk) DO UPDATE
                                SET
                                    gamedate = EXCLUDED.gamedate,
                                    officialdate = EXCLUDED.officialdate,
                                    sportid = EXCLUDED.sportid,
                                    gametype = EXCLUDED.gametype,
                                    codedgamestate = EXCLUDED.codedgamestate,
                                    detailedstate = EXCLUDED.detailedstate,
                                    awayteamid = EXCLUDED.awayteamid,
                                    awayteamname = EXCLUDED.awayteamname,
                                    awayteamscore = EXCLUDED.awayteamscore,
                                    hometeamid = EXCLUDED.hometeamid,
                                    hometeamname = EXCLUDED.hometeamname,
                                    hometeamscore = EXCLUDED.hometeamscore,
                                    venueid = EXCLUDED.venueid,
                                    venuename = EXCLUDED.venuename,
                                    scheduledinnings = EXCLUDED.scheduledinnings
                                WHERE EXCLUDED.gamedate > {table_name}.gamedate;"""

                            cur.execute(insert_sql)
                            cur.execute("TRUNCATE TABLE tmp_games;")
                            conn.commit()
                            print(f"Successfully loaded {f.name} into {table_name}")


    except Exception as e:
        print(f"Error in {f.name}: {e}")
        conn.rollback()

if __name__ == "__main__":
    load_games_csvs_to_postgres("../MLB_Data_2025.zip", "games.csv", "game", "config.yaml")