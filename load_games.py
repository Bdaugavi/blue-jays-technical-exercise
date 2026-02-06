import csv
import psycopg
import os
import io
from dotenv import load_dotenv
import zipfile

load_dotenv("sample.env")

# Connection parameters
DATABASE_URL = os.getenv("DATABASE_URL")

def load_csv_to_neon(zip_path, table_name):
    """Read CSV file and load data into PostgreSQL table"""
    print(DATABASE_URL)
    # Connect to Neon database
    conn = psycopg.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:

            # Read CSV file
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                # Loop over all files in the zip
                for file_info in zipf.infolist():
                    # Check if the file is named games.csv (regardless of folder)
                    if file_info.filename.endswith("games.csv"):
                        # Open the file
                        with zipf.open(file_info) as f:
                            # Wrap binary stream in TextIOWrapper so COPY can read it as text
                            text_stream = io.TextIOWrapper(f, encoding="utf-8")

                            create_temp_sql = f"""
                            CREATE TEMP TABLE IF NOT EXISTS tmp_games (LIKE {table_name} INCLUDING DEFAULTS);
                            """

                            cur.execute(create_temp_sql)

                            copy_sql = """
                                       COPY tmp_games FROM STDIN WITH CSV HEADER NULL ''; \
                                       """

                            with cur.copy(copy_sql) as copy:
                                text_stream.seek(0)
                                while data := text_stream.read(8192):
                                    copy.write(data)

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
                            # reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
                            # headers = next(reader)  # Get column names from first row
                            #
                            # # Prepare insert query
                            # column_names = ', '.join(headers)
                            # placeholders = ', '.join(['%s'] * len(headers))
                            # insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                            #
                            # # Collect all rows
                            # rows = list(reader)
                            #
                            # # Execute batch insert
                            # cur.executemany(insert_query, rows)

                            conn.commit()
                            print(f"Successfully loaded {f.name} into {table_name}")

    except Exception as e:
        print(f"Error in {f.name}: {e}")
        #conn.rollback()

if __name__ == "__main__":
    load_csv_to_neon("MLB_Data_2025.zip", "game")