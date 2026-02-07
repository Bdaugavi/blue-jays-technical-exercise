import csv
from traceback import print_stack

import psycopg
import os
import io
from dotenv import load_dotenv
import zipfile

load_dotenv("sample.env")

# Connection parameters
DATABASE_URL = os.getenv("DATABASE_URL")

def load_csv_to_neon(zip_path, table_name, file_name):
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
                    if file_info.filename.endswith(file_name):
                        # Open the file
                        with zipf.open(file_info) as f:
                            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                            headers = reader.fieldnames  # Get column names from first row

                            # Read rows
                            current_gamepk = None
                            current_inning = None
                            home_score = None
                            away_score = None
                            rows_to_insert = []

                            for row in reader:
                                if current_inning is None or row['gamePk'] != current_gamepk:
                                    current_gamepk = row['gamePk']
                                    if int(row['inning']) != 1 or int(row['half']) != 0:
                                        raise ValueError(
                                            f"Missing data for inning 1 of game {current_gamepk} in file {f.name}."
                                        )
                                    home_score = 0
                                    away_score = 0

                                elif (
                                        (int(row['half']) == 0 and int(row['inning']) != current_inning + 1)
                                        or (int(row['half']) == 1 and int(row['inning']) != current_inning)
                                ):
                                    raise ValueError(
                                        f"Missing data for inning {current_inning} of game {current_gamepk} in file {f.name}."
                                    )

                                current_inning = int(row['inning'])

                                if int(row['half']) == 0:
                                    row['battingteam_score'] = away_score
                                    row['battingteam_score_diff'] = away_score - home_score
                                    if row['runs'] == '':
                                        row['runs'] = None
                                    else:
                                        away_score += int(row['runs'])

                                elif int(row['half']) == 1:
                                    row['battingteam_score'] = home_score
                                    row['battingteam_score_diff'] = home_score - away_score
                                    if row['runs'] == '':
                                        row['runs'] = None
                                    else:
                                        home_score += int(row['runs'])

                                rows_to_insert.append(row)

                            # ---- bulk insert happens here ----

                            if rows_to_insert:
                                columns = list(rows_to_insert[0].keys())
                                insert_column_names = ", ".join(columns)
                                placeholders = ", ".join(f"%({col})s" for col in columns)

                                insert_query = f"""
                                    INSERT INTO {table_name} ({insert_column_names})
                                    VALUES ({placeholders})
                                    ON CONFLICT (gamepk, inning, half) DO NOTHING;
                                """

                                cur.executemany(insert_query, rows_to_insert)

                            conn.commit()
                            print(f"Successfully loaded {f.name} into {table_name}")

    except Exception as e:
            print(f"Error in {f.name}: {e}")
            print_stack()
            print(e)
            conn.rollback()

if __name__ == "__main__":
    load_csv_to_neon("MLB_Data_2025.zip", "linescore", "linescores.csv")