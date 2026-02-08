import csv
import psycopg
import io
import zipfile
import yaml

def load_linescore_csvs_to_postgres(zip_path, table_name, file_name, yaml_name):
    """Read CSV files from zip, calculate the cumulative score each half inning
     and load data into PostgreSQL table"""
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
                            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                            # Keep track of the game, inning and cumulative score wihle reading records
                            current_gamepk = None
                            current_inning = None
                            home_score = None
                            away_score = None
                            rows_to_insert = []

                            for row in reader:
                                #New game
                                if current_inning is None or row['gamePk'] != current_gamepk:
                                    current_gamepk = row['gamePk']
                                    #Validate first record is the top of 1st
                                    if int(row['inning']) != 1 or int(row['half']) != 0:
                                        raise ValueError(
                                            f"Missing data for inning 1 of game {current_gamepk} in file {f.name}."
                                        )
                                    #Reset score
                                    home_score = 0
                                    away_score = 0
                                #Validate innings are processed in order
                                elif (
                                        (int(row['half']) == 0 and int(row['inning']) != current_inning + 1)
                                        or (int(row['half']) == 1 and int(row['inning']) != current_inning)
                                ):
                                    raise ValueError(
                                        f"Missing data for inning {current_inning} of game {current_gamepk} in file {f.name}."
                                    )

                                current_inning = int(row['inning'])
                                #Calculate score for batting team depending on if it is top or bottom
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

                            # Insert all the rows from the file
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
            conn.rollback()

if __name__ == "__main__":
    load_linescore_csvs_to_postgres("../MLB_Data_2025.zip", "linescore", "linescores.csv", "config.yaml")