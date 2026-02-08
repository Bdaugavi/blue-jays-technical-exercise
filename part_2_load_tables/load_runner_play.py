import csv
import psycopg
import io
import zipfile
import yaml


def add_indicators_and_clean(row):
    """
    After all the records for a play and runner are processed, calculate the fields is_firstrothird,
    is_secondtohome and is_risp using the startbase, reachedbase, is_out and eventType
    and add them to the row.  Then remove fields not needed in the final table.
    """
    row['is_firsttothird'] = (row['startbase'] == '1B' and row['reachedbase'] == '3B' and row['is_out'] == False and row['eventType'] != 'home_run')
    row['is_secondtohome'] = (row['startbase'] == '2B' and row['reachedbase'] == 'HM' and row['is_out'] == False and row['eventType'] != 'home_run')
    row['is_risp'] = (row['startbase'] in ['2B', '3B'])
    if row['playId'] == "":
        row["playId"] = None

    for k in ('originBase', 'outNumber', 'start','end','event','isOut','outBase','isScoringEvent',
              'rbi','earned','teamUnearned','responsiblepitcherid'):
        row.pop(k, None)



def load_runner_csvs_to_postgres(zip_path, table_name, file_name, yaml_name):
    """Read CSV files and load data into PostgreSQL table"""
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

                            # Keep track of the game, at bat, play and runner currently being processed
                            current_gamepk = None
                            current_atbatindex = None
                            current_playindex = None
                            current_runnerid = None
                            previous_row = None
                            rows_to_insert = []

                            for row in reader:
                                #If the runner is a new runner, then finalize the previous runner's row and reset the current row
                                if current_gamepk is None or row['gamePk'] != current_gamepk or row['atBatIndex'] != current_atbatindex \
                                    or row['playIndex'] != current_playindex or row['runnerid'] != current_runnerid:
                                    if previous_row is not None:
                                        add_indicators_and_clean(previous_row)
                                        rows_to_insert.append(previous_row)
                                    current_gamepk = row['gamePk']
                                    current_atbatindex = row['atBatIndex']
                                    current_playindex = row['playIndex']
                                    current_runnerid = row['runnerid']
                                    #Set the startbase and endbase values based on the originBase,
                                    # and outBase or reachedBase depending on if the runner is out
                                    row['is_out'] = (row['isOut'] == "True")
                                    if row['originBase'] != '':
                                        row['startbase'] = row['originBase']
                                    else:
                                        row['startbase'] = 'B'

                                    if row['is_out']:
                                        row['endbase'] = row['outBase']
                                        row['reachedbase'] = None
                                    else:
                                        if row['end'] == 'score':
                                            row['endbase'] = 'HM'
                                            row['reachedbase'] = 'HM'
                                        else:
                                            row['endbase'] = row['end']
                                            row['reachedbase'] = row['end']
                                #If this row is an additional record for the same play and runner, then
                                #update the endbase and reachedbase to be the farthest one
                                #Use the earliest eventtype, and the latest movementreason
                                else:
                                    current_out = (row['isOut'] == 'True')
                                    row['startbase'] = previous_row['startbase']
                                    if current_out:
                                        row['endbase'] = row['outBase']
                                        row['reachedbase'] = previous_row['reachedbase']
                                    else:
                                        if row['end'] == 'score':
                                            row['endbase'] = 'HM'
                                            row['reachedbase'] = 'HM'
                                            #Latest movement reason kept
                                        else:
                                            if row['end'] >=  previous_row['endbase']:
                                                row['endbase'] = row['end']
                                                row['reachedbase'] = row['end']
                                                #Movement reason remains from current row (latest)
                                            else:
                                                row['endbase'] = previous_row['endbase']
                                                row['reachedbase'] = previous_row['endbase']
                                                row['movementReason'] = previous_row['movementReason']

                                            row['endbase'] = max(row['end'], previous_row['endbase'])
                                            row['reachedbase'] = max(row['end'], previous_row['endbase'])


                                    if previous_row['start'] < row['start']:
                                        row['eventType'] = previous_row['eventType'] #Keep the earliest eventType
                                    row ['is_out'] = row['isOut'] or previous_row ['is_out']

                                previous_row = row

                            #Add the last row to the list
                            if previous_row:
                                add_indicators_and_clean(previous_row)
                                rows_to_insert.append(previous_row)
                            # Insert all the rows from the file
                            if rows_to_insert:
                                columns = list(rows_to_insert[0].keys())
                                insert_column_names = ", ".join(columns)
                                placeholders = ", ".join(f"%({col})s" for col in columns)
                                insert_query = f"""
                                    INSERT INTO {table_name} ({insert_column_names})
                                    VALUES ({placeholders})
                                    ON CONFLICT  (gamepk, atbatindex, playindex, runnerid) DO NOTHING;
                                """
                                cur.executemany(insert_query, rows_to_insert)
                            conn.commit()
                            print(f"Successfully loaded {f.name} into {table_name}")

    except Exception as e:
        print(f"Error in {f.name}: {e}")
        conn.rollback()

if __name__ == "__main__":
    load_runner_csvs_to_postgres("../MLB_Data_2025.zip", "runner_play", "runners.csv", "config.yaml")