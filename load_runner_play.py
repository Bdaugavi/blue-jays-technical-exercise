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

def add_indicators_and_clean(row):
    row['is_firsttothird'] = (row['startbase'] == '1B' and row['reachedbase'] == '3B' and row['is_out'] == False and row['eventType'] != 'home_run')
    row['is_secondtohome'] = (row['startbase'] == '2B' and row['reachedbase'] == 'HM' and row['is_out'] == False and row['eventType'] != 'home_run')
    row['is_risp'] = (row['startbase'] in ['2B', '3B'])

    for k in ('originBase', 'outNumber', 'start','end','event','isOut','outBase','isScoringEvent',
              'rbi','earned','teamUnearned','responsiblepitcherid'):
        row.pop(k, None)



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
                            current_atbatindex = None
                            current_playindex = None
                            current_runnerid = None
                            previous_row = None
                            rows_to_insert = []

                            for row in reader:
                                if current_gamepk is None or row['gamePk'] != current_gamepk or row['atBatIndex'] != current_atbatindex \
                                    or row['playIndex'] != current_playindex or row['runnerid'] != current_runnerid:
                                    if previous_row is not None:
                                        add_indicators_and_clean(previous_row)
                                        rows_to_insert.append(previous_row)
                                    current_gamepk = row['gamePk']
                                    current_atbatindex = row['atBatIndex']
                                    current_playindex = row['playIndex']
                                    current_runnerid = row['runnerid']
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

                            # ---- bulk insert happens here ----
                            if previous_row:
                                add_indicators_and_clean(previous_row)
                                rows_to_insert.append(previous_row)
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
        print_stack()
        print(e)
        conn.rollback()

if __name__ == "__main__":
    load_csv_to_neon("MLB_Data_2025.zip", "runner_play", "runners.csv")