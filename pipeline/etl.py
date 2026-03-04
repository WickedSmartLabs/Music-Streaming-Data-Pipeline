import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a single song JSON file and inserts song and artist
    records into the Sparkify database.
    """
    print(f"Processing song file: {filepath}")
    # open song file
    df = pd.read_json(filepath, typ='series').to_frame().T

    # insert song record
    song_data = (
        df['song_id'][0],
        df['title'][0],
        df['artist_id'][0],
        df['year'][0],
        df['duration'][0]
    )
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (
        df['artist_id'][0],
        df['artist_name'][0],
        df['artist_location'][0],
        df['artist_latitude'][0],
        df['artist_longitude'][0]
    )
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Processes a single log JSON file and inserts time, user,
    and songplay records into the Sparkify database.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = {
        'start_time': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.isocalendar().week.astype(int),
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    }

    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for _, row in time_df.iterrows():
        time_data = (
            row['start_time'],
            row['hour'],
            row['day'],
            row['week'],
            row['month'],
            row['year'],
            row['weekday']
        )
        cur.execute(time_table_insert, time_data)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
    user_df = user_df[user_df['userId'].notnull()]
    user_df = user_df[user_df['userId'] != 0]
    user_df['userId'] = user_df['userId'].astype(int)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            int(row.userId),
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        )

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates over all JSON files in a directory and applies the
    given processing function to each file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Establishes database connection and runs the full ETL pipeline
    for song and log data.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=Nike1234")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
