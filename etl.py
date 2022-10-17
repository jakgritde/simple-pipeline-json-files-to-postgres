import os
import configparser

import psycopg2
from psycopg2._psycopg import AsIs
from psycopg2.extensions import register_adapter
import pandas as pd
import numpy as np

register_adapter(np.int64, AsIs)

def get_files(path):
    """ Get all file paths in folder
    """
    all_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            all_files.append(os.path.join(root, file))
    
    return all_files

def process_song_data():
    """ Extract data from song_data folder
    """
    song_files = get_files('data/song_data')

    dfs = []
    for file in song_files:
        df = pd.read_json(file, lines=True)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    return df
    

def process_log_data():
    """ Extract data from log_data folder
    """
    log_files = get_files('data/log_data')

    dfs = []
    for file in log_files:
        df = pd.read_json(file, lines=True)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    # clean column userID
    df['userId'] = df['userId'].astype(str).str.replace(r'[\D]', '', regex=True)
    df.drop(df.loc[df['userId'] == ''].index, inplace=True)
    return df

def insert_into_tables(conn, cur):

    # Read song data
    df1 = process_song_data()

    # Insert into songs Table
    songs_df = df1[['song_id', 'title', 'artist_id', 'year', 'duration']]
    songs_df

    for i, row in songs_df.iterrows():
        sql = """
        INSERT INTO songs VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (row['song_id'], row['title'], row['artist_id'], row['year'], row['duration']))
        conn.commit() # Make the changes to the database
    
    # Insert into artists Table
    artists_df = df1[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artists_df = artists_df.drop_duplicates(subset=['artist_id'])

    for i, row in artists_df.iterrows():
        sql = """
        INSERT INTO artists VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (row['artist_id'], row['artist_name'], row['artist_location'], row['artist_latitude'], row['artist_longitude']))
        conn.commit()
    
    # Read log data
    df2 = process_log_data()

    # Insert into time Table
    t = pd.to_datetime(df2['ts'], unit='ms')
    t

    time_df = pd.concat([df2['ts'], t.dt.hour, t.dt.day, t.dt.weekday, t.dt.month, t.dt.year], axis=1)
    time_df.columns = ['unix_timestamp', 'hour', 'day', 'weekday', 'month', 'year']
    for i, row in time_df.iterrows():
        sql = """
        INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (row['unix_timestamp'], row['hour'], row['day'], row['weekday'], row['month'], row['year']))
        conn.commit()

    # Insert into time Table
    user_df = df2[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df

    for i, row in user_df.iterrows():
        sql = """
        INSERT INTO users VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (row['userId'], row['firstName'], row['lastName'], row['gender'], row['level']))
        conn.commit()

    # Insert into songplays Table
    for index, row in df2.iterrows():

        sql = """
        SELECT songs.id, artists.id FROM songs
        JOIN artists ON songs."artistID"=artists.id 
        WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s;
        """
        cur.execute(sql, (row['song'], row['artist'], row['length']))
        result = cur.fetchone()

        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None
        
        sql = """
            INSERT INTO songplays VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(sql, (index, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent']))
        conn.commit()

def main():
    """ Data insertion to PostgreSQL
    """
    config = configparser.ConfigParser()
    config.read('psqlconfig.ini')

    host = config['postgres']['host']
    port = config['postgres']['port']
    user = config['postgres']['user']
    passwd = config['postgres']['passwd']
    db = config['postgres']['db']

    conn_string = f'dbname={db} user={user} password={passwd} host={host} port={port}'
    conn = psycopg2.connect(conn_string)
    
    with conn.cursor() as cur:
        insert_into_tables(conn, cur)

if __name__ == "__main__":
    main()