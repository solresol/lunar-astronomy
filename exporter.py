import argparse
import configparser
import sqlite3
import datetime

import psycopg2


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgresql-database-config")
    parser.add_argument("--start-timestamp", default='1970-01-01 00:00:00')
    parser.add_argument("--end-timestamp", default='2025-01-12 05:59:49')
    parser.add_argument("--sqlite-database")
    return parser.parse_args()

def connect_postgresql(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    dbname = config['database']['dbname']
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    return psycopg2.connect(f'dbname={dbname} user={user} password={password} host={host} port={port}')

def connect_sqlite(database):
    return sqlite3.connect(database)

def query_postgresql(conn, start_timestamp, end_timestamp):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT when_recorded, clouds FROM weather WHERE when_recorded BETWEEN '{start_timestamp}' AND '{end_timestamp}';
        SELECT when_recorded_rounded, watts, moon_azimuth, moon_altitude, moon_phase, sun_azimuth, sun_altitude
        FROM production_rounded_off
        JOIN sun_positions ON production_rounded_off.when_recorded_rounded = sun_positions.when_recorded_rounded
        JOIN moon_positions ON production_rounded_off.when_recorded_rounded = moon_positions.when_recorded_rounded
        WHERE when_recorded_rounded BETWEEN '{start_timestamp}' AND '{end_timestamp}';
    """)
    return cursor.fetchall()

def write_sqlite(conn, data):
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO weather (when_recorded, clouds) VALUES (?, ?);
        INSERT INTO astronomy (when_recorded_rounded, watts, moon_azimuth, moon_altitude, moon_phase, sun_azimuth, sun_altitude) VALUES (?, ?, ?, ?, ?, ?, ?);
    """, data)
    conn.commit()

def main():
    args = parse_arguments()
    pg_conn = connect_postgresql(args.postgresql_database_config)
    sqlite_conn = connect_sqlite(args.sqlite_database)
    data = query_postgresql(pg_conn, args.start_timestamp, args.end_timestamp)
    write_sqlite(sqlite_conn, data)

if __name__ == "__main__":
    main()
