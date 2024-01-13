#!/usr/bin/env python3
import argparse
import configparser
import sqlite3
import datetime
import os
import sys
import psycopg2


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgresql-database-config", default='config.ini', help="Path to the PostgreSQL database configuration file")
    parser.add_argument("--start-timestamp", default='1970-01-01 00:00:00', help="Start timestamp for querying the PostgreSQL database (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-timestamp", default='2025-01-12 05:59:49', help="End timestamp for querying the PostgreSQL database (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--sqlite-database", help="Path to the SQLite database file where the data will be exported")
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
        SELECT pr.when_recorded_rounded, pr.watts, mp.moon_azimuth, mp.moon_altitude, mp.moon_phase, sp.azimuth, sp.elevation
    FROM production_rounded_off pr
    JOIN sun_position sp ON pr.when_recorded_rounded = date_trunc('hour', sp.when_recorded) + interval '5 min' * round(date_part('minute', sp.when_recorded) / 5.0)
    JOIN moon_positions mp ON pr.when_recorded_rounded = date_trunc('hour', mp.when_recorded) + interval '5 min' * round(date_part('minute', mp.when_recorded) / 5.0)
    WHERE pr.when_recorded_rounded BETWEEN '{start_timestamp}' AND '{end_timestamp}';
    """)
    return cursor.fetchall()

def write_sqlite(conn, data):
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO weather (when_recorded, clouds) VALUES (?, ?);
        INSERT INTO astronomy (when_recorded_rounded, watts, moon_azimuth, moon_altitude, moon_phase, sun_azimuth, sun_altitude) VALUES (?, ?, ?, ?, ?, ?, ?);
    """, data)
    conn.commit()

def check_config_file_exists(config_file):
    if not os.path.isfile(config_file):
        print("Error: PostgreSQL database configuration file does not exist.")
        sys.exit(1)

def main():
    args = parse_arguments()
    check_config_file_exists(args.postgresql_database_config)
    pg_conn = connect_postgresql(args.postgresql_database_config)
    sqlite_conn = connect_sqlite(args.sqlite_database)
    data = query_postgresql(pg_conn, args.start_timestamp, args.end_timestamp)
    write_sqlite(sqlite_conn, data)

if __name__ == "__main__":
    main()