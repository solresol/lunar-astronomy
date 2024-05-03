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
    default_end_timestamp = datetime.datetime.now() + datetime.timedelta(days=365)
    parser.add_argument("--end-timestamp", default=default_end_timestamp.strftime('%Y-%m-%d %H:%M:%S'), help="End timestamp for querying the PostgreSQL database (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--astronomy-csv", default=None, help="Path to the CSV file where astronomy data will be saved")
    parser.add_argument("--weather-csv", default=None, help="Path to the CSV file where weather data will be saved")
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
        SELECT when_recorded, clouds FROM weather WHERE when_recorded BETWEEN '{start_timestamp}' AND '{end_timestamp}'
    """)
    weather_data = cursor.fetchall()

    cursor.execute(f"""
        SELECT pr.when_recorded_rounded, pr.watts, mp.azimuth, mp.altitude, mp.phase, sp.azimuth, sp.elevation
        FROM production_rounded_off pr
        JOIN sun_position sp ON pr.when_recorded_rounded = sp.when_recorded
        JOIN moon_position mp ON pr.when_recorded_rounded = mp.when_recorded
        WHERE pr.when_recorded_rounded BETWEEN '{start_timestamp}' AND '{end_timestamp}'
    """)
    astronomy_data = cursor.fetchall()
    return weather_data, astronomy_data

def write_sqlite(conn, weather_data, astronomy_data):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        when_recorded TIMESTAMP,
        clouds FLOAT
    )
    """)
    conn.commit()
    cursor.execute("DELETE FROM weather")
    conn.commit()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS astronomy (
        when_recorded_rounded TIMESTAMP,
        watts FLOAT,
        moon_azimuth FLOAT,
        moon_altitude FLOAT,
        moon_phase FLOAT,
        sun_azimuth FLOAT,
        sun_altitude FLOAT
    )
    """)
    conn.commit()
    cursor.execute("DELETE FROM astronomy")
    conn.commit()
    for i, record in enumerate(weather_data):
        cursor.execute("INSERT INTO weather_data (when_recorded, clouds) VALUES (?, ?)", record)
        if i % 1000 == 0:
            conn.commit()
    for i, record in enumerate(astronomy_data):
        cursor.execute("INSERT INTO astronomy (when_recorded_rounded, watts, moon_azimuth, moon_altitude, moon_phase, sun_azimuth, sun_altitude) VALUES (?, ?, ?, ?, ?, ?, ?)", record)
        if i % 1000 == 0:
            conn.commit()
    conn.commit()

def check_config_file_exists(config_file):
    if not os.path.isfile(config_file):
        print("Error: PostgreSQL database configuration file does not exist.")
        sys.exit(1)

def write_astronomy_csv(astronomy_data, csv_path):
    import csv
    headers = ['when_recorded_rounded', 'watts', 'moon_azimuth', 'moon_altitude', 'moon_phase', 'sun_azimuth', 'sun_elevation']
    with open(csv_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)
        for row in astronomy_data:
            csvwriter.writerow(row)


def write_weather_csv(weather_data, csv_path):
    import csv
    headers = ['when_recorded', 'clouds']
    with open(csv_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)
        for row in weather_data:
            csvwriter.writerow(row)

def main():
    args = parse_arguments()
    check_config_file_exists(args.postgresql_database_config)
    pg_conn = connect_postgresql(args.postgresql_database_config)
    weather_data, astronomy_data = query_postgresql(pg_conn, args.start_timestamp, args.end_timestamp)
    if args.sqlite_database:
        sqlite_conn = connect_sqlite(args.sqlite_database)
        write_sqlite(sqlite_conn, weather_data, astronomy_data)
    if args.weather_csv:
        write_weather_csv(weather_data, args.weather_csv)
    if args.astronomy_csv:
        write_astronomy_csv(astronomy_data, args.astronomy_csv)

if __name__ == "__main__":
    main()
