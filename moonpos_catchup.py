#!/usr/bin/env python3

import argparse
import configparser

import psycopg2
from moonpos import calculate_moon_position_and_phase

parser = argparse.ArgumentParser()
parser.add_argument("--config", default='config.ini')
parser.add_argument("--progress", action="store_true")
parser.add_argument("--skip-refresh", action="store_true",
                    help="Skip running a time-consuming refresh materialized view for electricity production data")
args = parser.parse_args()

c = configparser.ConfigParser()
c.read(args.config)
latitude = c['location']['latitude']
longitude = c['location']['longitude']
dbname = c['database']['dbname']
user = c['database']['user']
password = c['database']['password']
host = c['database']['host']
port = c['database']['port']
conn = psycopg2.connect(f'dbname={dbname} user={user} password={password} host={host} port={port}')
write_cursor = conn.cursor()
read_cursor = conn.cursor()

if not args.skip_refresh:
    write_cursor.execute("refresh materialized view production_rounded_off")
    conn.commit()

read_cursor.execute("select when_recorded_rounded AT TIME ZONE 'gmt' as gmt_when, when_recorded_rounded from missing_moonpositions order by 1")

location = (float(latitude), float(longitude))
iterator = read_cursor
if args.progress:
    import tqdm
    iterator = tqdm.tqdm(iterator, total=read_cursor.rowcount)
for row in iterator:
    gmt_when = row[0]
    when_recorded = row[1]
    altitude, azimuth, phase = calculate_moon_position_and_phase(
        (gmt_when.year, gmt_when.month, gmt_when.day, gmt_when.hour, gmt_when.minute, gmt_when.second, 0),
        location)
    write_cursor.execute("insert into moon_position (when_recorded, altitude, azimuth, phase) values (%s, %s, %s, %s)", [when_recorded, altitude, azimuth, phase])
    conn.commit()
