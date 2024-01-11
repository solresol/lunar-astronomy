import argparse

import ephem


def calculate_moon_position_and_phase(date_time):
    observer = ephem.Observer()
    observer.date = date_time
    moon = ephem.Moon(observer)
    return moon.alt, moon.az, moon.phase

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--when', required=True, help='ISO formatted date and time')
    parser.add_argument('--latitude', required=True, help='Observers latitude')
    parser.add_argument('--longitude', required=True, help='Observers longitude')
    args = parser.parse_args()

    alt, az, phase = calculate_moon_position_and_phase(args.when)
    print(f'Moon Position: Altitude = {alt}, Azimuth = {az}')
    print(f'Moon Phase: {phase}')

if __name__ == '__main__':
    main()
