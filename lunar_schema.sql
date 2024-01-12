-- lunar_schema.sql

CREATE VIEW moon_positions_rounded_off AS
   SELECT date_trunc('hour', when_recorded) + interval '5 min' * round(date_part('minute', when_recorded) / 5.0) AS when_recorded_rounded,
   AVG(right_ascension) AS right_ascension,
   AVG(declination) AS declination,
   AVG(altitude) AS altitude,
   AVG(azimuth) AS azimuth,
   AVG(phase) AS phase
   FROM moon_position GROUP BY 1;

CREATE VIEW missing_moonpositions AS
 SELECT when_recorded_rounded FROM moon_positions_rounded_off
  WHERE when_recorded_rounded NOT IN (SELECT when_recorded_rounded FROM moon_positions_rounded_off);
