create table sun_position (
   when_recorded timestamp with time zone default current_timestamp primary key,
   azimuth float,
   elevation float
);



create view sun_positions_rounded_off as
   SELECT date_trunc('hour', when_recorded) + interval '5 min' * round(date_part('minute', when_recorded) / 5.0) as when_recorded_rounded,
   avg(azimuth) as azimuth,
   avg(elevation) as elevation
   from sun_position group by 1;


create table moon_position (
   when_recorded timestamp with time zone default current_timestamp,
   right_ascension float,
   declination float,
   altitude float,
   azimuth float,
   phase float
);

create table weather (
   when_recorded timestamp with time zone default current_timestamp primary key,
   weather_main varchar,
   description varchar,
   temperature float,
   feels_like float,
   pressure float,
   humidity float,
   visibility float,
   wind_speed float,
   wind_gusts float,
   clouds float,
   sunrise bigint,
   sunset bigint,
   sunrise_tstamp timestamp with time zone generated always as (to_timestamp(sunrise)) stored,
   sunset_tstamp timestamp with time zone generated always as (to_timestamp(sunset)) stored
);

create view current_weather as
 select * from weather order by when_recorded desc limit 1;

----------------------------------------------------------------------

create materialized view production_rounded_off as
   SELECT date_trunc('hour', when_recorded) + interval '5 min' * round(date_part('minute', when_recorded) / 5.0) as when_recorded_rounded,
   avg(watts) as watts
   from production group by 1;

create view missing_sunpositions as
 select when_recorded_rounded from production_rounded_off
  where when_recorded_rounded not in (select when_recorded_rounded from sun_positions_rounded_off);

create view paint_the_sky as select
   production_rounded_off.when_recorded_rounded,
   watts,
   azimuth,
   elevation
  from production_rounded_off join sun_position on
    (
       production_rounded_off.when_recorded_rounded
	=
       date_trunc('hour', sun_position.when_recorded) + interval '5 min' * round(date_part('minute', sun_position.when_recorded) / 5.0)
       )
	;
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
 SELECT production_rounded_off.when_recorded_rounded 
   FROM production_rounded_off
  WHERE when_recorded_rounded NOT IN (SELECT when_recorded_rounded FROM moon_positions_rounded_off);