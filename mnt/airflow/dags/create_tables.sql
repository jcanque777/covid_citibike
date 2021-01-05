DROP TABLE IF EXISTS public.covid;
DROP TABLE IF EXISTS public.bike;
DROP TABLE IF EXISTS public.weather;
DROP TABLE IF EXISTS public.stations;
DROP TABLE IF EXISTS public.dates;

CREATE TABLE IF NOT EXISTS public.covid (
    week_ending date,
    zip_code int, 
    case_rate numeric
);

CREATE TABLE IF NOT EXISTS public.bike (
    date date,
    hour int,
    start_station_name varchar,
    end_station_name varchar,
    ride_counts int
);

CREATE TABLE IF NOT EXISTS public.weather (
    date date,
    hour int,
    report_type varchar,
    HourlyDryBulbTemperature numeric,
    HourlyRelativeHumidity numeric,
    HourlyPrecipitation numeric,
    HourlyWindSpeed numeric
);


CREATE TABLE IF NOT EXISTS public.stations(
    station_name varchar,
    station_latitude numeric,
    station_longitude numeric,
    station_zip int
);

CREATE TABLE IF NOT EXISTS public.dates(
    week_ending date,
    date date,
    day_of_week int
)
