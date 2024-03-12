
CREATE MATERIALIZED VIEW zone_trip_stats AS 
SELECT 
    (pickup_location.zone || ', ' || dropoff_location.zone) as trip,
    AVG(trip_distance) as avg_trip,
    MIN(trip_distance) as min_trip,
    MAX(trip_distance) as max_trip,
    COUNT(*) as trip_cnt
FROM trip_data
JOIN taxi_zone as pickup_location
    ON trip_data.pulocationid = pickup_location.location_id
JOIN taxi_zone as dropoff_location
    ON trip_data.dolocationid = dropoff_location.location_id
GROUP BY trip
    
DROP MATERIALIZED VIEW zone_trip_stats;

SELECT
    avg_trip,
    trip_cnt,
    trip
FROM zone_trip_stats
WHERE trip in ('Yorkville East, Steinway')
--WHERE trip in ('Yorkville East, Steinway', 'Murray Hill, Midwood', 'East Flatbush/Farragut, East Harlem North', 'Midtown Center, University Heights/Morris Heights')
ORDER by avg_trip DESC

SELECT
    avg_trip,
    min_trip,
    max_trip,
    trip_cnt,
    trip
FROM zone_trip_stats
WHERE trip in ('Yorkville East, Steinway', 'Murray Hill, Midwood', 'East Flatbush/Farragut, East Harlem North', 'Midtown Center, University Heights/Morris Heights')
ORDER by trip_cnt DESC

-- Question 3: Top 3 busiest zones

CREATE MATERIALIZED VIEW busiest_zones AS
    WITH max_trip AS (
    SELECT MAX(tpep_pickup_datetime) max_trip
    FROM trip_data
    )

    SELECT taxi_zone.zone as pickup_zone, COUNT(*) as trips
    FROM max_trip, trip_data
    JOIN taxi_zone
        ON trip_data.pulocationid = taxi_zone.location_id 
    WHERE trip_data.tpep_pickup_datetime > (max_trip - INTERVAL '17' HOUR)
    GROUP BY pickup_zone
    order by trips DESC
 

SELECT * FROM busiest_zones order by trips DESC;

