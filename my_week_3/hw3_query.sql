-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-jonah-oliver/green/green_tripdata_2022-*.parquet']
);

-- Check green trip data
SELECT * FROM dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata;

SELECT * FROM dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitioned LIMIT 100;

-- Question 1
SELECT count(*) as trips
FROM dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitioned;
-- 840,402

-- Question 2

-- Question 3
SELECT COUNT(*) FROM dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitioned
WHERE fare_amount = 0;
-- 1622

-- Question 4

-- Question 5

-- Question 6

-- Question 7