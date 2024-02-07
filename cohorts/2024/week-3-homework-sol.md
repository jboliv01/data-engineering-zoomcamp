# Week 2 Homework Solution
## Author: Jonah Oliver

1. 840,402
2. 0 B processed for External Table and 6.41 MB processed for Materialized Table
3. 1622
4. Partition by lpep_pickup_datetime and cluster on PULocationID
5. 12.82 MB processed for non-partitioned Table and 1.12 MB processed for partitioned Table
6. GCP Bucket
7. False
8. 0 bytes due to the query being cached
   
# Solution Queries

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-jonah-oliver/green/green_tripdata_2022-*.parquet']
);

-- Check green trip data test
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
-- create materialized table 
CREATE OR REPLACE TABLE dtc-de-zoomcamp-410523.ny_taxi.materialized_green_tripdata 
AS SELECT * FROM dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata;

SELECT COUNT(DISTINCT PULocationID) as distinctPickup FROM dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata;
-- 0 B processed 
SELECT COUNT(DISTINCT PULocationID) as distinctPickup FROM dtc-de-zoomcamp-410523.ny_taxi.materialized_green_tripdata;
-- 6.41 MB processed 

-- Question 3
SELECT COUNT(*) FROM dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitioned
WHERE fare_amount = 0;
-- 1622

-- Question 4
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata`;
-- Partition by lpep_pickup_datetime and cluster on PULocationID

-- Question 5
SELECT DISTINCT(PULocationID) as distinctPickup FROM dtc-de-zoomcamp-410523.ny_taxi.materialized_green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- 12.82 MB processed

SELECT DISTINCT(PULocationID) as distinctPickup FROM dtc-de-zoomcamp-410523.ny_taxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- 1.12 MB processed

-- Question 6
-- GCP Bucket 

-- Question 7
-- FALSE

-- Question 8
SELECT count(*) as query FROM dtc-de-zoomcamp-410523.ny_taxi.materialized_green_tripdata;
-- 0 bytes due to the query being cached
```