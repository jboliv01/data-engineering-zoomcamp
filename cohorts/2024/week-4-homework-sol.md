# Week 2 Homework Solution
## Author: Jonah Oliver
   
1. It applies a limit 100 to all of our models, considering our `fact_trips` model references both staging tables, it too will have a limit imposed.
2. The code from a dev branch requesting a merge to main
3. I got 43261276, closet answer is 42998722. I used parquet files from the web.
4. Yellow has the most rides at 109247536.


# Solution Queries

```sql
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoomcamp-410523.ny_taxi.external_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-jonah-oliver/ny_taxi_data/service=fhv/year=2019/*.parquet']
);


SELECT COUNT(*) FROM `dtc-de-zoomcamp-410523.ny_taxi.external_fhv_tripdata`; -- 43261276
SELECT COUNT(*) FROM `dtc-de-zoomcamp-410523.ny_taxi.external_green_tripdata`; -- 8035161
SELECT COUNT(*) FROM `dtc-de-zoomcamp-410523.ny_taxi.external_yellow_tripdata`; -- 109247536
```