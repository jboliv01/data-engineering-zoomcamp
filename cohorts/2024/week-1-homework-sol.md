# Week 2 Homework Solution
## Author: Jonah Oliver

1. --rm
2. 0.42.0
3. 15612
4. 2019-09-26

### QUERY USED: 

```sql
SELECT DATE(lpep_pickup_datetime) as pickupdate, MAX(trip_distance) as top_dist
FROM public.yellow_txi_data
GROUP BY pickupdate
ORDER BY top_dist DESC
```

5. "Bronx" "Manhattan" "Queens"

### QUERY USED: 
```sql
SELECT "Borough", SUM(total_amount) 
FROM public.yellow_txi_data
INNER JOIN 
taxi_zone_data 
ON "PULocationID" = "LocationID"
WHERE "Borough" <> 'Unknown'
AND DATE(lpep_pickup_datetime) = '2019-09-18'
GROUP BY "Borough"
HAVING SUM(total_amount) > 50000
```

6. JFK Airport

### QUERY USED: 

```sql
SELECT pickup."Zone", dropoff."Zone", MAX(tip_amount) 
FROM public.yellow_txi_data
INNER JOIN 
taxi_zone_data as pickup
ON "PULocationID" = pickup."LocationID"
INNER JOIN 
taxi_zone_data as dropoff
ON "DOLocationID" = dropoff."LocationID"
WHERE pickup."Zone" = 'Astoria'
AND (DATE(lpep_pickup_datetime) >= '2019-09-01'
AND DATE(lpep_pickup_datetime) <= '2019-09-30')
GROUP BY pickup."Zone", dropoff."Zone"
ORDER BY max(tip_amount) DESC
```

7. Results of terraform apply

```shell
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo_bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/data-en-404300/datasets/demo_dataset]
google_storage_bucket.demo_bucket: Creation complete after 2s [id=terraform-demo-terra-bucket-1]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```
