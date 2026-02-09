## BigQuery Setup
Create an external table using the Yellow Taxi Trip Records.
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).

1. BigQuery 콘솔 왼쪽 탐색기에서 내 프로젝트 ID 옆의 점 3개(...) 클릭
2. "데이터셋 만들기(Create dataset)" 클릭
3. 데이터셋 ID: ny_taxi (또는 원하시는 이름)
4. 위치(Location): US (중요! GCS 버킷이랑 같은 위치여야 합니다.)

```sql
-- external table
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://data-engineering-camp-yj_hw3/yellow_tripdata_2024-*.parquet'
  ]
);

-- materialized table
CREATE OR REPLACE TABLE `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned` AS
SELECT * FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024`;
```

---

## Question 1. Counting records
What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT COUNT(*) FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`;
```

- 20,332,093

## Question 2. Data read estimation
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
SELECT COUNT(DISTINCT PULocationID) FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID) FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`;
```

- external: 0B
- materialized: 155.12MB
- 0 MB for the External Table and 155.12 MB for the Materialized Table

## Question 3. Understanding columnar storage
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
Why are the estimated number of Bytes different?

```sql
SELECT PULocationID FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`;
SELECT PULocationID, DOLocationID FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`;
```

- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. Counting zero fare trips
How many records have a fare_amount of 0?

```sql
SELECT COUNT(*) FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned` WHERE fare_amount = 0;
```

- 8,333

## Question 5. Partitioning and clustering
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```sql
CREATE OR REPLACE TABLE `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024`;
```

- Partition by tpep_dropoff_datetime and Cluster on VendorID

## Question 6. Partition benefits
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
Choose the answer which most closely matches.

```sql
SELECT DISTINCT VendorID FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01 00:00:00' AND '2024-03-15 23:59:59';

SELECT DISTINCT VendorID FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01 00:00:00' AND '2024-03-15 23:59:59';
```

- materialized: 310.24MB
- partitioned & clustered: 26.84MB
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7. External table storage
Where is the data stored in the External Table you created?

- GCP Bucket

## Question 8. Clustering best practices
It is best practice in Big Query to always cluster your data:

- False
- 데이터가 너무 작을 때 (예: 1GB 미만): 클러스터링이나 파티셔닝을 관리하는 메타데이터 비용이 더 듭니다.
- 정렬이 필요 없는 경우: 쿼리에서 필터링(WHERE)이나 정렬(ORDER BY)을 거의 안 한다면 굳이 클러스터링을 할 필요가 없습니다.

## Question 9. Understanding table scans
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
SELECT COUNT(*) FROM `data-engineering-camp-486909.yellow_taxi_hw3.yellow_tripdata_2024_non_partitioned`;
```

- BigQuery stores metadata about the table including the total number of rows. 
- When we use COUNT(*), BigQuery does not scan the materialized table but reads from the metadata. 
- This is why the estimated bytes processed is '0B'.