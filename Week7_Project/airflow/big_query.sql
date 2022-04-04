-- Query airine_external_table
SELECT Origin, Dest FROM
    `triple-water-339502.airline_delays.airline_external_table`
LIMIT 100;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `triple-water-339502.airline_delays.airline_non_partitoned` AS
SELECT * FROM `triple-water-339502.airline_delays.airline_external_table`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `triple-water-339502.airline_delays.airline_partitioned`
PARTITION BY
  RANGE_BUCKET(Month, GENERATE_ARRAY(1, 13)) AS
SELECT * FROM `triple-water-339502.airline_delays.airline_external_table`;

-- Impact of partition
-- Scanning over 2.63GB of data
SELECT ArrTime, DepTime 
FROM `triple-water-339502.airline_delays.airline_non_partitioned`
WHERE Month = 10;

-- Scanning over 231.51MB of data
SELECT ArrTime, DepTime 
FROM `triple-water-339502.airline_delays.airline_partitioned`
WHERE Month = 10;

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `airline_delays.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'airline_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `triple-water-339502.airline_delays.airline_partitioned_clustered`
PARTITION BY RANGE_BUCKET(Month, GENERATE_ARRAY(1, 13))
CLUSTER BY FlightNum AS
SELECT * FROM `triple-water-339502.airline_delays.airline_external_table`;

-- Impact of partition & Clustering
-- Scanning over 1.76GB of data
SELECT SUM(DepDelay) AS departure_delay
FROM `triple-water-339502.airline_delays.airline_partitioned`
WHERE FlightNum = 1603;

-- Impact of partition & Clustering
-- Scanning over 66.59MB of data
SELECT SUM(DepDelay) AS departure_delay
FROM `triple-water-339502.airline_delays.airline_partitioned_clustered`
WHERE FlightNum = 1603;