USE default;

ADD JAR /opt/hive/lib/opencsv-5.9.jar;

DROP TABLE IF EXISTS map_reduce_output;

CREATE EXTERNAL TABLE map_reduce_output (
    park_id STRING,
    `date` STRING,
    visits_count INT,
    avg_group_size DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:INPUT_MR}';

DROP TABLE IF EXISTS parks_dictionary;

CREATE EXTERNAL TABLE parks_dictionary (
    park_id STRING,
    name STRING,
    region STRING,
    facilities STRING,
    attractions STRING,
    nature_types STRING,
    established_year STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)

STORED AS TEXTFILE
LOCATION '${hiveconf:INPUT_PARKS}'
TBLPROPERTIES("skip.header.line.count"="1");

DROP TABLE IF EXISTS results;

CREATE EXTERNAL TABLE results (
    nature_type STRING,
    region STRING,
    total_visits INT,
    avg_group_size DOUBLE,
    visits_deviation DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hiveconf:OUTPUT}';

WITH exploded_dictionary AS (
    SELECT
        park_id,
        region,
        TRIM(single_nature_type) AS nature_type
    FROM
        parks_dictionary
            LATERAL VIEW explode(split(nature_types, ';')) exploded_table AS single_nature_type
),
     region_aggregation AS (
         SELECT
             d.nature_type,
             d.region,
             COALESCE(SUM(m.visits_count), 0) AS total_visits,
             COALESCE(SUM(m.visits_count * m.avg_group_size) / NULLIF(SUM(m.visits_count), 0), 0) AS avg_group_size
         FROM
             exploded_dictionary d
                 LEFT JOIN
             map_reduce_output m ON d.park_id = m.park_id
         GROUP BY
             d.nature_type,
             d.region
     )
INSERT OVERWRITE TABLE results
SELECT
    nature_type,
    region,
    total_visits,
    avg_group_size,
    total_visits - AVG(total_visits) OVER (PARTITION BY nature_type) AS visits_deviation
FROM
    region_aggregation;