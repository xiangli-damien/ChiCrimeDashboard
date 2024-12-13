-- HQL Script for Managing Chicago Crime Data in HBase and Hive

-- Step 1: Drop and Create External Table for Raw Chicago Crime Data
DROP TABLE IF EXISTS xli0808_raw_chicago_crime;

CREATE EXTERNAL TABLE IF NOT EXISTS xli0808_raw_chicago_crime (
      id INT,                          -- Unique identifier
      case_number STRING,              -- Case number
      `date` STRING,                   -- Date and time of crime
      block STRING,                    -- Block information
      iucr STRING,                     -- Crime code
      primary_type STRING,             -- Type of crime
      description STRING,              -- Description of the crime
      location_description STRING,     -- Location description
      arrest BOOLEAN,                  -- Whether an arrest was made
      domestic BOOLEAN,                -- Whether it is a domestic crime
      beat STRING,                     -- Police beat
      district STRING,                 -- Police district
      ward INT,                        -- Ward
      community_area INT,              -- Community area
      fbi_code STRING,                 -- FBI code
      x_coordinate DOUBLE,             -- X coordinate
      y_coordinate DOUBLE,             -- Y coordinate
      year INT,                        -- Year of the crime
      updated_on STRING,               -- Last updated date
      latitude DOUBLE,                 -- Latitude
      longitude DOUBLE                 -- Longitude
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/xli0808/crime_data';

-- Step 2: Drop and Create External Table for Raw Police Stations Data
DROP TABLE IF EXISTS xli0808_raw_police_stations;

CREATE EXTERNAL TABLE IF NOT EXISTS xli0808_raw_police_stations (
                                                                    district STRING,                 -- District identifier
                                                                    district_name STRING,            -- Name of the police district
                                                                    address STRING,                  -- Address of the police station
                                                                    city STRING,                     -- City
                                                                    state STRING,                    -- State
                                                                    zip STRING,                      -- ZIP code
                                                                    website STRING,                  -- Website URL
                                                                    phone STRING,                    -- Phone number
                                                                    fax STRING,                      -- Fax number
                                                                    tty STRING,                      -- TTY (Teletypewriter) number
                                                                    x_coordinate DOUBLE,             -- X coordinate
                                                                    y_coordinate DOUBLE,             -- Y coordinate
                                                                    latitude DOUBLE,                 -- Latitude
                                                                    longitude DOUBLE,                -- Longitude
                                                                    location STRING                  -- Location in "(latitude, longitude)" format
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar"     = "\"",
        "escapeChar"    = "\\"
        )
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/xli0808/policestations'
    TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 3: Drop and Create External Table for Raw Police District Data
DROP TABLE IF EXISTS xli0808_raw_police_district;

CREATE EXTERNAL TABLE IF NOT EXISTS xli0808_raw_police_district (
                                                                    the_geom STRING,         -- Geometric data (MULTIPOLYGON)
                                                                    dist_label STRING,       -- District label
                                                                    dist_num STRING          -- District number
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar"     = "\"",
        "escapeChar"    = "\\"
        )
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/xli0808/policedistrict'
    TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 4: Clean Raw Chicago Crime Data and Save as ORC Format
DROP TABLE IF EXISTS xli0808_cleaned_chicago_crime;

CREATE TABLE xli0808_cleaned_chicago_crime STORED AS ORC AS
SELECT
    id,
    case_number,
    from_unixtime(unix_timestamp(`date`, 'MM/dd/yyyy hh:mm:ss a')) AS `date`, -- Convert date format
    block,
    iucr,
    primary_type,
    description,
    location_description,
    CAST(arrest AS BOOLEAN) AS arrest,    -- Ensure arrest is boolean
    CAST(domestic AS BOOLEAN) AS domestic, -- Ensure domestic is boolean
    district,
    beat,
    ward,
    community_area,
    fbi_code,
    x_coordinate,
    y_coordinate,
    year,
    from_unixtime(unix_timestamp(updated_on, 'MM/dd/yyyy hh:mm:ss a')) AS updated_on, -- Convert updated date format
    latitude,
    longitude
FROM xli0808_raw_chicago_crime
WHERE
    district RLIKE '^[0-9]+$' AND
    beat RLIKE '^[0-9]+$' AND
    arrest IN ('true', 'false') AND
    domestic IN ('true', 'false') AND
    unix_timestamp(`date`, 'MM/dd/yyyy hh:mm:ss a') IS NOT NULL AND
    unix_timestamp(updated_on, 'MM/dd/yyyy hh:mm:ss a') IS NOT NULL;

-- Step 5: Aggregate Monthly Crime Data
DROP TABLE IF EXISTS xli0808_project2_crime_monthly_agg;

CREATE TABLE xli0808_project2_crime_monthly_agg AS
SELECT
    CONCAT(
            CAST(YEAR(`date`) AS STRING),
            LPAD(CAST(MONTH(`date`) AS STRING), 2, '0'),
            '#',
            CASE WHEN arrest THEN '1' ELSE '0' END,
            '#',
            CASE WHEN domestic THEN '1' ELSE '0' END,
            '#',
            district
    ) AS rowkey, -- Generate HBase-compatible rowkey: YYYYMM#arrest#domestic#district
    COUNT(*) AS crime_count -- Count total crimes
FROM xli0808_cleaned_chicago_crime
GROUP BY
    YEAR(`date`),
    MONTH(`date`),
    arrest,
    domestic,
    district;

-- Step 6: Write Aggregated Data to HBase
DROP TABLE IF EXISTS xli0808_project2_crime_monthly_hbase;

CREATE EXTERNAL TABLE IF NOT EXISTS xli0808_project2_crime_monthly_hbase (
                                                                             rowkey STRING,         -- HBase rowkey: YYYYMM#arrest#domestic#district
                                                                             crime_count INT        -- Crime count
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        'hbase.columns.mapping' = ':key,cf:crime_count' -- Map HBase column family and columns
        )
    TBLPROPERTIES ('hbase.table.name' = 'xli0808_project2_crime_monthly_hbase');

INSERT OVERWRITE TABLE xli0808_project2_crime_monthly_hbase
SELECT
    rowkey,
    crime_count
FROM xli0808_project2_crime_monthly_agg;

-- End of HQL Script
