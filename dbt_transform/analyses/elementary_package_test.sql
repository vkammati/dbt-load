
-- Elementary package documentation
-- https://github.com/sede-x/Template-EDP-DBT-Reference-Pipeline/wiki/DBT-packages#elementary

-- Elementary Package Test Has 2 Parts

--1. Schema Tests
  --1.a Schema Changes
--2. Anomaly Detections
  --2.a Volume Anomalies
  --2.b Freshness Anomalies
  --2.c Column Anomalies

-- PART 1 (Schema Tests)

--First of all, to be able to run elementary report on your local, you will need some configuration in the dbt_project.yml file. Please configure these two line like below:
--line 51:   +enabled: True #"{{ env_var('USER', default='')[0:4] in ['root','spar'] }}"
--line 102:  prefix_schema: False # Hard coded, Databricks workflow set to false.

-- Set schema_name (Recommended: keep the same)
{% set schema_name_raw = 'dbt_example_raw' %}
{% set schema_name_euh_uh = 'edp_dbt_ref_pipeline_example' %}

 -- Don't change the table name
{% set source_table = env_var("DBX_UNITY_CATALOG") +'.'+ schema_name_raw + '.raw_country' %}
{% set euh_table = env_var("DBX_UNITY_CATALOG") +'.'+ schema_name_euh_uh + '.euh_country' %}
{% set eh_table = env_var("DBX_UNITY_CATALOG") +'.'+ schema_name_euh_uh + '.eh_country' %}


-- Execute dbt in your terminal
dbt compile -s elementary_package_test
-- Copy and past the log result into you Databricks SQL editor
-- Follow the steps from there


------ Part 1 Step 1
drop table if exists {{source_table}};
drop table if exists {{euh_table}};
drop table if exists {{eh_table}};



-- Creates a Delta table

CREATE TABLE {{ source_table }} (name STRING, code INT, iso STRING);
insert into {{ source_table }} values ('Netherlands' as name, 31 as code, 'NL' as iso);

-- Check raw table
select * from {{ source_table }};


------ Part 1 Step 2 ( First Look Structure Overview)
-- Create an elementary folder like "dbt_transform/models/elementary"
-- Under elementary folder create a yml file "dbt_transform/models/elementary/source_elementary.yml" as below
-- and again under elementary folder create two new sub folders "dbt_transform/models/elementary/enriched_unharmonized" and "dbt_transform/models/elementary/enriched_harmonized"
-- Under these new 2 sub folders create a yml file and a sql file for each sub folder.

--Structure Overview should be like below

├── macros
│   └── elementary
│        ├── source_elementary.yml
│        ├── enriched_unharmonized
│        │     ├── euh_country.yml
│        │     ├── euh_country.sql
│        ├── enriched_harmonized
│        │     ├── eh_country.yml
│        │     ├── eh_country.sql


-- Remove the comment / forward slash

-- Create a yml file "dbt_transform/models/elementary/source_elementary.yml" as below
--start-source_elementary.yml-------------------------------------------

version: 2

sources:
  - name: raw_country
    schema: dbt_example_raw
    catalog: "{{ env_var('DBX_UNITY_CATALOG') }}"
    tables:
      - name: raw_country
        data_tests:
          - elementary.schema_changes:    # Fails on changes in schema: deleted or added columns, or change of data type of a column.
              tags: ["schema_changes"]    # for special testing purposes  -> dbt test --select tag:schema_changes
              config:
                severity: warn   # warn | error

--end-source_elementary.yml----------------------------------------------

-- Create a sql file "dbt_transform/models/elementary/enriched_unharmonized/euh_country.sql" as below
--start-euh_country.sql-------------------------------------------

/{/{ config
(schema='edp_dbt_ref_pipeline_example')
}/}/

with

source as (

    select * from /{/{ source ('raw_country', 'raw_country') }/}/

)


select * from source

--end-euh_country.sql-------------------------------------------

-- Create a yml file "dbt_transform/models/elementary/enriched_unharmonized/euh_country.yml" as below
--start-euh_country.yml-------------------------------------------

version: 2

models:
  - name: euh_country
    config:
      tags: ["elementary_test"]
    data_tests:
      - elementary.schema_changes:
          tags: ["schema_changes"]
          config:
            severity: warn

--end-euh_country.yml-------------------------------------------

-- Create a sql "dbt_transform/models/elementary/enriched_harmonized/eh_country.sql" as below
--start-eh_country.sql-------------------------------------------

/{/{ config
(schema='edp_dbt_ref_pipeline_example')
}/}/

with
euh_country as (
    select *
    from /{/{ ref('euh_country') }/}/
)

select *
from euh_country

--end-eh_country.sql-------------------------------------------

-- Create a yml "dbt_transform/models/elementary/enriched_harmonized/eh_country.yml" as below
--start-eh_country.yml-------------------------------------------

version: 2

models:
  - name: eh_country
    config:
      tags: ["elementary_test"]
    data_tests:
      - elementary.schema_changes:
          tags: ["schema_changes"]
          config:
            severity: warn

--end-eh_country.yml-------------------------------------------


------ Part 1 Step 3

--Run dbt in your terminal
--> dbt run -s tag:elementary_test

--Test dbt in your terminal
--> dbt test -s tag:elementary_test

--run elementary in your terminal
--> edr report

-- Check elemantary report via Elementary UI and Note it


------ Part 1 Step 4

-- Add more columns to raw

ALTER TABLE {{ source_table }}
ADD COLUMN updated_at TIMESTAMP;
ALTER TABLE {{ source_table }}
ADD COLUMN population BIGINT;
ALTER TABLE {{ source_table }}
ADD COLUMN latitude DECIMAL(10,2);
ALTER TABLE {{ source_table }}
ADD COLUMN capital_city STRING;

-- Check raw table
select * from {{ source_table }};


------ Part 1 Step 5

--Run dbt in your terminal
--> dbt run -s tag:elementary_test

--Test dbt in your terminal
--> dbt test -s tag:elementary_test

--run elementary in your terminal
--> edr report

-- Check elemantary schema changes report via Elementary UI

-- Notice the schema changes report at Elementary UI. (Test Type: column_added)
-- We only process the new columns


------ Part 1 Step 6

-- Delete(Drop) column from raw
-- to be able to delete column we need upgrade the table version and enable column mapping with the following command.

ALTER TABLE {{ source_table }} SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
)
ALTER TABLE {{ source_table }}
DROP COLUMN latitude;
ALTER TABLE {{ source_table }}
DROP COLUMN capital_city;

-- Check raw table
select * from {{ source_table }};


------ Part 1 Step 7

--Run dbt in your terminal
--> dbt run -s tag:elementary_test

--Test dbt in your terminal
--> dbt test -s tag:elementary_test

--run elementary in your terminal
--> edr report

-- Check elemantary schema changes report via Elementary UI

-- Notice the schema changes report at Elementary UI. (Test Type: column_removed)
-- We only delete a column from raw.


-- PART 2  (Anomaly Detections)

------ Part 2 Step 1

-- Update source yml file "dbt_transform/models/example/elementary/source_elementary.yml" as below
--start-source_elementary.yml-------------------------------------------

        config:
          elementary:
            timestamp_column: "updated_at"   # Elementary anomaly detection tests will use this column to create time buckets and filter the table.
        data_tests:
          - elementary.freshness_anomalies:  # Monitors the latest timestamp of a table to detect data delays.
              timestamp_column: updated_at
              severity: warn    # warn | error
          - elementary.volume_anomalies:    # Monitors table row count over time to detect drops or spikes in volume.
              timestamp_column: updated_at
              severity: warn    # warn | error
          - elementary.all_columns_anomalies:
              #where_expression:          # Filter the tested data using a valid sql expression. "country_name != 'unwanted country'"
              tags: ["all_columns_anomalies"]      # for special testing purposes -> dbt test --select tag:all_columns_anomalies
              days_back: 1                # nb of days of data to retrieve default: 14 days
              anomaly_sensitivity: 2      # 1 lowest - 3 highest and Configuration to define how the expected range is calculated. A sensitivity of 2 means that the expected range is within 2 standard deviations from the average of the training set
              anomaly_direction: both     # both | spike | drop
          - elementary.all_columns_anomalies:
              column_anomalies:
              - null_count      # it alerts if the latest daily null_count is significantly less or more than the daily null_count in the last 14 days (can be configured)
              - null_percent    # it alerts if the latest daily percentage of nulls is significantly less or more than the daily percentage of nulls in the last 14 days (can be configured)
              - missing_count   # it alerts if the latest daily missing_count is significantly less or more than the daily missing_count in the last 14 days (can be configured)
              anomaly_sensitivity: 4  # Configuration to define how the expected range is calculated. A sensitivity of 4 means that the expected range is within 4 standard deviations from the average of the training set
              timestamp_column: updated_at

--end-source_elementary.yml----------------------------------------------


------ Part 2 Step 2

-- Add more new data to raw

-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Afghanistan  ' as name , 93   as code , 'AF   ' as iso, current_timestamp() as updated_at,    1   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Albania  ' as name , 355   as code , 'AL   ' as iso, current_timestamp() as updated_at,    2   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Algeria  ' as name , 213   as code , 'DZ   ' as iso, current_timestamp() as updated_at,    3   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('American Samoa  ' as name , 1-684   as code , 'AS   ' as iso, current_timestamp() as updated_at,    4   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Andorra  ' as name , 376   as code , 'AD   ' as iso, current_timestamp() as updated_at,    5   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Angola  ' as name , 244   as code , 'AO   ' as iso, current_timestamp() as updated_at,    6   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Anguilla  ' as name , 1264   as code , 'AI   ' as iso, current_timestamp() as updated_at,    7   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Antarctica  ' as name , 672   as code , 'AQ   ' as iso, current_timestamp() as updated_at,    8   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Antigua and Barbuda  ' as name , 1-268   as code , 'AG   ' as iso, current_timestamp() as updated_at,    9   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Argentina  ' as name , 54   as code , 'AR   ' as iso, current_timestamp() as updated_at,    10   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Armenia  ' as name , 374   as code , 'AM   ' as iso, current_timestamp() as updated_at,    11   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Aruba  ' as name , 297   as code , 'AW   ' as iso, current_timestamp() as updated_at,    12   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Australia  ' as name , 61   as code , 'AU   ' as iso, current_timestamp() as updated_at,    13   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Austria  ' as name , 43   as code , 'AT   ' as iso, current_timestamp() as updated_at,    14   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Azerbaijan  ' as name , 994   as code , 'AZ   ' as iso, current_timestamp() as updated_at,    15   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Bahamas  ' as name , 1242   as code , 'BS   ' as iso, current_timestamp() as updated_at,    16   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Bahrain  ' as name , 973   as code , 'BH   ' as iso, current_timestamp() as updated_at,    17   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Bangladesh  ' as name , 880   as code , 'BD   ' as iso, current_timestamp() as updated_at,    18   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Barbados  ' as name , 1246   as code , 'BB   ' as iso, current_timestamp() as updated_at,    19   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Belarus  ' as name , 375   as code , 'BY   ' as iso, current_timestamp() as updated_at,    20   as population);
-- insert into dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.raw_country values ('Belgium  ' as name , 32   as code , 'BE   ' as iso, current_timestamp() as updated_at,    21   as population);


------ Part 2 Step 3

--Run dbt in your terminal
--> dbt run -s tag:elementary_test

--Test dbt in your terminal
--> dbt test -s tag:anomaly_detection_test

--run elementary in your terminal
--> edr report

-- Check elemantary Freshness, Volume, Anomalies report via Elementary UI
-- Click on the specific Test Result such as Freshness or Volume, you will see the result and the result expectation based on your configuration as well.
