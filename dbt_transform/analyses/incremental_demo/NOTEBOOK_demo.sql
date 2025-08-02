-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SCD Type 1 & 2 Incremental Demo
-- MAGIC
-- MAGIC This demo covers the incremental load of a source table to a target table using delta change data feed and slow changing dimension (SCD) Type 1 & 2.
-- MAGIC
-- MAGIC **SCD Type 1 (Overwrite):**
-- MAGIC - In SCD Type 1, historical data is not maintained. When new data arrives, it directly overwrites the existing data.
-- MAGIC - Use this type when you don’t need to track historical changes. For example, mapping tables (like states, zip codes) fall into this category.
-- MAGIC - DBT model config `unique_key`
-- MAGIC
-- MAGIC **SCD Type 2 (Historical Tracking):**
-- MAGIC - SCD Type 2 keeps both current and historical data.
-- MAGIC - When a change occurs, a new row is added with updated values, preserving the old record. This type allows tracking changes over time, which is crucial for metrics and analytics.
-- MAGIC - DBT model **do NOT** config `unique_key`
-- MAGIC
-- MAGIC ## Single table use cases
-- MAGIC
-- MAGIC ### Case 1: SCD type 1 - insert, update only
-- MAGIC - Only get inserted and updated data from the source table.
-- MAGIC - SCD type 1, merge data
-- MAGIC ```
-- MAGIC raw_source_base_a -> euh_base_a_scd1
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 2: SCD type 1 - insert, update and delete
-- MAGIC - When the data is directly deleted in the source table.
-- MAGIC - SCD type 1, merge data
-- MAGIC ```
-- MAGIC raw_source_base_a -> euh_base_a_scd1_with_del_from_source
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 3: SCD type 1 - insert, update and delete
-- MAGIC - When there is a column to flag when the record was deleted in the source.
-- MAGIC - SCD type 1, merge data
-- MAGIC ```
-- MAGIC raw_source_base_a                  \
-- MAGIC                                      euh_base_a_scd1_with_del_from_flag
-- MAGIC euh_child_b_scd1_with_del_from_flag /
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 4: SCD type 2 - insert, update only
-- MAGIC - Only get inserted and updated data from the source table.
-- MAGIC - SCD type 2, appended only
-- MAGIC ```
-- MAGIC raw_source_base_a -> euh_base_a_scd2
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 5: SCD type 2 - insert, update and delete
-- MAGIC - When the data is directly deleted in the source table.
-- MAGIC - SCD type 2, appended only
-- MAGIC ```
-- MAGIC raw_source_base_a -> euh_base_a_scd2_with_del_from_source
-- MAGIC ```
-- MAGIC ### Case 6: SCD type 2 - insert, update and delete
-- MAGIC - When there is a column to flag when the record was deleted in the source.
-- MAGIC - SCD type 2, appended only
-- MAGIC ```
-- MAGIC raw_source_base_a     \
-- MAGIC                        euh_base_a_append_with_del_from_flag -> eh_base_a_scd2_with_del
-- MAGIC raw_source_base_a_del /
-- MAGIC ```
-- MAGIC ---
-- MAGIC
-- MAGIC ### Join tables uses cases
-- MAGIC
-- MAGIC ### Case 1: SCD type 1 - insert, update only
-- MAGIC ```
-- MAGIC euh_base_a_scd1  \
-- MAGIC                   eh_join_scd1
-- MAGIC euh_child_b_scd1 /
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 2: SCD type 1 - insert, update and delete
-- MAGIC ```shell
-- MAGIC eh_base_a_scd2_with_del   \
-- MAGIC                            eh_join_scd1_with_del
-- MAGIC euh_child_b_scd1_with_del /
-- MAGIC ```
-- MAGIC ---
-- MAGIC ### Case 3: SCD type 2 - insert, update and delete
-- MAGIC ```
-- MAGIC euh_base_a_scd1_with_del_from_flag  \
-- MAGIC                                      eh_join_scd1_with_del_view
-- MAGIC euh_child_b_scd1_with_del_from_flag /
-- MAGIC ```
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Before start
-- MAGIC
-- MAGIC - In your IDE move the folder `dbt_transform/analyses/incremental_demo` to `dbt_transform/models/example`.
-- MAGIC ```shell
-- MAGIC cp -r dbt_transform/analyses/incremental_demo dbt_transform/models/example/
-- MAGIC ```
-- MAGIC - Update `dbt_transform/analyses/incremental_demo/source_incremental.yml` with the schema you want to use for the raw tables.
-- MAGIC ```yml
-- MAGIC version: 2
-- MAGIC
-- MAGIC sources:
-- MAGIC   - name: example_incremental_demo
-- MAGIC     schema: <your-schema-name-here>
-- MAGIC     tables:
-- MAGIC       - name: raw_source_base_a
-- MAGIC       - name: raw_source_child_b
-- MAGIC       - name: raw_source_base_a_del
-- MAGIC       - name: raw_source_child_b_del
-- MAGIC ```
-- MAGIC - Open this notebook in your databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1
-- MAGIC - Set your unity catalog and sandbox schema in the next cell and run it.
-- MAGIC - Drop all source and target tables to start from scratch. Just run the "Drop/create" cell.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Use UC
USE CATALOG dev_prod_dbt_poc_unitycatalog_dev;
USE SCHEMA edp_dbt_ref_pipeline_example;

-- COMMAND ----------

-- DBTITLE 1,drop/create
-- sources
drop table if exists raw_source_base_a;
drop table if exists raw_source_child_b;
drop table if exists raw_source_base_a_del;
drop table if exists raw_source_child_b_del;

-- targets scd1
drop table if exists euh_base_a_scd1;
drop table if exists euh_child_b_scd1;
-- targets scd1 with delete
drop table if exists euh_base_a_scd1_with_del_from_source;
drop table if exists euh_base_a_scd1_with_del_from_flag;
drop table if exists euh_child_b_scd1_with_del_from_flag;

-- targets append only
drop table if exists euh_base_a_append;
-- targets append with delete
drop table if exists euh_base_a_append_with_del_from_source;
drop table if exists euh_base_a_append_with_del_from_flag;

-- targets scd2
drop view if exists eh_base_a_scd2;
-- targets scd2 with delete
drop view if exists eh_base_a_scd2_with_del;

-- targets join
drop table if exists eh_join_scd1;
-- targets join with delete
drop table if exists eh_join_scd1_with_del;
drop view if exists eh_join_scd1_with_del_view;

----------------------
----------------------

create table raw_source_base_a using delta tblproperties ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name','delta.enableChangeDataFeed' = 'True') as (
  select 1 as id, 'Ana' as name, 'ana@shell.com' as email, current_timestamp() as raw_insert_date union
  select 2 as id,'Maria' as name, 'maria@shell.com' as email, current_timestamp() as raw_insert_date union
  select 3 as id,'John' as name, 'paul@shell.com' as email, current_timestamp() as raw_insert_date union
  select 4 as id,'Sarah' as name, 'sarah@shell.com' as email, current_timestamp() as raw_insert_date
);

create table raw_source_child_b using delta tblproperties ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name','delta.enableChangeDataFeed' = 'True') as (
  select 1 as id, 'NL' as country, current_timestamp() as raw_insert_date union
  select 2 as id,'IT' as country, current_timestamp() as raw_insert_date union
  select 3 as id,'NL' as country, current_timestamp() as raw_insert_date union
  select 10 as id,'BR' as country, current_timestamp() as raw_insert_date
);

create table raw_source_base_a_del using delta tblproperties ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name','delta.enableChangeDataFeed' = 'True'
) as (
  select 2 as id, 'D' operation, current_timestamp() as raw_insert_date union
  select 5 as id, 'D' operation, current_timestamp() as raw_insert_date
);

create table raw_source_child_b_del using delta tblproperties ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name','delta.enableChangeDataFeed' = 'True'
) as (
  select 3 as id, 'D' operation, current_timestamp() as raw_insert_date union
  select 6 as id, 'D' operation, current_timestamp() as raw_insert_date
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2
-- MAGIC - Execute DBT in your local terminal.
-- MAGIC ``` shell
-- MAGIC dbt run -s tag:inc_demo --vars '{"job_id":"incremental load test","run_id":"run 1 - full load"}'
-- MAGIC ```
-- MAGIC - As none of those tables exist yet, it will be create as a full load.
-- MAGIC - Check the result in the differnet tables with the queries below.

-- COMMAND ----------

-- DBTITLE 1,select source

select * from raw_source_base_a;
select * from raw_source_child_b;
select * from raw_source_base_a_del;
select * from raw_source_child_b_del;

-- COMMAND ----------

-- DBTITLE 1,select euh_base_a_scd1
select * from euh_base_a_scd1 order by 1;
select * from euh_base_a_scd1_with_del_from_source order by 1;
select * from euh_base_a_scd1_with_del_from_flag order by 1;

-- COMMAND ----------

-- DBTITLE 1,select euh_child_b_scd1
select * from euh_child_b_scd1 order by 1;
select * from euh_child_b_scd1_with_del_from_flag order by 1;

-- COMMAND ----------

-- DBTITLE 1,select euh_base_a_scd2
select * from euh_base_a_append order by 1;
select * from euh_base_a_append_with_del_from_flag order by 1;
select * from euh_base_a_append_with_del_from_source order by 1;
---
select * from eh_base_a_scd2 order by 1;
select * from eh_base_a_scd2_with_del order by 1;

-- COMMAND ----------

-- DBTITLE 1,select eh_join_scd1
select * from eh_join_scd1 order by 1;
select * from eh_join_scd1_with_del order by 1;
select * from eh_join_scd1_with_del_view order by 1;

-- COMMAND ----------

-- DBTITLE 1,CDF metadata

-- "Change data feed" log describe
describe history euh_base_a_scd1_with_del_from_source;

-- Table "Change data feed" log per row
select _change_type,_commit_version,_commit_timestamp,*
from table_changes('euh_base_a_scd1_with_del_from_source', 0)
order by _commit_timestamp;

-- To check the target table metadata. We save the last version of the source there.
show tblproperties `euh_base_a_scd1_with_del_from_source`;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 3
-- MAGIC - Insert data in the raw source tables.
-- MAGIC - In your local terminal run DBT. As the target table already exist and the source tables have new data, it will be an incremental load.
-- MAGIC ``` shell
-- MAGIC dbt run -s tag:inc_demo --vars '{"job_id":"incremental load test","run_id":"run 2 - incremental load"}'
-- MAGIC ```
-- MAGIC - Rows will be inserted, updated or deleted in the target tables according to the SCD type configuration.
-- MAGIC - Check the result of the differnet tables with the queries above.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Source insert

insert into raw_source_base_a values (3,'John','john@shell.com',current_timestamp());
insert into raw_source_base_a values (5,'Paul','paul@shell.com',current_timestamp());
--
insert into raw_source_child_b values (4,'NL',current_timestamp());
--
insert into raw_source_base_a_del values (3,'D',current_timestamp());
--
insert into raw_source_child_b_del values (2,'D',current_timestamp());

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 4
-- MAGIC - Insert Update and delete data in the raw source tables.
-- MAGIC - In your local terminal run DBT. As the target table already exist and the source tables have new data, it will be an incremental load.
-- MAGIC ``` shell
-- MAGIC dbt run -s tag:inc_demo --vars '{"job_id":"incremental load test","run_id":"run 3 - incremental load"}'
-- MAGIC ```
-- MAGIC - Rows will be inserted, updated or deleted in the target tables according to the SCD type configuration.
-- MAGIC - Check the result of the differnet tables with the queries above.

-- COMMAND ----------

-- DBTITLE 1,Source: Insert, update and delete
insert into raw_source_base_a values (6,'Nina','nina@shell.com',current_timestamp());
update raw_source_base_a set email = 'ana.ana@shell.com' where id = 1;
delete from raw_source_base_a where id = 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q&A
-- MAGIC
-- MAGIC ### How DBT knows which one is the last version which was loaded?
-- MAGIC It's not the default behavior of DBT.
-- MAGIC
-- MAGIC We, the DBT CoE, did changes to the DBT core code to save the last version of the source table in the target table as Properties "delta.lastUpdateVersion".
-- MAGIC
-- MAGIC You can check it on databricks -> Catalog find your table, go to Details/Properties.
-- MAGIC
-- MAGIC The main change we did you can find in the file `dbt_transform/macros/incremental/get_incremental_relation.sql`.
-- MAGIC
-- MAGIC ### How DBT selects only the new data?
-- MAGIC We select from "table_changes" only the new versions.
-- MAGIC
-- MAGIC If did all the steps in the demo, you can check your target file e.g. "dbt_transform/target/compiled/dbt_transform/models/example/euh_incremental.sql"
-- MAGIC
-- MAGIC There you can find the query DBT executed to get only the new data
-- MAGIC
-- MAGIC ### What happens if data is delete in the raw table?
-- MAGIC Nothing. Incremental only append or update depending of your configuration, never deletes.
-- MAGIC
-- MAGIC ### What happens if I have duplicate data on my raw table?
-- MAGIC If defined a "unique_key" in your model and it's duplicated in the raw data. DBT will fail.
-- MAGIC
-- MAGIC You need to deduplicate your data.
-- MAGIC
-- MAGIC ### How do I enable change data feed on my raw tables?
-- MAGIC [Official documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed#frequently-asked-questions-faq)
-- MAGIC
-- MAGIC You must explicitly enable the change data feed option using one of the following methods:
-- MAGIC - For new table, set the table property delta.enableChangeDataFeed = true in the CREATE TABLE command.
-- MAGIC ``` sql
-- MAGIC CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC ```
-- MAGIC - For existing table, set the table property delta.enableChangeDataFeed = true in the ALTER TABLE command.
-- MAGIC ``` sql
-- MAGIC ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC ```
-- MAGIC - All new tables
-- MAGIC ``` sql
-- MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
-- MAGIC ```
-- MAGIC
-- MAGIC ### How do I configure change data feed on DBT? (euh, eh, cur, etc ))
-- MAGIC On `dbt_project.yml` you will find the parameter "tblproperties" under models
-- MAGIC
