# Incremental Load Demo Setup

- In your IDE **move** the folder `dbt_transform/analyses/incremental_demo` to `dbt_transform/models/example`.
```shell
mv -r dbt_transform/analyses/incremental_demo/incremental_models dbt_transform/models/example/
```
- Update `dbt_transform/models/example/source_incremental.yml` with the schema you want to use for the raw tables.
```yml
version: 2

sources:
  - name: example_incremental_demo
    schema: <your-schema-name-here>
    tables:
      - name: raw_source_base_a
      - name: raw_source_child_b
      - name: raw_source_base_a_del
      - name: raw_source_child_b_del
```
- Open the notebook `dbt_transform/analyses/incremental_demo/NOTEBOOK_demo.sql` in your databricks workspace.
- Follow the instructions in the notebook to create the raw tables.
