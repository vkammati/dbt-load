# Test big load performance with different models configurations

The result of this study case can be found [here](https://github.com/sede-x/Template-EDP-DBT-Reference-Pipeline/wiki/DBT-study-cases)

To run this test
- Move this folder from `dbt_transform/analyses` to `dbt_transform/models/example`.
- Enable "elementary" to run locally (dbt_project.yml).
- Remove the comment hashtag # from all models
  - from `{{ ref('raw__big_order_table') }}` to `from {{ ref('raw__big_order_table') }}`
- This test assume you access to `dev_prod_dbt_poc_unitycatalog_dev.dbt_example_raw.big_order_table`
  - Otherwise, create your own by using [create_example_data](https://github.com/sede-x/Template-EDP-DBT-Reference-Pipeline/blob/main/utils/create_example_data/main.py)
``` shell
dbt run -s raw__big_order_table+1 --vars '{"run_id":first,"job_id":"Test performance. 1st run. All tables are new"}';
dbt run -s raw__big_order_table+1 --vars '{"run_id":do_nothing,"job_id":"Test performance. 2nd run. No changes on raw"}';
dbt run -s raw__big_order_table+1 --vars '{"run_id":update,"job_id":"Test performance. 3th run. Update type, same unique_keys"}';
dbt run -s raw__big_order_table+1 --vars '{"run_id":insert,"job_id":"Test performance. 4th run. Insert type, new unique_keys"}';
dbt run -s raw__big_order_table+1 --vars '{"run_id":update_insert,"job_id":"Test performance. 5th run. Undate and insert"}';
```
## Query to check execution time of each model per run
``` sql
with cte_base as (
select
    mrr.generated_at,
    mrr.name,
    mrr.execution_time/60 as execution_time_minutes,
    mrr.materialization,
    inv.invocation_vars,
    mrr.model_invocation_reverse_index,
    mrr.is_the_first_invocation_of_the_day
from ludmila_bertier_edp_dbt_ref_pipeline_elementary.model_run_results as mrr
inner join ludmila_bertier_edp_dbt_ref_pipeline_elementary.dbt_invocations as inv on inv.invocation_id = mrr.invocation_id
where  mrr.status = 'success'
and inv.invocation_vars like '%Test performance%'
--and mrr.generated_at >= '2024-04-02 13:30:00'
)
--select * from cte_base order by 1 desc;
select
  substr(invocation_vars,charindex('run',invocation_vars,4)-4,7) as run_type ,
  name,
  max(execution_time_minutes) as max_exec_min,
  min(execution_time_minutes) as min_exec_min,
  avg(execution_time_minutes) as avg_exec_min,
  count(1) as n_exec
from cte_base
group by 1,2
order by 1;
```
