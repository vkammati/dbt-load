
-- When materializing as streaming tables (recommended), this could be the content of the
-- a model to load the (json) files stored at 'path/to/source'.
-- The env_var('DBT_LANDING_LOC') is available at runtime when the GitHub environment
-- variable 'LANDING_LOCATION_URL' is set.

SELECT *,
    _metadata.file_path -- this will add the file path of the loaded file as a column
FROM read_files(
    "{{ env_var('DBT_LANDING_LOC', '') }}/path/to/source"
    , format => 'json');


-- When materializing as 'incremental', that same source can be loaded incrementally as
-- well as shown in the example below. This way, one can still use SQL classic and no DLT
-- pipelines are running in the background. However, keep in mind that 'read_files' will
-- first need to discover all the files in the specified location, read them and only
-- then filter out the files that have already been loaded. For folders that contain many
-- files, this can have a serious performance impact.
SELECT *,
    _metadata.file_path -- this will add the file path of the loaded file as a column
FROM read_files(
    "{{ env_var('DBT_LANDING_LOC', '') }}/path/to/source"
    , format => 'json')
{% if is_incremental() -%}
where _metadata.file_path not in (
    select file_path
    from {{ this }}
)
{%- endif -%}
