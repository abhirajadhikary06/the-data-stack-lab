select *
from {{ ref('emp_workspace_snapshot') }}
where dbt_valid_to is null