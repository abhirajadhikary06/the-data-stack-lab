select
    e.employee_id,
    e.first_name,
    e.last_name,
    e.company,
    e.department,
    e.workspace_country,
    e.updated_at
from {{ ref('stg_emp_workspace_country') }} e
where e.company is null
order by e.workspace_country desc
    