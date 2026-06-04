select
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'workspace_country']) }} as employee_workspace_key,
    employee_id,
    first_name,
    last_name,
    workspace_country,
    company,
    department,
    updated_at
from {{ ref('stg_emp_workspace_country') }}