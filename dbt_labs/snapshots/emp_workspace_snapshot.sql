{% snapshot emp_workspace_snapshot %}

{{ config(
    target_database='analytics',
    target_schema='snapshots',
    unique_key='employee_id',
    strategy='timestamp',
    updated_at='updated_at' 
    )
}}

select * from {{ ref('employee_workspace_country') }} e

{% endsnapshot %}