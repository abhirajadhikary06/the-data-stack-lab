# What is dbt (data build tool)?

**dbt (Data Build Tool)** is an open-source command-line tool and cloud-hosted compilation engine that acts as the orchestration layer for executing modular, version-controlled SQL statements directly inside an ELT (Extract, Load, Transform) cloud data warehouse.

---

## Phase 1 — dbt Basics

### 1. Install dbt

Installed via `pip install dbt-core` or a warehouse-specific adapter:

- `dbt-snowflake`
- `dbt-bigquery`
- `dbt-duckdb`

### 2. Initialize a dbt Project

Run `dbt init` to scaffold the project structure (`models/`, `macros/`, `tests/`, etc.).

### 3. Configure Warehouse Connections (Dev & Prod)

`profiles.yml` - connection and environment settings.
Edit `profiles.yml` to define multiple targets (`dev`, `prod`) with credentials, schema, and database.

`dbt_project.yml` - project behaviour and model configuration.
Edit `dbt_project.yml` to define behaviour of your model like views, tables, packages path, schemas, tags.

Switch environments using:

```bash
dbt run --target dev
dbt run --target prod
```

### 4. Common Commands in dbt

- `dbt debug` — verify that the environment for dbt is set up correctly
- `dbt compile` — generates executable SQL from dbt models; compilations are not run against the warehouse, they are compiled and kept in `/target/compiled` directory
- `dbt run` — compiles the models and executes them against the warehouse; all runs are saved in `/target/run/` directory
- `dbt clean` — deletes temporary files and the `/target` and other artifacts created during previous run processes

### 5. Understand Project Structure

| Folder | Purpose |
| --- | --- |
| `models/` | SQL transformation files |
| `macros/` | Reusable Jinja logic |
| `tests/` | Data validation queries |
| `seeds/` | Seed CSVs |

### 6. Write First Models

Create `.sql` files in `models/` that select and transform raw data. These models represent tables or views in the warehouse.

### 7. Configure Materializations (Views vs Tables)

Use inline config blocks inside model files:

```sql
{{ config(materialized='table') }}       -- persisted snapshot
{{ config(materialized='view') }}        -- always queries source
{{ config(materialized='incremental') }} -- appends/updates new data
{{ config(materialized='ephemeral') }}   -- temporary or inline
```

Or set global defaults in `dbt_project.yml` for entire folders (e.g., staging as views, marts as tables).

### 8. Run dbt

```bash
dbt run
```

Builds all models in the warehouse according to chosen materializations.

### 9. Test Models

Add schema tests in `.yml` files:

```yaml
columns:
  - name: order_id
    tests:
      - unique
      - not_null
```

Run tests with:

```bash
dbt test
```

### 10. Document Models

Generate and serve documentation:

```bash
dbt docs generate
dbt docs serve
```

Explore lineage graphs and column-level metadata in the docs site.

### 11. Introduce Macros

Define reusable SQL/Jinja snippets in `macros/` and call them inside models for consistency, abstraction, and flexibility.

---

## Phase 2 — Core Modeling Engine

### 01. Initialize a dbt Project

```bash
dbt init my_project
```

Creates the standard folder structure. Connection configured in `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: dbt_user
      password: <your_password>
      role: DBT_DEV_ROLE
      warehouse: dev_wh
      database: analytics
      schema: dev_schema
```

### 02. Understand Project Structure

Key file: `dbt_project.yml`

```yaml
name: my_project
version: 1.0
models:
  my_project:
    +materialized: view
```

Organizes models, macros, tests, and sets global defaults (like materialization type).

### 03. Build Dependency Graph (DAGs)

Use `ref()` to declare dependencies:

```sql
select * from {{ ref('stg_orders') }}
```

dbt automatically builds the DAG lineage. Visualize it with:

```bash
dbt docs generate && dbt docs serve
```

### 04. Configure Materializations

Control how models are built:

```sql
{{ config(materialized='table') }}       -- persisted snapshot
{{ config(materialized='view') }}        -- always queries source
{{ config(materialized='incremental') }} -- appends/updates new data
{{ config(materialized='ephemeral') }}   -- temporary or inline
```

### 05. Use Incremental Builds

Conditional logic with `is_incremental()`:

```sql
{{ config(materialized='incremental') }}

select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}
```

- **First run** → full table load
- **Later runs** → only new rows processed

### 06. Apply Jinja Templating

Three Jinja block types:

| Syntax | Purpose |
| --- | --- |
| `{{ ... }}` | Expressions / output |
| `{% ... %}` | Control logic (if/else, loops) |
| `{# ... #}` | Comments |

Example:

```sql
select amount * {{ var('tax_rate', 0.1) }} as tax
from {{ ref('stg_orders') }}
{% if target.name == 'dev' %} limit 5 {% endif %}
```

### 07. Create and Use Macros

Define reusable SQL snippets in `macros/`:

```sql
{% macro add_margin(revenue_col, cost_col) %}
  ({{ revenue_col }} - {{ cost_col }}) / {{ revenue_col }} as margin
{% endmacro %}
```

Call in models:

```sql
select order_id, {{ add_margin('revenue', 'cost') }}
from {{ ref('stg_orders') }}
```

**Benefit:** DRY (Don't Repeat Yourself), maintainable, parameterized logic.

### 08. Sources, Seeds & Snapshots (Governance Layer)

**Sources** — declare raw tables loaded by ELT tools (e.g., Airbyte → Snowflake):

```yaml
version: 2
sources:
  - name: raw
    schema: raw
    tables:
      - name: orders
      - name: customers
```

Usage in models:

```sql
select * from {{ source('raw', 'orders') }}
```

**Seeds** — load static CSVs:

```bash
dbt seed
```

Places a `countries.csv` from `seeds/` into `dev_schema.countries`.

**Snapshots** — track historical changes (SCD):

```sql
{% snapshot customer_snapshot %}
  {{ config(target_schema='snapshots', unique_key='customer_id') }}
  select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
```

```bash
dbt snapshot
```

For a timestamp-based dbt snapshot, dbt automatically adds the following metadata columns:

- `dbt_valid_from`
- `dbt_valid_to`
- `dbt_scd_id`

---

## Phase 3 — Testing & Quality Enforcement

### 09. Schema & Custom Tests

**Schema tests** defined in YAML:

```yaml
version: 2
models:
  - name: stg_orders
    description: "Staging table for orders data."
    columns:
      - name: order_id
        description: "Unique identifier for each order."
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'delivered']
```

**Custom tests** written as SQL returning failing rows:

```sql
-- tests/not_null_customer_id.sql
select * from {{ ref('stg_orders') }}
where customer_id is null
```

Run all tests:

```bash
dbt test
```

### 10. Test Execution Strategy

**Key principles:**

- Without a strategy, tests may run inconsistently, leading to broken DAGs or unnoticed data issues.
- Tests should be automated, selective, and comprehensive.

**CI simulation with state-based testing:**

```bash
dbt build --select state:modified+
```

Runs only modified models and their downstream dependencies — faster pipelines, avoids re-testing unchanged models.

**Targeted builds:**

```bash
dbt build --select +stg_orders
```

Builds `stg_orders` and all downstream models, then runs their tests.

**Strategic goals:**

- **Automated** — integrated into CI/CD pipelines
- **Selective** — state-based runs for efficiency
- **Comprehensive** — combines schema + custom tests

---

## Phase 4 — Documentation & Observability

### 11. YAML Specs & Documentation

Documentation is co-located with models in YAML:

```yaml
version: 2
models:
  - name: stg_orders
    description: "Staging model for orders data"
    columns:
      - name: order_id
        description: "Unique identifier for each order"
      - name: status
        description: "Order status (placed, shipped, delivered)"
```

Generate and serve docs:

```bash
dbt docs generate
dbt docs serve
```

Produces a self-contained site with searchable metadata, column descriptions, and lineage graphs.

### 12. Lineage & Impact Analysis

dbt automatically builds lineage graphs using `ref()` and `source()`:

```sql
select * from {{ ref('stg_orders') }}
join {{ source('raw', 'customers') }} using (customer_id)
```

In the docs site you can:

- Visualize upstream sources feeding into staging
- See downstream marts depending on staging
- Perform **impact analysis**: if a column changes in `raw.customers`, dbt shows which models and tests are affected

---

## Phase 5 — Performance Engineering

### 13. Query Optimization

Focus on efficient SQL — select only required columns and push filters upstream:

```sql
select order_id, customer_id
from {{ ref('stg_orders') }}
where status = 'delivered'
```

**Benefit:** smaller scans → faster queries → lower warehouse cost.

### 14. Incremental Strategies

Use incremental materializations for large tables to avoid full rebuilds:

```sql
{{ config(materialized='incremental') }}

select * from {{ source('raw', 'orders') }}
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}
```

```bash
dbt run --select incremental_models
```

### 15. Partitioning & Clustering

For Snowflake / BigQuery, apply clustering or partitioning:

```sql
{{ config(
    materialized='table',
    cluster_by=['customer_id'],
    partition_by={'field': 'created_at', 'data_type': 'date'}
) }}
```

**Benefit:** improves query performance on large datasets.

### 16. Performance Testing

Run targeted builds and profile queries in the warehouse:

```bash
dbt build --select stg_orders+
```

| Warehouse | Profiling Tool |
| --- | --- |
| Snowflake | `EXPLAIN` or `QUERY_HISTORY` |
| BigQuery | `EXPLAIN` or job stats |

Identify bottlenecks: joins, aggregations, unfiltered scans.

### 17. Resource Management

Configure threads in `profiles.yml` to parallelize model builds:

```yaml
threads: 8
```

Override at runtime:

```bash
dbt run --threads 8
```

### 18. Performance Governance

Add tests to ensure optimized models don't break:

```yaml
models:
  - name: fct_sales
    tests:
      - not_null:
          column_name: sale_id
      - unique:
          column_name: sale_id
```

Monitor runtime metrics with dbt Cloud or warehouse logs.

---

## Phase 6 — Environment & Production Setup

### 19. Dev vs Prod Architecture

Define multiple targets in `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      schema: dev_schema
      threads: 4
    prod:
      type: snowflake
      schema: prod_schema
      threads: 8
```

Switch environments:

```bash
dbt run --target dev
dbt run --target prod
```

**Purpose:** clean separation between development and production schemas — safe experimentation in dev, reliable builds in prod.

### 20. External Packages (`dbt_utils` and more)

Add packages in `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: calogica/dbt_expectations
    version: 0.10.1
  - package: dbt-labs/snowplow
    version: 0.7.3
```

Install:

```bash
dbt deps
```

Example usage:

```sql
select * from {{ dbt_utils.star(from=ref('stg_orders'), except=["created_at"]) }}
```

**Purpose:** leverage community macros for common tasks (pivoting, surrogate keys, star selects).

### 21. Internal Packages (Reusing dbt Across Projects)

Reference a shared internal repo in `packages.yml`:

```yaml
packages:
  - git: "https://github.com/my-org/dbt-shared-macros.git"
    revision: main
```

Install with:

```bash
dbt deps
```

**Purpose:** modular architecture, DRY principle, consistency across teams and projects.

### 22. Modular Architecture Pattern

Break the project into distinct layers:

```
models/
├── staging/        ← cleaned, standardized from raw sources
├── intermediate/   ← business logic transformations
└── marts/          ← final business-facing tables
```

| Layer | Description |
| --- | --- |
| Sources | Raw data from ingestion tools (e.g., Airbyte) |
| Staging | Cleaned, renamed, typed data |
| Intermediate | Business logic and joins |
| Marts | Final tables consumed by BI tools |

**Purpose:** maintainable, scalable, and reusable design.

### 23. Governance & Production Best Practices

- Add tests for all critical models (uniqueness, not-null checks)
- Document models with YAML specs for observability
- Monitor runs with dbt Cloud or orchestrators (Airflow, Prefect, Dagster)

### 24. CI/CD Integration

Add dbt commands into pipelines (GitHub Actions, GitLab CI, Azure DevOps):

```yaml
name: dbt CI

on:
  push:
    branches:
      - main
      - dev
  pull_request:

jobs:
  dbt-ci:
    runs-on: ubuntu-latest

    env:
      DBT_PROFILES_DIR: ${{ github.workspace }}/.ci-dbt

    defaults:
      run:
        working-directory: ./dbt_labs

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt and adapter
        run: |
          pip install --upgrade pip
          pip install dbt-core dbt-duckdb

      - name: Create dbt profile
        run: |
          mkdir -p "$DBT_PROFILES_DIR"
          cat <<EOF > "$DBT_PROFILES_DIR/profiles.yml"
          dbt_project:
            target: dev
            outputs:
              dev:
                type: duckdb
                path: ./duckdb_data/analytics.duckdb
                schema: dev
                threads: 4
          EOF

      - name: Install dbt packages
        run: dbt deps

      - name: Seed data
        run: dbt seed

      - name: Run snapshots
        run: dbt snapshot

      - name: Build project
        run: dbt build
```

**Purpose:** automated testing and deployment, ensuring data quality before merging to production.

### 25. Strategic Takeaway

| Concept | Value |
| --- | --- |
| Dev vs Prod separation | Safe experimentation vs reliable production |
| External packages (`dbt_utils`) | Accelerate development with proven macros |
| Internal packages | Reuse dbt code across projects for consistency |
| Modular architecture | Layered design improves maintainability |
| Governance & CI/CD | Enforce quality, automate deployment, monitor production |

# Common SQL Patterns in dbt Models

## 1. `select ... from ...`

This is the basic shape of almost every dbt model.

```sql
select *
from {{ ref('stg_customers') }}
```

In dbt, models are usually just `SELECT` queries that define the final dataset.

---

## 2. `WITH ... AS (...)` (CTEs)

Very common for organizing transformations cleanly.

```sql
with source_data as (

    select *
    from {{ source('raw', 'customers') }}

),

final as (

    select
        id,
        upper(name) as name
    from source_data

)

select *
from final
```

CTEs make dbt models easier to read and maintain.

---

## 3. `ref()`

This is dbt’s most important SQL helper.

```sql
select *
from {{ ref('stg_orders') }}
```

It means:

- "Use another dbt model here"
- dbt will build dependency order automatically
- dbt can track lineage

---

## 4. `source()`

Used for raw source tables.

```sql
select *
from {{ source('raw', 'orders') }}
```

This tells dbt the table comes from an external source, not another dbt model.

---

## 5. `case when`

Used for conditional logic.

```sql
select
    customer_id,
    case
        when total_amount >= 1000 then 'vip'
        when total_amount >= 500 then 'regular'
        else 'new'
    end as customer_segment
from {{ ref('stg_customers') }}
```

Very common for:

- Categorization
- Flags
- Business rules

---

## 6. `join`

Used to combine datasets.

```sql
select
    o.order_id,
    c.customer_name
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
```

Common join types in dbt models:

- `left join`
- `inner join`
- `full outer join`

---

## 7. `group by`

Used for aggregations.

```sql
select
    customer_id,
    count(*) as order_count,
    sum(order_amount) as total_spent
from {{ ref('stg_orders') }}
group by customer_id
```

This is common in mart models and reporting models.

---

## 8. Window Functions

Very common in dbt for ranking, deduplication, and analytics.

```sql
select
    *,
    row_number() over (
        partition by customer_id
        order by updated_at desc
    ) as rn
from {{ ref('stg_customers') }}
```

Useful functions:

- `row_number()`
- `rank()`
- `dense_rank()`
- `lag()`
- `lead()`
- `sum() over (...)`

---

## 9. Deduplication Pattern

A very common dbt SQL pattern.

```sql
with ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as rn
    from {{ source('raw', 'customers') }}

)

select *
from ranked
where rn = 1
```

This keeps only the latest record per key.

---

## 10. Incremental Filter Logic

Used in incremental models.

```sql
{% if is_incremental() %}

where updated_at > (
    select max(updated_at)
    from {{ this }}
)

{% endif %}
```

This tells dbt to process only new or changed rows.

---

## 11. `union all`

Used to stack datasets.

```sql
select * from {{ ref('stg_orders_us') }}

union all

select * from {{ ref('stg_orders_eu') }}
```

Very common when combining similar tables from different regions or systems.

---

## 12. `coalesce`

Used to handle nulls.

```sql
select
    customer_id,
    coalesce(phone, email, 'unknown') as contact_value
from {{ ref('stg_customers') }}
```

Useful for:

- Default values
- Fallback logic
- Cleaning data

---

## 13. `cast`

Used to convert data types.

```sql
select
    cast(order_date as date) as order_date
from {{ ref('stg_orders') }}
```

Very common when source systems store data in inconsistent formats.

---

## 14. `dbt_utils`Style Patterns

If your project uses packages, you may also see macros like:

### `dbt_utils.star()`

```sql
{{ dbt_utils.star(from=ref('stg_customers')) }}
```

### `dbt_utils.generate_surrogate_key()`

```sql
{{ dbt_utils.generate_surrogate_key([
    'customer_id',
    'order_id'
]) }}
```

These are not plain SQL, but they are very common in dbt projects.